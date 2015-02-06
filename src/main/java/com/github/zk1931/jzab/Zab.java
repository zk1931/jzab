/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.jzab;

import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import com.github.zk1931.jzab.ZabException.InvalidPhase;
import com.github.zk1931.jzab.ZabException.TooManyPendingRequests;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Zab is a fault-tolerant, replicated protocol that guarantees all requests
 * submitted to it will be delivered in same order to all servers in the
 * cluster. The Zab class exposes all the operations of Jzab library.
 */
public class Zab {
  private static final Logger LOG = LoggerFactory.getLogger(Zab.class);

  /**
   * Future for background "main" thread.
   */
  private final Future<Void> ft;

  /**
   * Server Id for Zab.
   */
  private String serverId;

  /**
   * Configuration for Zab.
   */
  private final ZabConfig config;

  /**
   * StateMachine callback.
   */
  private final StateMachine stateMachine;

  /**
   * Background thread for Zab.
   */
  private final MainThread mainThread;

  /**
   * Constructs a Zab instance by recovering from the log directory.
   *
   * @param stateMachine the state machine implementation of application.
   * @param config the configuration for Jzab, see {@link ZabConfig}.
   */
  public Zab(StateMachine stateMachine, ZabConfig config) {
    this(stateMachine, config, null, null, null);
  }

  /**
   * Constructs a Zab instance by joining an existing cluster. This constructor
   * is supposed to be called only for the very first time to initialize the
   * log directory, once the log directory gets initialized you should call
   * {@link #Zab(StateMachine, ZabConfig) Zab} which recovers the Zab instance
   * from log directory.
   *
   * @param stateMachine the state machine implementation of application.
   * @param config the configuration for Jzab, see {@link ZabConfig}.
   * @param serverId ID ("host:port") of this server.
   * @param joinPeer the ID of peer you want to join in, the ID is a host:port
   * string of the peer. The first server bootstraps the cluster by joining
   * itself.
   */
  public Zab(StateMachine stateMachine, ZabConfig config, String serverId,
             String joinPeer) {
    this(stateMachine, config, serverId, joinPeer, null, null, null);
  }

  /**
   * Constructs a Zab instance by booting from static cluster configuration.
   * This constructor is supposed to be called only for the very first time to
   * initialize the log directory, once the log directory gets initialized you
   * should call {@link #Zab(StateMachine, ZabConfig) Zab} which recovers the
   * Zab instance from log directory.
   *
   * @param stateMachine the state machine implementation of application.
   * @param config the configuration for Jzab, see {@link ZabConfig}.
   * @param serverId ID ("host:port") of this server.
   * @param peers the IDs of the servers in cluster, including itself.
   */
  public Zab(StateMachine stateMachine, ZabConfig config, String serverId,
             Set<String> peers) {
    this(stateMachine, config, serverId, peers, null, null, null);
  }

  // This constructor is for internal testing purpose. "initState" allows us to
  // setup initial state of Jzab before starting Jzab. "stateCallback" allows
  // us catch the state transition happend in the runtime. "failureCallback"
  // allows us to inject failures to different points of code path.
  Zab(StateMachine stateMachine,
      ZabConfig config,
      PersistentState initState,
      StateChangeCallback stateCallback,
      FailureCaseCallback failureCallback) {
    this(stateMachine, config, null, null, null, initState, stateCallback,
        failureCallback);
  }

  // Same as the above, but for joining a peer.
  Zab(StateMachine stateMachine,
      ZabConfig config,
      String serverId,
      String joinPeer,
      PersistentState initState,
      StateChangeCallback stateCallback,
      FailureCaseCallback failureCallback) {
    this(stateMachine, config, serverId, joinPeer, null, initState,
        stateCallback, failureCallback);
  }

  // Starts with static configuration.
  Zab(StateMachine stateMachine,
      ZabConfig config,
      String serverId,
      Set<String> peers,
      PersistentState initState,
      StateChangeCallback stateCallback,
      FailureCaseCallback failureCallback) {
    this(stateMachine, config, serverId, null, peers, initState,
         stateCallback, failureCallback);
  }

  Zab(StateMachine stateMachine,
      ZabConfig config,
      String serverId,
      String joinPeer,
      Set<String> peers,
      PersistentState initState,
      StateChangeCallback stateCallback,
      FailureCaseCallback failureCallback) {
    this.config = config;
    this.stateMachine = stateMachine;
    this.serverId = serverId;
    try {
      // Initialize.
      this.mainThread = new MainThread(joinPeer,
                                       peers,
                                       stateCallback,
                                       failureCallback,
                                       initState);
    } catch (Exception e) {
      LOG.warn("Caught an exception while initializing Zab.");
      throw new IllegalStateException("Failed to initialize Zab.", e);
    }
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    // Starts main thread.
    this.ft = es.submit(this.mainThread);
    es.shutdown();
  }

  /**
   * Get the future of the background working thread of Zab. Users can check
   * the status of the thread via the future.
   *
   * @return the future object of MainThread.
   */
  public Future<Void> getFuture() {
    return this.ft;
  }

  /**
   * Submits a request to Zab. Under the hood, followers forward requests to the
   * leader and the leader will be responsible for converting this request to
   * idempotent transaction and broadcasting. If you send request in
   * non-broadcasting phase, the operation will fail.
   *
   * @param request the request to send through Zab
   * @param ctx context to be provided to the callback
   * @throws ZabException.InvalidPhase if Zab is not in broadcasting phase.
   * @throws ZabException.TooManyPendingRequests if the pending requests exceeds
   * the certain size, for example: if there are more pending requests than
   * ZabConfig.MAX_PENDING_REQS.
   */
  public void send(ByteBuffer request, Object ctx)
      throws InvalidPhase, TooManyPendingRequests {
    this.mainThread.send(request, ctx);
  }

  /**
   * Flushes a request through pipeline. The flushed request will be delivered
   * in order with other sending requests, but it will not be convereted to
   * idempotent transaction and will not be persisted in log. And it will only
   * be delivered on the server who issued this request. The purpose of flush
   * is to allow implementing a consistent read-after-write. If you send flush
   * request in non-broadcasting phase, the operation will fail.
   *
   * @param request the request to be flushed.
   * @param ctx context to be provided to the callback
   * @throws ZabException.InvalidPhase if Zab is not in broadcasting phase.
   * @throws ZabException.TooManyPendingRequests if the pending requests exceeds
   * the certain size, for example: if there are more pending requests than
   * ZabConfig.MAX_PENDING_REQS.
   */
  public void flush(ByteBuffer request, Object ctx)
      throws InvalidPhase, TooManyPendingRequests {
    this.mainThread.flush(request, ctx);
  }

  /**
   * Removes a peer from the cluster. If you send remove request in
   * non-broadcasting phase, the operation will fail.
   *
   * @param peerId the id of the peer who will be removed from the cluster.
   * @param ctx context to be provided to the callback
   * @throws ZabException.InvalidPhase if Zab is not in broadcasting phase.
   * @throws ZabException.TooManyPendingRequests if there is a pending snapshot
   * request.
   */
  public void remove(String peerId, Object ctx)
      throws InvalidPhase, TooManyPendingRequests {
    this.mainThread.remove(peerId, ctx);
  }

  /**
   * Issues the request to take a snapshot. The {@link StateMachine#save}
   * callback will be called for serializing the application's state to disk.
   *
   * @param ctx context to be provided to the callback
   * @throws ZabException.InvalidPhase if Zab is not in broadcasting phase.
   * @throws ZabException.TooManyPendingRequests if there is a pending snapshot
   * request.
   */
  public void takeSnapshot(Object ctx)
      throws InvalidPhase, TooManyPendingRequests {
    this.mainThread.takeSnapshot(ctx);
  }

  /**
   * Shut down the Zab.
   *
   * @throws InterruptedException in case of it's interrupted.
   */
  public void shutdown() throws InterruptedException {
    this.mainThread.shutdown();
    LOG.debug("Shutdown successfully.");
  }

  /**
   * Returns the server Id for this Zab instance. The application which
   * recovers from log directory probably needs to know the server Id of Zab.
   *
   * @return the server Id of this Zab instance.
   */
  public String getServerId() {
    return this.serverId;
  }

  /**
   * Interface of callbacks which will be called when phase change happens.
   * Used for testing purpose.
   *
   * Phase changes :
   *
   *        leaderDiscovering - leaderSynchronizating - leaderBroadcasting
   *        /                                                              \
   * electing                                                               Exit
   *        \                                                              /
   *        followerDiscovering - followerSynchronizating - followerBroadcasting
   *
   */
  interface StateChangeCallback {

    /**
     * Will be called when entering electing phase.
     */
    void electing();

    /**
     * Will be called when entering discovering phase of leader.
     *
     * @param electedLeader the elected leader.
     */
    void leaderDiscovering(String electedLeader);

    /**
     * Will be called when entering discovery phase of follower.
     *
     * @param electedLeader the elected leader of this follower.
     */
    void followerDiscovering(String electedLeader);

    /**
     * Will be called on leader side when the owner of initial history is
     * chosen.
     *
     * @param server the id of the server whose history is selected for
     * synchronization.
     * @param aEpoch the acknowledged epoch of the node whose initial history
     * is chosen for synchronization.
     * @param zxid the last transaction id of the node whose initial history
     * is chosen for synchronization.
     */
    void initialHistoryOwner(String server, long aEpoch, Zxid zxid);

    /**
     * Will be called when entering synchronization phase of leader.
     *
     * @param epoch the established epoch.
     */
    void leaderSynchronizing(long epoch);

    /**
     * Will be called when entering synchronization phase of follower.
     *
     * @param epoch the established epoch.
     */
    void followerSynchronizing(long epoch);

    /**
     * Will be called when entering broadcasting phase of leader.
     *
     * @param epoch the acknowledged epoch (f.a).
     * @param history the initial history (f.h) of broadcasting phase.
     */
    void leaderBroadcasting(long epoch, List<Transaction> history,
                            ClusterConfiguration config);

    /**
     * Will be called when entering broadcasting phase of follower.
     *
     * @param epoch the current epoch (f.a).
     * @param history the initial history (f.h) of broadcasting phase.
     */
    void followerBroadcasting(long epoch, List<Transaction> history,
                              ClusterConfiguration config);

    /**
     * Will be called when Zab stops running.
     */
    void leftCluster();

    /**
     * Will be called once a COP is committed on leader side.
     */
    void commitCop();
  }

  /**
   * Will be thrown to force servers go back to electing phase, for test
   * purpose only.
   */
  static class SimulatedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SimulatedException(String desc) {
      super(desc);
    }

    public SimulatedException() {}
  }

  /**
   * Interface of callbacks which simulate different kinds of failure cases for
   * testing purpose.
   */
  abstract static class FailureCaseCallback {

    /**
     * Will be called when entering discovering phase of leader.
     *
     * @throws SimulatedException forces leader goes back to electing phase.
     */
    void leaderDiscovering() {};

    /**
     * Will be called when entering discovering phase of followers.
     *
     * @throws SimulatedException forces followers goes back to electing phase.
     */
    void followerDiscovering() {};

    /**
     * Will be called when entering synchronizing phase of leader.
     *
     * @throws SimulatedException forces leader goes back to electing phase.
     */
    void leaderSynchronizing() {};

    /**
     * Will be called when entering synchronizing phase of followers.
     *
     * @throws SimulatedException forces followers goes back to electing phase.
     */
    void followerSynchronizing() {};

    /**
     * Will be called when entering broadcasting phase of leader.
     *
     * @throws SimulatedException forces leader goes back to electing phase.
     */
    void leaderBroadcasting() {};

    /**
     * Will be called when entering discovering phase of followers.
     *
     * @throws SimulatedException forces followers goes back to electing phase.
     */
    void followerBroadcasting() {};
  }

  /**
   * Main working thread for Zab.
   */
  class MainThread implements Callable<Void>,
                              Transport.Receiver {
    /**
     * The state of Zab, it will be shared through different instance of
     * Participant object.
     */
    private ParticipantState participantState;
    /**
     * Message queue. The receiving callback simply parses the message and puts
     * it in queue, it's up to Leader/Follower/Election to take out
     * and process the message.
     */
    private final BlockingQueue<MessageTuple> messageQueue =
      new LinkedBlockingQueue<>();
    private final String joinPeer;
    private final StateChangeCallback stateChangeCallback;
    private final Transport transport;
    private final Election election;
    private final PersistentState persistence;
    private Participant participant = null;

    MainThread(String joinPeer,
               Set<String> peers,
               StateChangeCallback stateChangeCallback,
               FailureCaseCallback failureCallback,
               PersistentState initState)
        throws IOException, InterruptedException, GeneralSecurityException {
      this.joinPeer = joinPeer;
      this.stateChangeCallback = stateChangeCallback;
      if (initState == null) {
        // If there's no initial state, we'll constructs the PersistenState
        // from the the log directory.
        persistence = new PersistentState(config.getLogDir());
      } else {
        persistence = initState;
      }
      if (joinPeer != null) {
        // First time start up. Joining someone.
        if (!persistence.isEmpty()) {
          LOG.error("The log directory is not empty while joining.");
          throw new RuntimeException("Log directory must be empty.");
        }
      } else {
        // Means either it starts booting from static configuration or
        // recovering from a log directory.
        if (serverId != null) {
          LOG.debug("Boots from static configuration.");
          Zxid version = new Zxid(0, -1);
          ClusterConfiguration cnf =
            new ClusterConfiguration(version, peers, serverId);
          persistence.setLastSeenConfig(cnf);
        } else {
          // Restore from log directory.
          LOG.debug("Restores from log directory {}", config.getLogDir());
          ClusterConfiguration cnf = persistence.getLastSeenConfig();
          if (cnf == null) {
            throw new RuntimeException("Can't find configuration file.");
          }
          serverId = cnf.getServerId();
          persistence.cleanupClusterConfigFiles();
        }
      }
      MDC.put("serverId", serverId);
      // Creates transport.
      this.transport = new NettyTransport(serverId,
                                          this,
                                          config.getSslParameters(),
                                          persistence.getLogDir());

      election = new FastLeaderElection(persistence, transport, messageQueue);

      participantState = new ParticipantState(persistence,
                                              serverId,
                                              transport,
                                              messageQueue,
                                              stateChangeCallback,
                                              failureCallback,
                                              config.getMinSyncTimeoutMs(),
                                              election);
    }

    @Override
    public Void call() throws Exception {
      try {
        if (this.joinPeer != null) {
          join(this.joinPeer);
        }
        while (true) {
          if (stateChangeCallback != null) {
            stateChangeCallback.electing();
          }
          LOG.debug("Waiting for electing a leader.");
          String leader = this.election.electLeader();
          LOG.debug("Select {} as leader.", leader);
          if (leader.equals(serverId)) {
            participant = new Leader(participantState, stateMachine, config);
            ((Leader)participant).lead();
          } else {
            participant = new Follower(participantState, stateMachine, config);
            ((Follower)participant).follow(leader);
          }
        }
      } catch (InterruptedException e) {
        LOG.debug("Caught Interrupted exception, it has been shut down?");
        Thread.currentThread().interrupt();
      } catch (Participant.LeftCluster e) {
        LOG.debug("Zab has been shutdown.");
      } catch (Exception e) {
        LOG.error("Caught exception :", e);
        throw e;
      } finally {
        participantState.getTransport().shutdown();
      }
      if (stateChangeCallback != null) {
        stateChangeCallback.leftCluster();
      }
      return null;
    }

    @Override
    public void onReceived(String source, Message message) {
      MessageTuple tuple = new MessageTuple(source, message);
      this.messageQueue.add(tuple);
    }

    @Override
    public void onDisconnected(String server) {
      LOG.debug("ONDISCONNECTED from {}", server);
      Message disconnected = MessageBuilder.buildDisconnected(server);
      this.participantState.enqueueMessage(new MessageTuple(serverId,
                                                            disconnected));
    }

    void join(String peer) throws Exception {
      if (peer.equals(serverId)) {
        LOG.debug("Trying to join itself. Becomes leader directly.");
        participant = new Leader(participantState, stateMachine, config);
      } else {
        LOG.debug("Trying to join {}.", peer);
        participant = new Follower(participantState, stateMachine, config);
      }
      participant.join(peer);
    }

    void send(ByteBuffer buffer, Object ctx)
        throws InvalidPhase, TooManyPendingRequests {
      if (this.participant == null) {
        throw new InvalidPhase("Zab.send() called while recovering");
      }
      this.participant.send(buffer, ctx);
    }

    void remove(String peerId, Object ctx)
        throws InvalidPhase, TooManyPendingRequests {
      if (this.participant == null) {
        throw new InvalidPhase("Zab.remove() called while recovering");
      }
      this.participant.remove(peerId, ctx);
    }

    void flush(ByteBuffer buffer, Object ctx)
        throws InvalidPhase, TooManyPendingRequests {
      if (this.participant == null) {
        throw new InvalidPhase("Zab.flush() called while recovering");
      }
      this.participant.flush(buffer, ctx);
    }

    void takeSnapshot(Object ctx)
        throws InvalidPhase, TooManyPendingRequests {
      if (this.participant == null) {
        throw new InvalidPhase("Zab.takeSnapshot() called while recovering");
      }
      this.participant.takeSnapshot(ctx);
    }

    // Waits until MainThread thread has been shutdown. This function should be
    // called from a different thread.
    void shutdown() throws InterruptedException {
      this.participantState.enqueueShutdown();
      try {
        ft.get();
      } catch (ExecutionException ex) {
        throw new RuntimeException(ex);
      } finally {
        // Make sure we shutdown the transport in the end.
        this.transport.shutdown();
      }
    }

    /**
     * Clears all the messages in the message queue, clears the peer in
     * transport if it's the DISCONNECTED message. This function should be
     * called only right before going back to recovery.
     */
    protected void clearMessageQueue() {
      MessageTuple tuple = null;
      while ((tuple = messageQueue.poll()) != null) {
        Message msg = tuple.getMessage();
        if (msg.getType() == MessageType.DISCONNECTED) {
          this.transport.clear(msg.getDisconnected().getServerId());
        } else if (msg.getType() == MessageType.SHUT_DOWN) {
          throw new Participant.LeftCluster("Shutdown Zab.");
        }
      }
    }
  }
}
