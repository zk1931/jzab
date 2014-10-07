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

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Quorum zab implementation. This class manages the quorum zab servers.
 * For each server, it can be in one of three states :
 *   ELECTING, FOLLOWING, LEADING.
 * Among all the nodes, there's only one server can be established leader.
 */
public class Zab {
  private static final Logger LOG = LoggerFactory.getLogger(Zab.class);

  /**
   * Future for background "main" thread.
   */
  private final Future<Void> ft;

  /**
   * Server id for Zab.
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
   * Constructs a Zab instance by recovering from log directory.
   *
   * @param stateMachine the state machine implementation of clients.
   * @param prop the Properties object stores the configuration of Zab.
   */
  public Zab(StateMachine stateMachine, Properties prop) {
    this(stateMachine, prop, new SslParameters());
  }

  /**
   * Constructs a Zab instance by joining an existing cluster.
   *
   * @param stateMachine the state machine implementation of clients.
   * @param prop the Properties object stores the configuration of Zab.
   * @param joinPeer the id of peer you want to join in.
   */
  public Zab(StateMachine stateMachine, Properties prop,
                   String joinPeer) {
    this(stateMachine, prop, joinPeer, new SslParameters());
  }

  /**
   * Constructs a Zab instance with Ssl support by recovering from log
   * directory.
   *
   * @param stateMachine the state machine implementation of clients.
   * @param prop the Properties object stores the configuration of Zab.
   * @param sslParam parameters for Ssl.
   */
  public Zab(StateMachine stateMachine,
                   Properties prop,
                   SslParameters sslParam) {
    this(stateMachine, prop, null, sslParam);
  }

  /**
   * Constructs a Zab instance with Ssl support by joining an exisintg
   * cluster.
   *
   * @param stateMachine the state machine implementation of clients.
   * @param prop the Properties object stores the configuration of Zab.
   * @param joinPeer the id of peer you want to join in.
   *                           password is not set.
   * @param sslParam parameters for Ssl.
   */
  public Zab(StateMachine stateMachine,
                   Properties prop,
                   String joinPeer,
                   SslParameters sslParam) {
    this(stateMachine, null, null, new TestState(prop), joinPeer, sslParam);
  }

  Zab(StateMachine stateMachine,
            StateChangeCallback stateCallback,
            FailureCaseCallback failureCallback,
            TestState initialState) {
    this(stateMachine, stateCallback, failureCallback, initialState,
         null);
  }

  Zab(StateMachine stateMachine,
            StateChangeCallback stateCallback,
            FailureCaseCallback failureCallback,
            TestState initialState,
            String joinPeer) {
    this(stateMachine, stateCallback, failureCallback, initialState, joinPeer,
         new SslParameters());
  }

  Zab(StateMachine stateMachine,
            StateChangeCallback stateCallback,
            FailureCaseCallback failureCallback,
            TestState initialState,
            String joinPeer,
            SslParameters sslParam) {
    this.config = new ZabConfig(initialState.prop);
    this.stateMachine = stateMachine;
    this.mainThread = new MainThread(joinPeer, stateCallback, failureCallback);
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    try {
      // Initialize.
      this.mainThread.init(sslParam, initialState);
    } catch (Exception e) {
      LOG.warn("Caught an exception while initializing Zab.");
      throw new IllegalStateException("Failed to initialize Zab.", e);
    }
    this.ft = es.submit(this.mainThread);
    es.shutdown();
  }

  /**
   * Get the future of the background working thread of Zab.
   */
  public Future<Void> getFuture() {
    return this.ft;
  }

  /**
   * Submits a request to Zab. Any one can call call this send. Under the hood,
   * followers forward requests to the leader and the leader will be responsible
   * for converting this request to idempotent transaction and broadcasting.
   *
   * @param request request to send through Zab
   */
  public void send(ByteBuffer request) {
    this.mainThread.enqueueRequest(request);
  }

  /**
   * Flushes a request through pipeline. The flushed request will be delivered
   * in order with other sending requests, but it will not be convereted to
   * idempotent transaction and will not be persisted in log. And it will only
   * be delivered on the server who issued this request. The purpose of flush
   * is to allow implementing a consistent read-after-write.
   *
   * @param request the request to be flushed.
   */
  public void flush(ByteBuffer request) {
    this.mainThread.enqueueFlush(request);
  }

  /**
   * Removes a peer from the cluster.
   *
   * @param peerId the id of the peer who will be removed from the cluster.
   */
  public void remove(String peerId) {
    this.mainThread.enqueueRemove(peerId);
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
   * Returns the server id for this Zab instance. The application which
   * recovers from log directory probably needs to know the server id of Zab.
   *
   * @return the server id of this Zab instance.
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
   * Used for initializing the state of Zab for testing purpose.
   */
  static class TestState {
    Properties prop;
    File logDir = null;
    private File fAckEpoch;
    private File fProposedEpoch;
    private File fLastSeenConfig;
    private PersistentState persistence;
    Log log = null;

    /**
     * Creates the TestState object. It should be passed to Zab to
     * initialize its state.
     *
     * @param serverId the server id of the Zab.
     * @param servers the server list of the ensemble.
     * @param baseLogDir the base log directory of all the peers in ensemble.
     * The specific log directory of each peer is baseLogDir/serverId.
     * For example, if the baseLogDir is /tmp/log and the serverId is server1,
     * then the log directory for this server is /tmp/log/server1.
     */
    public TestState(String serverId, String servers, File baseLogDir)
        throws IOException {
      prop = new Properties();
      if (serverId != null) {
        this.prop.setProperty("serverId", serverId);
      }
      if (servers != null) {
        this.prop.setProperty("servers", servers);
      }
      if (serverId != null) {
        this.logDir = new File(baseLogDir, serverId);
      } else {
        this.logDir = baseLogDir;
      }
      this.persistence = new PersistentState(this.logDir);
      this.prop.setProperty("logdir", logDir.getAbsolutePath());
      this.fAckEpoch = new File(this.logDir, "ack_epoch");
      this.fProposedEpoch = new File(this.logDir, "proposed_epoch");
    }

    public TestState(Properties prop) {
      this.prop = prop;
    }

    TestState setProposedEpoch(long epoch) throws IOException {
      FileUtils.writeLongToFile(epoch, this.fProposedEpoch);
      return this;
    }

    TestState setAckEpoch(long epoch) throws IOException {
      FileUtils.writeLongToFile(epoch, this.fAckEpoch);
      return this;
    }

    TestState setLog(Log tlog) {
      this.log = tlog;
      return this;
    }

    Log getLog() {
      return this.log;
    }

    TestState setClusterConfiguration(ClusterConfiguration conf)
        throws IOException {
      this.persistence.setLastSeenConfig(conf);
      return this;
    }

    TestState setJoinPeer(String peer) {
      this.prop.setProperty("joinPeer", peer);
      return this;
    }
  }

  /**
   * Main working thread for Zab.
   */
  class MainThread implements Callable<Void> {
    /**
     * The State for Zab. It will be passed accross different instances of
     * Leader/Follower class.
     */
    private ParticipantState participantState;

    private final String joinPeer;
    private final StateChangeCallback stateChangeCallback;
    private final FailureCaseCallback failureCallback;

    MainThread(String joinPeer,
               StateChangeCallback stateChangeCallback,
               FailureCaseCallback failureCallback) {
      this.joinPeer = joinPeer;
      this.stateChangeCallback = stateChangeCallback;
      this.failureCallback = failureCallback;
    }

    private void init(SslParameters sslParam,
                      TestState testState)
        throws IOException, InterruptedException, GeneralSecurityException {
      PersistentState persistence =
        new PersistentState(config.getLogDir(), testState.getLog());
      if (joinPeer != null) {
        // First time start up. Joining some one.
        if (!persistence.isEmpty()) {
          LOG.error("The log directory is not empty while joining.");
          throw new RuntimeException("Log directory must be empty.");
        }
        serverId = config.getServerId();
      } else {
        // Means either it starts booting from static configuration or
        // recovering from a log directory.
        if (config.getPeers().size() > 0) {
          // TODO : Static configuration should be removed eventually.
          LOG.debug("Boots from static configuration.");
          // User has specified server list.
          List<String> peers = config.getPeers();
          serverId = config.getServerId();
          ClusterConfiguration cnf =
            new ClusterConfiguration(new Zxid(0, 0), peers, serverId);
          persistence.setLastSeenConfig(cnf);
        } else {
          // Restore from log directory.
          LOG.debug("Restores from log directory {}", config.getLogDir());
          ClusterConfiguration cnf = persistence.getLastSeenConfig();
          if (cnf == null) {
            throw new RuntimeException("Can't find configuration file.");
          }
          serverId = cnf.getServerId();
        }
      }
      participantState = new ParticipantState(persistence,
                                              serverId,
                                              sslParam,
                                              stateChangeCallback,
                                              failureCallback,
                                              config.getMinSyncTimeoutMs());
      MDC.put("serverId", serverId);
    }

    @Override
    public Void call() throws Exception {
      Election electionAlg = new RoundRobinElection();
      try {
        if (this.joinPeer != null) {
          stateMachine.recovering();
          join(this.joinPeer);
        }
        while (true) {
          stateMachine.recovering();
          PersistentState persistence = participantState.getPersistence();
          String leader = electionAlg.electLeader(persistence);
          LOG.debug("Select {} as leader.", leader);
          if (leader.equals(serverId)) {
            Participant participant =
              new Leader(participantState, stateMachine, config);
            ((Leader)participant).lead();
          } else {
            Participant participant =
              new Follower(participantState, stateMachine, config);
            ((Follower)participant).follow(leader);
          }
        }
      } catch (InterruptedException e) {
        LOG.debug("Caught Interrupted exception, it has been shut down?");
        participantState.getTransport().shutdown();
        Thread.currentThread().interrupt();
      } catch (Participant.LeftCluster e) {
        LOG.debug("Zab has been shutdown.");
      } catch (Exception e) {
        LOG.error("Caught exception :", e);
        throw e;
      }
      if (stateChangeCallback != null) {
        stateChangeCallback.leftCluster();
      }
      return null;
    }

    void join(String peer) throws Exception {
      Participant participant;
      if (peer.equals(serverId)) {
        LOG.debug("Trying to join itself. Becomes leader directly.");
        participant = new Leader(participantState, stateMachine, config);
      } else {
        LOG.debug("Trying to join {}.", peer);
        participant = new Follower(participantState, stateMachine, config);
      }
      participant.join(peer);
    }

    void enqueueRequest(ByteBuffer buffer) {
      this.participantState.enqueueRequest(buffer);
    }

    void enqueueRemove(String peerId) {
      this.participantState.enqueueRemove(peerId);
    }

    void enqueueFlush(ByteBuffer buffer) {
      this.participantState.enqueueFlush(buffer);
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
        participantState.clear();
      }
    }
  }
}
