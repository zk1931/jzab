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

import com.google.protobuf.TextFormat;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The processor which is responsible for taking the snapshot of application
 * when it's required.
 */
class SnapshotProcessor implements RequestProcessor, Callable<Void> {
  private final BlockingQueue<MessageTuple> requestQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotProcessor.class);

  private final StateMachine stateMachine;

  private final PersistentState persistence;

  private final Transport transport;

  private final String serverId;

  Future<Void> ft;

  public SnapshotProcessor(StateMachine stateMachine,
                           PersistentState persistence,
                           String serverId,
                           Transport transport) {
    this.stateMachine = stateMachine;
    this.persistence = persistence;
    this.serverId = serverId;
    this.transport = transport;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.requestQueue.add(request);
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.requestQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
    LOG.debug("SnapshotProcessor has been shut down.");
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("SnapshotProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = requestQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        if (msg.getType() == MessageType.SNAPSHOT) {
          Zxid zxid =
            MessageBuilder.fromProtoZxid(msg.getSnapshot().getLastZxid());
          LOG.debug("Got SNAPSHOT, the zxid of last transaction which is " +
              "guaranteed in log is {}.", zxid);
          // Create a temporary file for snapshot.
          File temp = persistence.createTempFile("snapshot");
          try (FileOutputStream fout = new FileOutputStream(temp)) {
            stateMachine.save(fout);
            fout.close();
            // Mark it valid.
            File file = persistence.setSnapshotFile(temp, zxid);
            Message done = MessageBuilder.buildSnapshotDone(file.getPath());
            // Sends it back to main thread.
            this.transport.send(this.serverId, done);
          }
        } else {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Got unexpected message {}.",
                     TextFormat.shortDebugString(msg));
          }
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception", e);
      throw e;
    }
    return null;
  }
}
