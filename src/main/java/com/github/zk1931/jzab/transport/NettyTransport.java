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

package com.github.zk1931.jzab.transport;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FileRegion;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import com.github.zk1931.jzab.MessageBuilder;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.SslParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;

/**
 * Netty-based transport.
 */
public class NettyTransport extends Transport {
  private static final Logger LOG = LoggerFactory
                                    .getLogger(NettyTransport.class);
  static final AttributeKey<Sender> SENDER = AttributeKey.valueOf("remote");

  private final String hostPort;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  Channel channel;
  private final File keyStore;
  private final char[] keyStorePassword;
  private final File trustStore;
  private final char[] trustStorePassword;
  private SSLContext clientContext;
  private SSLContext serverContext;
  private final File dir;

  // remote id => sender map.
  ConcurrentMap<String, Sender> senders =
    new ConcurrentHashMap<String, Sender>();

  public NettyTransport(String hostPort, final Receiver receiver,
                        final File dir)
      throws InterruptedException, GeneralSecurityException, IOException {
    this(hostPort, receiver, new SslParameters(), dir);
  }

  /**
   * Constructs a NettyTransport object.
   *
   * @param hostPort "hostname:port" string. The netty transport binds to the
   *                 port specified in the string.
   * @param receiver receiver callback.
   * @param sslParam Ssl parameters.
   * @param dir the directory used to store the received file.
   */
  public NettyTransport(String hostPort, final Receiver receiver,
                        SslParameters sslParam,
                        final File dir)
      throws InterruptedException, GeneralSecurityException, IOException {
    super(receiver);
    this.keyStore = sslParam.getKeyStore();
    this.trustStore = sslParam.getTrustStore();
    this.keyStorePassword = sslParam.getKeyStorePassword() != null ?
                            sslParam.getKeyStorePassword().toCharArray() : null;
    this.trustStorePassword =
      sslParam.getTrustStorePassword() != null ?
      sslParam.getTrustStorePassword().toCharArray() : null;
    this.dir = dir;
    if (isSslEnabled()) {
      initSsl();
    }

    this.hostPort = hostPort;
    String[] address = hostPort.split(":", 2);
    int port = Integer.parseInt(address[1]);
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true)
      .childOption(ChannelOption.TCP_NODELAY, true)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
          if (isSslEnabled()) {
            SSLEngine engine = serverContext.createSSLEngine();
            engine.setUseClientMode(false);
            engine.setNeedClientAuth(true);
            ch.pipeline().addLast(new SslHandler(engine));
          }
          // Incoming handlers
          ch.pipeline().addLast(new MainHandler());
          ch.pipeline().addLast(new ServerHandshakeHandler());
          ch.pipeline().addLast(new NotifyHandler());
          ch.pipeline().addLast(new ErrorHandler());
          // Outgoing handlers.
          ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
        }
      });

    // Travis build fails once in a while because it fails to bind to a port.
    // This is most likely a transient failure. Retry binding for 5 times with
    // 1 second sleep in between before giving up.
    int bindRetryCount = 5;
    for (int i = 0;; i++) {
      try {
        channel = b.bind(port).sync().channel();
        LOG.info("Server started: {}", hostPort);
        return;
      } catch (Exception ex) {
        if (i >= bindRetryCount) {
          throw ex;
        }
        LOG.debug("Failed to bind to {}. Retrying after 1 second.", hostPort);
        Thread.sleep(1000);
      }
    }
  }

  private boolean isSslEnabled() {
    return keyStore != null && trustStore != null;
  }

  private void initSsl() throws IOException, GeneralSecurityException {
    String kmAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
    String tmAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
    // TODO make the protocol and keystore type configurable.
    String protocol = "TLS";
    KeyStore ks = KeyStore.getInstance("JKS");
    KeyStore ts = KeyStore.getInstance("JKS");
    try (FileInputStream keyStoreStream = new FileInputStream(keyStore);
         FileInputStream trustStoreStream = new FileInputStream(trustStore)) {
      ks.load(keyStoreStream, keyStorePassword);
      ts.load(trustStoreStream, trustStorePassword);
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmAlgorithm);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmAlgorithm);
    kmf.init(ks, keyStorePassword);
    tmf.init(ts);
    serverContext = SSLContext.getInstance(protocol);
    clientContext = SSLContext.getInstance(protocol);
    serverContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    clientContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
  }

  /**
   * Destroys the transport.
   */
  @Override
  public void shutdown() throws InterruptedException {
    try {
      channel.close();
      for(Map.Entry<String, Sender> entry: senders.entrySet()) {
        LOG.debug("Shutting down the sender({})", entry.getKey());
        entry.getValue().shutdown();
      }
      senders.clear();
      LOG.debug("Shutdown complete");
    } finally {
      try {
        long quietPeriodSec = 0;
        long timeoutSec = 10;
        io.netty.util.concurrent.Future wf =
          workerGroup.shutdownGracefully(quietPeriodSec, timeoutSec,
                                         TimeUnit.SECONDS);
        io.netty.util.concurrent.Future bf =
          bossGroup.shutdownGracefully(quietPeriodSec, timeoutSec,
                                       TimeUnit.SECONDS);
        wf.await();
        bf.await();
        LOG.debug("Shutdown complete");
      } catch (InterruptedException ex) {
        LOG.debug("Interrupted while shutting down NioEventLoopGroup", ex);
      }
    }
  }

  /**
   * Handles server-side handshake.
   */
  private class ServerHandshakeHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      try {
        Message message = (Message)msg;
        // Make sure it's a handshake message.
        if (message .getType() != MessageType.HANDSHAKE) {
          LOG.debug("The first message from {} was not a handshake",
                    ctx.channel().remoteAddress());
          ctx.close();
          return;
        }
        String remoteId = message.getHandshake().getNodeId();
        LOG.debug("{} received handshake from {}", hostPort, remoteId);
        Sender sender = new Sender(remoteId, ctx.channel());
        Sender currentSender = senders.putIfAbsent(remoteId, sender);

        if (currentSender != null) {
          // The lock is for Sender.future.
          synchronized(currentSender) {
            if (currentSender.future == null) {
              // Means the current sender hasn't been started, we treat this as
              // tie of handshake.
              if (hostPort.compareTo(remoteId) > 0) {
                // Won the tie, this connection from the peer will be given up.
                LOG.debug("{} won the tie breaking from {}.", hostPort,
                          remoteId);
                return;
              } else {
                // Lose the tie, the connection from peer (this channel) will be
                // used as the communication channel.
                LOG.debug("{} lose the tie breaking from {}.", hostPort,
                          remoteId);
                // Transfer the message queue.
                sender.requests = currentSender.requests;
                // Use new sender to replace current sender.
                senders.put(remoteId, sender);
                // Shut down current sender.
                currentSender.shutdown();
              }
            } else {
              // The sender has started, reject the handshake.
              LOG.debug("Reject handshake from {}.", remoteId);
              ctx.close();
              return;
            }
          }
        }
        // Attach the Sender to this channel. Subsequent handlers use
        // this information to determine origins of messages.
        ctx.channel().attr(SENDER).set(sender);
        // Send a response and remove the handler from the pipeline.
        LOG.debug("Server-side handshake completed from {} to {}", hostPort,
                  remoteId);
        Message response = MessageBuilder.buildHandshake(hostPort);
        ByteBuffer buf = ByteBuffer.wrap(response.toByteArray());
        ctx.channel().writeAndFlush(Unpooled.wrappedBuffer(buf));
        sender.start();
        ctx.pipeline().remove(this);
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  private class MainHandler extends ByteToMessageDecoder {
    private FileReceiver fileReceiver = null;

    private Message decodeToMessage(ByteBuf in) {
      if (in.readableBytes() < 4) {
        return null;
      }
      in.markReaderIndex();
      int messageLength = in.readInt();
      if (in.readableBytes() < messageLength) {
        in.resetReaderIndex();
        return null;
      }
      byte[] buffer = new byte[messageLength];
      in.readBytes(buffer);
      try {
        Message msg = Message.parseFrom(buffer);
        return msg;
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Exception when parse protocol buffer.", e);
        Message msg = MessageBuilder.buildInvalidMessage(buffer);
        return msg;
      }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                          List<Object> out) throws Exception {
      if (fileReceiver == null) {
        Message msg = decodeToMessage(in);
        if (msg == null) {
          return;
        } else if (msg.getType() == MessageType.FILE_HEADER) {
          LOG.debug("Got FILE_HEADER.");
          fileReceiver = new FileReceiver(msg.getFileHeader().getLength());
        } else {
          out.add(msg);
        }
      } else {
        fileReceiver.process(in);
        if (fileReceiver.isDone()) {
          String filePath = fileReceiver.file.getPath();
          Message msg = MessageBuilder.buildFileReceived(filePath);
          out.add(msg);
          // Resets it to null to switch back to normal decode mode.
          fileReceiver = null;
        }
      }
    }

    class FileReceiver {
      final long fileLength;
      long receivedLength = 0;
      final File file;
      final FileOutputStream fout;

      public FileReceiver(long length) throws IOException {
        this.file = File.createTempFile("transport", "", dir);
        this.fileLength = length;
        this.fout = new FileOutputStream(this.file);
      }

      public void process(ByteBuf in) throws IOException {
        long readableBytes = in.readableBytes();
        long remainingBytes = fileLength - receivedLength;
        long bytesToRead =
          (remainingBytes < readableBytes)? remainingBytes : readableBytes;
        byte[] buffer = new byte[(int)bytesToRead];
        in.readBytes(buffer);
        fout.write(buffer);
        receivedLength += bytesToRead;
        if (receivedLength == fileLength) {
          fout.getChannel().force(false);
          fout.close();
        }
      }

      boolean isDone() {
        return receivedLength == fileLength;
      }
    }
  }

  private class NotifyHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) {
      String remoteId =
        ctx.channel().attr(NettyTransport.SENDER).get().destination;
      Message msg = (Message)obj;
      receiver.onReceived(remoteId, msg);
    }
  }

  /**
   * Handles errors.
   */
  private class ErrorHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      Sender sender = ctx.channel().attr(NettyTransport.SENDER).get();
      ctx.close();
      if (sender != null) {
        LOG.debug("Channel from {} is inavtive.", sender.destination);
        Sender senderInMap = senders.get(sender.destination);
        // We call onDisconnected callback if and only if the Sender of the
        // channel is still in map.
        if (senderInMap != null && senderInMap == sender) {
          // It's possible that the user calls transport.clear(serverId) first
          // and the clear call triggers the channelInactive callback, and in
          // this case either the Map doesn't contain Sender for the given
          // serverId or the user established a new Sender after calling clear(
          // so the Sender in the map doesn't match the Sender of the channel).
          // In either case we shouldn't call onDisconnected callback since the
          // disconnection is triggered by calling transport.clear.
          LOG.debug("The inactive channel is in current map, calling "
              + "onDisconnected.");
          receiver.onDisconnected(sender.destination);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      // Don't handle errors here. Call ctx.close() and let channelInactive()
      // handle all the errrors.
      LOG.debug("Caught an exception", cause);
      ctx.close();
    }
  }

  @Override
  public void send(final String destination, Message message) {
    if (destination.equals(hostPort)) {
      // The message is being sent to itself. Don't bother going over TCP.
      // Directly call onReceived.
      receiver.onReceived(destination, message);
      return;
    }
    ByteBuffer bytes = ByteBuffer.wrap(message.toByteArray());
    Sender currentSender = senders.get(destination);
    if (currentSender != null) {
      currentSender.requests.add(bytes);
    } else {
      // no connection exists.
      LOG.debug("No connection from {} to {}. Creating a new one",
                hostPort, destination);
      Sender newSender = new Sender(hostPort, destination);
      currentSender = senders.putIfAbsent(destination, newSender);
      if (currentSender == null) {
        newSender.requests.add(bytes);
        newSender.startHandshake();
      } else {
        currentSender.requests.add(bytes);
      }
    }
  }

  @Override
  public void send(final String destination, File file) {
    if (destination.equals(hostPort)) {
      LOG.error("Can't send file to itself.");
      throw new RuntimeException("Can't send file to itself.");
    }
    Sender currentSender = senders.get(destination);
    if (currentSender != null) {
      currentSender.requests.add(file);
    } else {
      // no connection exists.
      LOG.debug("No connection from {} to {}. Creating a new one",
                hostPort, destination);
      Sender newSender = new Sender(hostPort, destination);
      currentSender = senders.putIfAbsent(destination, newSender);
      if (currentSender == null) {
        newSender.requests.add(file);
        newSender.startHandshake();
      } else {
        currentSender.requests.add(file);
      }
    }
  }

  @Override
  public void clear(String destination) {
    LOG.debug("Closing the connection to {}", destination);
    Sender sender = senders.remove(destination);
    if (sender != null) {
      sender.shutdown();
    }
  }

  /**
   * sender thread.
   */
  private class Sender implements Callable<Void> {
    final String destination;
    private Bootstrap bootstrap = null;
    private Channel channel;
    Future<Void> future;
    BlockingDeque<Object> requests = new LinkedBlockingDeque<>();
    boolean shutdown = false;

    public Sender(String destination, Channel channel) {
      this.destination = destination;
      this.channel = channel;
    }

    public Sender(final String source, final String destination) {
      this.destination = destination;
      bootstrap = new Bootstrap();
      bootstrap.group(workerGroup);
      bootstrap.channel(NioSocketChannel.class);
      bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
      bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
      bootstrap.option(ChannelOption.TCP_NODELAY, true);
      bootstrap.handler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
          if (isSslEnabled()) {
            SSLEngine engine = serverContext.createSSLEngine();
            engine.setUseClientMode(true);
            ch.pipeline().addLast(new SslHandler(engine));
          }
          // Inbound handlers.
          ch.pipeline().addLast(new ReadTimeoutHandler(2));
          ch.pipeline().addLast(new MainHandler());
          ch.pipeline().addLast(new ClientHandshakeHandler());
          // Outbound handlers.
          ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
        }
      });
    }

    public synchronized void startHandshake() {
      if (this.shutdown) {
        // Since everything happens asynchronously in different threads, it's
        // possible we shutdown the Sender before handshake gets started, if
        // the Sender has been shut down, there's no reason we need to continue
        // the handshake.
        return;
      }
      String[] address = destination.split(":", 2);
      String host = address[0];
      int port = Integer.parseInt(address[1]);
      LOG.debug("host: {}, port: {}", host, port);
      bootstrap.connect(host, port).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture cfuture) {
          if (cfuture.isSuccess()) {
            LOG.debug("{} connected to {}. Sending a handshake",
                      hostPort, destination);
            channel = cfuture.channel();
            // Associates the Sender with channel once the connection
            // has been established.
            channel.attr(SENDER).set(Sender.this);
            Message msg = MessageBuilder.buildHandshake(hostPort);
            ByteBuffer bb = ByteBuffer.wrap(msg.toByteArray());
            channel.writeAndFlush(Unpooled.wrappedBuffer(bb));
          } else {
            LOG.debug("Failed to connect to {}: {}", destination,
                      cfuture.cause().getMessage());
            handshakeFailed();
          }
        }
      });
    }

    public void handshakeCompleted() {
      LOG.debug("Client handshake completed: {} => {}", hostPort, destination);
      Sender sender = senders.get(destination);
      assert sender == this;
      sender.channel.pipeline().remove(ReadTimeoutHandler.class);
      sender.channel.pipeline().addLast(new NotifyHandler());
      sender.channel.pipeline().addLast(new ErrorHandler());
      sender.start();
    }

    public void handshakeFailed() {
      LOG.debug("Client handshake failed: {} => {}", hostPort, destination);
      Sender sender = senders.get(destination);
      if (sender != null) {
        sender.shutdown();
      }
      receiver.onDisconnected(destination);
    }

    void sendFile(File file) throws Exception {
      long length = file.length();
      LOG.debug("Got request of sending file {} of length {}.",
                file, length);
      Message handshake = MessageBuilder.buildFileHeader(length);
      byte[] bytes = handshake.toByteArray();
      // Sends HANDSHAKE first before transferring actual file data, the
      // HANDSHAKE will tell the peer's channel to prepare for the file
      // transferring.
      channel.writeAndFlush(Unpooled.wrappedBuffer(bytes)).sync();
      ChannelHandler prepender = channel.pipeline().get("frameEncoder");
      // Removes length prepender, we don't need this handler for file
      // transferring.
      channel.pipeline().remove(prepender);
      // Adds ChunkedWriteHandler for file transferring.
      ChannelHandler cwh = new ChunkedWriteHandler();
      channel.pipeline().addLast(cwh);
      // Begins file transferring.
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      if (channel.pipeline().get(SslHandler.class) != null) {
        // Zero-Copy file transferring is not supported for ssl.
        channel.writeAndFlush(new ChunkedFile(raf, 0, length, 8912));
      } else {
        // Use Zero-Copy file transferring in non-ssl mode.
        FileRegion region = new DefaultFileRegion(raf.getChannel(), 0, length);
        channel.writeAndFlush(region);
      }
      // Restores pipeline to original state.
      channel.pipeline().remove(cwh);
      channel.pipeline().addLast("frameEncoder", prepender);
    }

    @Override
    public Void call() throws Exception {
      LOG.debug("Started the sender: {} => {}", hostPort, destination);
      try {
        while (true) {
          Object req  = requests.take();
          if (req instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer)req;
            channel.writeAndFlush(Unpooled.wrappedBuffer(buf));
          } else if (req instanceof File) {
            File file = (File)req;
            sendFile(file);
          } else if (req instanceof Shutdown) {
            LOG.debug("Got shutdown request.");
            break;
          }
        }
      } catch (InterruptedException ex) {
        LOG.debug("Sender to {} got interrupted", destination);
        return null;
      } catch (Exception ex) {
        LOG.warn("Sender failed with an exception", ex);
        throw ex;
      } finally {
        channel.close();
      }
      return null;
    }

    public synchronized void start() {
      if (this.shutdown) {
        return;
      }
      ExecutorService es = Executors.newSingleThreadExecutor();
      future = es.submit(this);
      es.shutdown();
    }

    public synchronized void shutdown() {
      LOG.debug("Shutting down the sender: {} => {}", hostPort, destination);
      try {
        this.shutdown = true;
        if (future != null) {
          try {
            this.requests.add(new Shutdown());
            future.get();
          } catch (InterruptedException | ExecutionException ex) {
            LOG.debug("Ignore the exception", ex);
          }
        }
        if (channel != null) {
          channel.close().syncUninterruptibly();
        }
      } catch (RejectedExecutionException ex) {
        LOG.debug("Ignoring rejected execution exception", ex);
      }
    }

    class Shutdown {
      // We use it to shutdown the sender thread.
    }

    /**
     * Handles client-side handshake.
     */
    public class ClientHandshakeHandler extends ChannelInboundHandlerAdapter {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg)
          throws Exception {
        try {
          Message message = (Message)msg;
          if (message.getType() != MessageType.HANDSHAKE) {
            // Server responded with an invalid message.
            LOG.error("The first message from %s was not a handshake: %s",
                      ctx.channel().remoteAddress(),
                      TextFormat.shortDebugString(message));
            ctx.close();
            return;
          }

          String response = message.getHandshake().getNodeId();
          if (!response.equals(destination)) {
            // Handshake response doesn't match server's node ID.
            LOG.error("Invalid handshake response from %s: %s", destination,
                      response);
            ctx.close();
            return;
          }

          // Handshake is finished. Remove the handler from the pipeline.
          ctx.pipeline().remove(this);
          handshakeCompleted();
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Don't call the handshake callback here. Simply close the context and
        // let channelInactive() call the handshake callback.
        LOG.debug("Caught an exception", cause);
        ctx.close();
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("Channel from {} is closed.", destination);
        ctx.close();
        Sender sender = ctx.channel().attr(SENDER).get();
        if (sender != null && sender == senders.get(sender.destination)) {
          // sender != null means the connection of this channel has not been
          // established.
          // sender == senders.get(sender.destination) means at least there's
          // no tie breaking for now.
          // In either way we need to call handshakeFailed and trigger the
          // onDisconnected callback.
          handshakeFailed();
        } else {
          LOG.debug("The channel is closed due to losing the tie breaking.");
        }
      }
    }
  }
}
