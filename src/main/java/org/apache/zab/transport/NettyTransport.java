/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zab.transport;

import com.google.protobuf.TextFormat;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.zab.MessageBuilder;
import org.apache.zab.proto.ZabMessage.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.zab.proto.ZabMessage.Message.MessageType;

/**
 * Netty-based transport.
 */
public class NettyTransport extends Transport {
  private static final Logger LOG = LoggerFactory
                                    .getLogger(NettyTransport.class);
  static final AttributeKey<String> REMOTE_ID = AttributeKey.valueOf("remote");

  private final String hostPort;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  final Channel channel;

  // remote id => sender map.
  ConcurrentMap<String, Sender> senders =
    new ConcurrentHashMap<String, Sender>();

  public NettyTransport(String hostPort, final Receiver receiver)
      throws InterruptedException, UnknownHostException {
    super(receiver);
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
          // Incoming handlers
          ch.pipeline().addLast(
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
          ch.pipeline().addLast(new ServerHandshakeHandler());
          ch.pipeline().addLast(new ByteBufferHandler());
          ch.pipeline().addLast(new ErrorHandler());
          // Outgoing handlers.
          ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
        }
      });
    channel = b.bind(port).sync().channel();
    LOG.info("Server started: {}", hostPort);
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
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }

  /**
   * Handles server-side handshake.
   */
  public class ServerHandshakeHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      try {
        // Parses it to protocol message.
        ByteBuf bb = (ByteBuf)msg;
        LOG.debug("Received a message: {}", bb);
        byte[] buffer = new byte[bb.nioBuffer().remaining()];
        bb.nioBuffer().get(buffer);
        Message message = Message.parseFrom(buffer);

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
        // Attach the remote node id to this channel. Subsequent handlers use
        // this information to determine origins of messages.
        ctx.channel().attr(REMOTE_ID).set(remoteId);
        Sender currentSender = senders.putIfAbsent(remoteId, sender);

        if (currentSender != null) {
          LOG.debug("Rejecting a handshake from {}", remoteId);
          ctx.close();
          return;
        }

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

  /**
   * This handler converts incoming messages to ByteBuffer and calls
   * Transport.Receiver.onReceived() method.
   */
  public class ByteBufferHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf bb = (ByteBuf)msg;
      try {
        String remoteId = ctx.channel().attr(NettyTransport.REMOTE_ID).get();
        // TODO avoid copying ByteBuf to ByteBuffer.
        byte[] bytes = new byte[bb.readableBytes()];
        bb.readBytes(bytes);
        if (receiver != null) {
          receiver.onReceived(remoteId, ByteBuffer.wrap(bytes));
        }
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  /**
   * Handles errors.
   */
  public class ErrorHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      String remoteId = ctx.channel().attr(NettyTransport.REMOTE_ID).get();
      ctx.close();
      if (remoteId != null) {
        LOG.debug("Got disconnected from {}.", remoteId);
        // This must not be null.
        Sender sender = senders.get(remoteId);
        if (sender != null) {
          sender.shutdown();
        }
        receiver.onDisconnected(remoteId);
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
  public void send(final String destination, ByteBuffer message) {
    if (destination.equals(hostPort)) {
      // The message is being sent to itself. Don't bother going over TCP.
      // Directly call onReceived.
      receiver.onReceived(destination, message);
      return;
    }

    Sender currentSender = senders.get(destination);
    if (currentSender != null) {
      currentSender.requests.add(message);
    } else {
      // no connection exists.
      LOG.debug("No connection from {} to {}. Creating a new one",
                hostPort, destination);
      Sender newSender = new Sender(hostPort, destination);
      currentSender = senders.putIfAbsent(destination, newSender);
      if (currentSender == null) {
        newSender.requests.add(message);
        newSender.startHandshake();
      } else {
        currentSender.requests.add(message);
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
  public class Sender implements Callable<Void> {
    private final String destination;
    private Bootstrap bootstrap = null;
    private Channel channel;
    private Future<Void> future;
    private EventLoopGroup workerGroup = new NioEventLoopGroup();
    final BlockingDeque<ByteBuffer> requests = new LinkedBlockingDeque<>();

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
          // Inbound handlers.
          ch.pipeline().addLast(new ReadTimeoutHandler(2));
          ch.pipeline().addLast(
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
          ch.pipeline().addLast(new ClientHandshakeHandler());
          // Outbound handlers.
          ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
        }
      });
    }

    public void startHandshake() {
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
            Message msg = MessageBuilder.buildHandshake(hostPort);
            ByteBuffer bb = ByteBuffer.wrap(msg.toByteArray());
            channel = cfuture.channel();
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
      sender.channel.attr(REMOTE_ID).set(destination);
      sender.channel.pipeline().remove(ReadTimeoutHandler.class);
      sender.channel.pipeline().addLast(new ByteBufferHandler());
      sender.channel.pipeline().addLast(new ErrorHandler());
      sender.start();
    }

    public void handshakeFailed() {
      LOG.debug("Client handshake failed: {} => {}", hostPort, destination);
      Sender sender = senders.get(destination);
      assert sender == this;
      sender.shutdown();
      receiver.onDisconnected(destination);
    }

    @Override
    public Void call() throws Exception {
      LOG.debug("Started the sender: {} => {}", hostPort, destination);
      try {
        while (true) {
          ByteBuffer buf = requests.take();
          channel.writeAndFlush(Unpooled.wrappedBuffer(buf));
        }
      } catch (InterruptedException ex) {
        LOG.debug("Sender to {} got interrupted", destination);
        return null;
      } catch (Exception ex) {
        LOG.warn("Sender failed with an exception", ex);
        throw ex;
      } finally {
        channel.close().syncUninterruptibly();
      }
    }

    public void start() {
      future = Executors.newSingleThreadExecutor().submit(this);
    }

    public void shutdown() {
      LOG.debug("Shutting down the sender: {} => {}", hostPort, destination);
      try {
        if (future != null) {
          future.cancel(true);
        }
        if (channel != null) {
          channel.close().syncUninterruptibly();
        }
      } finally {
        workerGroup.shutdownGracefully();
      }
    }

    /**
     * Handles client-side handshake.
     */
    public class ClientHandshakeHandler extends ChannelInboundHandlerAdapter {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg)
          throws Exception {
        try {
          // Parse the message.
          ByteBuf bb = (ByteBuf)msg;
          LOG.debug("Received a message: {}", bb);
          byte[] buffer = new byte[bb.nioBuffer().remaining()];
          bb.nioBuffer().get(buffer);
          Message message = Message.parseFrom(buffer);

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
        LOG.debug("Got disconnected from {}", destination);
        ctx.close();
        handshakeFailed();
      }
    }
  }
}
