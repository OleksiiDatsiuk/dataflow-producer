package org.arpha;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;

public class Producer {

    private final String clusterHost;
    private final int clusterPort;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Producer(String clusterHost, int clusterPort) {
        this.clusterHost = clusterHost;
        this.clusterPort = clusterPort;
    }

    public void send(String topic, Object payload) {
        String messageJson;
        try {
            String payloadAsString = payload instanceof String ? (String) payload : objectMapper.writeValueAsString(payload);
            messageJson = objectMapper.writeValueAsString(new MessageWrapper(topic, payloadAsString));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8), new StringDecoder(StandardCharsets.UTF_8));
                        }
                    });

            ChannelFuture future = bootstrap.connect(clusterHost, clusterPort).sync();
            future.channel().writeAndFlush(messageJson + "\n").sync();
            future.channel().close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            //ignored
        } finally {
            group.shutdownGracefully();
        }
    }

}
