//
// MessagePack-RPC for Java
//
// Copyright (C) 2010 FURUHASHI Sadayuki
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

/*
* Copyright (C) 2014-2015 Information Analysis Laboratory, NICT
*
* RaSC is free software: you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 2.1 of the License, or (at
* your option) any later version.
*
* RaSC is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
* General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package org.msgpack.rpc.loop.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.socks.SocksInitResponseDecoder;
import io.netty.handler.codec.socks.SocksMessageEncoder;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.msgpack.rpc.Session;
import org.msgpack.rpc.address.IPAddress;
import org.msgpack.rpc.config.TcpClientConfig;
import org.msgpack.rpc.extension.socks.SocksProxyHandler;
import org.msgpack.rpc.transport.PooledStreamClientTransport;
import org.msgpack.rpc.transport.RpcMessageHandler;

public class NettyTcpClientTransport extends PooledStreamClientTransport<Channel, ByteBufOutputStream> {
	private static final InternalLogger LOG = InternalLoggerFactory.getInstance(NettyTcpClientTransport.class);
	private final Bootstrap bootstrap;
	private final EventLoopGroup group = new NioEventLoopGroup(maxChannel);
	private RpcMessageHandler handler = null;
	private NettyEventLoop eventLoop = null;

	NettyTcpClientTransport(TcpClientConfig config, Session session, NettyEventLoop loop, Class<? extends RpcMessageHandler> rpcHandlerClass) {
		super(config, session);

		try {
			handler = rpcHandlerClass.getConstructor(Session.class).newInstance(session);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		//		handler = new RpcMessageHandlerEx(session);
		eventLoop = loop;
		bootstrap = new Bootstrap().group(group);
		bootstrap.channel(NioSocketChannel.class);
		final NettyTcpClientTransport trans = this;
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				if (isSocks) {
					p.addFirst("socks-handler", new SocksProxyHandler(session, trans));
					p.addFirst("socks-encode", new SocksMessageEncoder());
					p.addFirst("socks-decode", new SocksInitResponseDecoder());
				}
				p.addLast("msgpack-decode-stream", new MessagePackStreamDecoder(eventLoop.getMessagePack()));
				p.addLast("msgpack-encode", new MessagePackEncoder(eventLoop.getMessagePack()));
				p.addLast("message", new MessageHandler(handler, trans));
			}

		});
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
	}

	@Override
	protected ByteBufOutputStream newPendingBuffer() {
		return new ByteBufOutputStream(Unpooled.buffer());
	}

	@Override
	protected void sendMessageChannel(Channel c, Object msg) {
		c.writeAndFlush(msg);
	}

	@Override
	protected void closeChannel(Channel c) {
		c.close();
	}

	@Override
	protected void resetPendingBuffer(ByteBufOutputStream b) {
		b.buffer().clear();

	}

	@Override
	protected void flushPendingBuffer(ByteBufOutputStream b, Channel c) {
		c.writeAndFlush(b.buffer());
		b.buffer().clear();

	}

	@Override
	protected void closePendingBuffer(ByteBufOutputStream b) {
		b.buffer().clear();

	}

	@Override
	public void close() {
		super.close();
		group.shutdownGracefully();
	}

	@Override
	protected void startConnection() {

		try {
			SocketAddress addr = (isSocks) ?
					new IPAddress(System.getProperty("socksProxyHost", "localhost"), Integer.valueOf(System.getProperty("socksProxyPort", "0"))).getSocketAddress() :
					session.getAddress().getSocketAddress();

			ChannelFuture f = bootstrap.connect(addr);

			f.addListener((ChannelFutureListener) future -> {

				if (!future.isSuccess()) {
					onConnectFailed(future.channel(), future.cause());
					return;
				}

				Channel c = f.channel();

				c.closeFuture().addListener((ChannelFutureListener) closeFt -> {
					Channel ch = closeFt.channel();
					onClosed(ch);
				});

				//				MessageChannel mc = (MessageChannel) f.channel();

				if (isSocks) {
					//					mc.setUseSocksProxy(isSocks);
					LOG.debug("--- useSocksProxy ----");
					LOG.debug(System.getProperty("socksProxyHost"));
					LOG.debug(System.getProperty("socksProxyPort"));

				}
			});

		} catch (NumberFormatException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
	}

	public void onSocksConnected(Channel c) {
		ChannelPipeline p = c.pipeline();
		p.fireChannelActive();
	}

}
