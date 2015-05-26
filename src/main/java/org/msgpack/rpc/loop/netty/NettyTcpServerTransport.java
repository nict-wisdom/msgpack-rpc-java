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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.msgpack.rpc.Server;
import org.msgpack.rpc.address.Address;
import org.msgpack.rpc.config.TcpServerConfig;
import org.msgpack.rpc.transport.RpcMessageHandler;
import org.msgpack.rpc.transport.ServerTransport;

class NettyTcpServerTransport implements ServerTransport {
	private ChannelFuture future = null;
	private final EventLoopGroup bossGroup = new NioEventLoopGroup(2);
	private final EventLoopGroup workerGroup = new NioEventLoopGroup(8);

	NettyTcpServerTransport(TcpServerConfig config, Server server, NettyEventLoop loop, Class<? extends RpcMessageHandler> rpcHandlerClass) {
		if (server == null) {
			throw new IllegalArgumentException("Server must not be null");
		}

		Address address = config.getListenAddress();

		try {
			RpcMessageHandler handler = rpcHandlerClass.getConstructor(Server.class).newInstance(server);
			handler.useThread(true);

			ServerBootstrap bootstrap = new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);

			bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline p = ch.pipeline();
					p.addLast("msgpack-decode-stream", new MessagePackStreamDecoder(loop.getMessagePack()));
					p.addLast("msgpack-encode", new MessagePackEncoder(loop.getMessagePack()));
					p.addLast("message", new MessageHandler(handler));
				}

			});
			bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
			bootstrap.option(ChannelOption.SO_REUSEADDR, true);
			bootstrap.option(ChannelOption.TCP_NODELAY, true);
			bootstrap.localAddress(address.getSocketAddress());
			future = bootstrap.bind();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		//		RpcMessageHandler handler = new RpcMessageHandlerEx(server);

	}

	public void close() {
		future.channel().close();
		workerGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
	}
}
