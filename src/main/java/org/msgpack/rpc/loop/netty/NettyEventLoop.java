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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.msgpack.MessagePack;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.Session;
import org.msgpack.rpc.config.TcpClientConfig;
import org.msgpack.rpc.config.TcpServerConfig;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.rpc.transport.ClientTransport;
import org.msgpack.rpc.transport.RpcMessageHandler;
import org.msgpack.rpc.transport.ServerTransport;

public class NettyEventLoop extends EventLoop {

	private Class<? extends RpcMessageHandler> handler = RpcMessageHandler.class;

	public NettyEventLoop(ExecutorService workerExecutor, ExecutorService ioExecutor, ScheduledExecutorService scheduledExecutor, MessagePack messagePack) {
		super(workerExecutor, ioExecutor, scheduledExecutor, messagePack);
	}

	public NettyEventLoop(ExecutorService workerExecutor, ExecutorService ioExecutor,
			ScheduledExecutorService scheduledExecutor, MessagePack messagePack, Class<? extends RpcMessageHandler> handlerClass) {
		this(workerExecutor, ioExecutor, scheduledExecutor, messagePack);
		this.handler = handlerClass;
	}

	protected ClientTransport openTcpTransport(TcpClientConfig config, Session session) {
		return new NettyTcpClientTransport(config, session, this, handler);
	}

	protected ServerTransport listenTcpTransport(TcpServerConfig config, Server server) {
		return new NettyTcpServerTransport(config, server, this, handler);
	}
}
