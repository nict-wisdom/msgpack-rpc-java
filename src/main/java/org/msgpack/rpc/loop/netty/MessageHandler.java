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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Objects;

import org.msgpack.rpc.transport.RpcMessageHandler;
import org.msgpack.type.Value;

class MessageHandler extends SimpleChannelInboundHandler<Value> {
    private RpcMessageHandler handler;
    private ChannelAdaptor adaptor;
    private NettyTcpClientTransport clientTransport = null;

    MessageHandler(RpcMessageHandler handler) {
        this.handler = handler;
    }

    public MessageHandler(RpcMessageHandler handler,NettyTcpClientTransport clientTransport) {
        this.handler = handler;
        this.clientTransport = clientTransport;

    }

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Value msg) throws Exception {
        handler.handleMessage(adaptor, msg);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.adaptor = new ChannelAdaptor(ctx);
        clientTransport.onConnected(ctx.channel());
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if(Objects.nonNull(clientTransport)){
			clientTransport.onError(ctx.channel(), cause.getMessage());
		}
	}

}
