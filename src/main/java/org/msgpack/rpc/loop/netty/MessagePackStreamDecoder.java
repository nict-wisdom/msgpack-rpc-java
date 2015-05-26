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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.List;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

public class MessagePackStreamDecoder extends ByteToMessageDecoder {
	protected MessagePack msgpack;

	public MessagePackStreamDecoder(MessagePack msgpack) {
		super();
		this.msgpack = msgpack;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> paramList) throws Exception {

		if (msg.isReadable()) {
			ByteBuffer buffer = msg.nioBuffer();
			Unpacker unpacker = msgpack.createBufferUnpacker(buffer);
			int lastPos = 0;
			try {
				while (buffer.position() < buffer.limit()) {
					Value v = unpacker.readValue();
					paramList.add(v);
					lastPos = buffer.position();
				}
				msg.skipBytes(lastPos);
			} catch (EOFException e) {
				msg.skipBytes(lastPos);
			}
		}
	}

}
