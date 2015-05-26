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

package org.msgpack.rpc.transport;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.msgpack.MessagePack;
import org.msgpack.rpc.Session;
import org.msgpack.rpc.config.StreamClientConfig;

public abstract class PooledStreamClientTransport<Channel, PendingBuffer extends OutputStream> implements ClientTransport {
	private static final InternalLogger LOG = InternalLoggerFactory.getInstance(PooledStreamClientTransport.class);
	private static final int MAX_CHANNEL = 2;

	private final List<Channel> pool = new CopyOnWriteArrayList<>();
	private final List<Channel> errorChannelPool = new CopyOnWriteArrayList<>();
	private final AtomicInteger connecting = new AtomicInteger();
	private final Queue<Object> queMessage = new ConcurrentLinkedQueue<>();
	private final AtomicInteger assignIndex = new AtomicInteger();
	protected final Session session;
	protected final StreamClientConfig config;
	protected final MessagePack messagePack;
	protected final int maxChannel = Objects.isNull(System.getProperty("maxChannel")) ? MAX_CHANNEL : Integer.valueOf(System.getProperty("maxChannel"));
	protected final boolean isSocks =   (Objects.nonNull(System.getProperty("socksProxyHost")) && Objects.nonNull(System.getProperty("socksProxyPort")));

	public PooledStreamClientTransport(StreamClientConfig config, Session session) {
		this.session = session;
		this.config = config;
		this.messagePack = session.getEventLoop().getMessagePack();
	}

	protected Session getSession() {
		return session;
	}

	protected StreamClientConfig getConfig() {
		return config;
	}

	public synchronized void sendMessage(Object msg) {
		if (connecting.get() == -1) {
			return;
		} // already closed

			if (connecting.get() <= 0) {
				if (connecting.get() < maxChannel) {
					connecting.incrementAndGet();
					startConnection();
					queMessage.offer(msg);
				} else {
					queMessage.offer(msg);
				}
			} else {
				if (connecting.get() >= maxChannel) {
					if (!pool.isEmpty()) {
						int index = assignIndex.getAndIncrement() % pool.size();
						if (index >= Integer.MAX_VALUE) {
							assignIndex.set(0);
						}
						Channel c = pool.get(index);
						sendMessageChannel(c, msg);
						flushMessageQue(c);
					} else {
						queMessage.offer(msg);
					}
				} else {
					connecting.incrementAndGet();
					startConnection();
					queMessage.offer(msg);
				}
			}
	}

	public void close() {
		LOG.debug("Close all channels");
		connecting.set(-1);
		for (Channel c : pool) {
			closeChannel(c);
		}
		for (Channel c : errorChannelPool) {
			closeChannel(c);
		}
		pool.clear();
		errorChannelPool.clear();
		queMessage.clear();
	}

	public void onConnected(Channel c) {
		if (connecting.get() == -1) {
			closeChannel(c);
			return;
		} // already closed

		LOG.debug("Success to connect new channel " + c);
		LOG.debug(Objects.toString(c));
		pool.add(c);
		flushMessageQue(c);
	}

	public void onConnectFailed(Channel c, Throwable cause) {
		if (connecting.get() == -1) {
			return;
		} // already closed
		LOG.error(String.format("Fail to connect %s", c), cause);
		connecting.set(pool.size());
		session.transportConnectFailed();
		closeChannel(c);
	}

	public void onClosed(Channel c) {
		if (connecting.get() == -1) {
			return;
		} // already closed
		LOG.debug(String.format("Close channel %s", c));
		pool.remove(c);
		errorChannelPool.remove(c);
		connecting.set(pool.size());
	}

	@Override
	public void sendDataDelay(Object obj) {

	}

	public void onError(Channel c, String msg) {
		if (connecting.get() == -1) {
			return;
		} // already closed
		LOG.info(String.format("Error channel %s", c));
		closeChannel(c);
		session.transportError(msg);

	}

	protected synchronized void flushMessageQue(Channel c) {
		if (!queMessage.isEmpty()) {
			queMessage.forEach(m -> sendMessageChannel(c, m));
			queMessage.clear();
		}
	}

	protected abstract PendingBuffer newPendingBuffer();

	protected abstract void resetPendingBuffer(PendingBuffer b);

	protected abstract void flushPendingBuffer(PendingBuffer b, Channel c);

	protected abstract void closePendingBuffer(PendingBuffer b);

	protected abstract void sendMessageChannel(Channel c, Object msg);

	protected abstract void closeChannel(Channel c);

	protected abstract void startConnection();

}
