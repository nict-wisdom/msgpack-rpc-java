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

package org.msgpack.rpc.config;

import io.netty.channel.ChannelOption;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ClientConfig {
    private Map<ChannelOption<?>,Object> options = new ConcurrentHashMap<>();
    protected int requestTimeout = 30; // FIXME default timeout time

    public void setRequestTimeout(int sec) {
        this.requestTimeout = sec;
    }

    public int getRequestTimeout() {
        return this.requestTimeout;
    }

    @SuppressWarnings("unchecked")
	public<T> T getOption(ChannelOption<T> key) {
        return (T)options.get(key);
    }

    public Map<ChannelOption<?>,Object> getOptions() {
        return options;
    }

    public<T> void setOption(ChannelOption<T> key, T value) {
        options.put(key, value);
    }
}
