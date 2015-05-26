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

package org.msgpack.rpc.extension.socks;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socks.SocksAddressType;
import io.netty.handler.codec.socks.SocksAuthScheme;
import io.netty.handler.codec.socks.SocksCmdRequest;
import io.netty.handler.codec.socks.SocksCmdResponse;
import io.netty.handler.codec.socks.SocksCmdResponseDecoder;
import io.netty.handler.codec.socks.SocksCmdStatus;
import io.netty.handler.codec.socks.SocksCmdType;
import io.netty.handler.codec.socks.SocksInitRequest;
import io.netty.handler.codec.socks.SocksResponse;
import io.netty.handler.codec.socks.SocksResponseType;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.msgpack.rpc.Session;
import org.msgpack.rpc.address.IPAddress;
import org.msgpack.rpc.loop.netty.NettyTcpClientTransport;

/**
 * SocksProxy用Handler.
 * Socks5/認証無し専用
 * @author kishimoto
 *
 */
@ChannelHandler.Sharable
public class SocksProxyHandler extends SimpleChannelInboundHandler<SocksResponse> {

	private Session session = null;
	private NettyTcpClientTransport clientTransport = null;

	public SocksProxyHandler() {
		super();
	}

	public SocksProxyHandler(Session session,NettyTcpClientTransport clientTransport) {
		super();
		this.session = session;
		this.clientTransport = clientTransport;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, SocksResponse response) throws Exception {

		SocksResponseType rt = response.responseType();

		switch (rt) {

		case INIT:
			ctx.pipeline().addFirst("socks-cmd-decoder", new SocksCmdResponseDecoder());
			InetSocketAddress addr = ((IPAddress) session.getAddress()).getInetSocketAddress();
			SocksCmdRequest cmdSocks = new SocksCmdRequest(SocksCmdType.CONNECT, SocksAddressType.DOMAIN, addr.getHostName(), addr.getPort());
			ctx.writeAndFlush(cmdSocks);
			break;

		case AUTH:
			break;
		case CMD:
			SocksCmdResponse scr = (SocksCmdResponse) response;
			ctx.pipeline().remove(this);
			ctx.pipeline().remove("socks-encode");
			if (scr.cmdStatus() != SocksCmdStatus.SUCCESS) {
				throw new ChannelException("Socks faild.");
			}
			clientTransport.onSocksConnected(ctx.channel());
			break;
		case UNKNOWN:
		default:
			throw new ChannelException("No support Socks Command.");
		}
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		List<SocksAuthScheme> lstAuth = new ArrayList<>();
		lstAuth.add(SocksAuthScheme.NO_AUTH);
		SocksInitRequest si = new SocksInitRequest(lstAuth);
		ctx.writeAndFlush(si);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		clientTransport.onError(ctx.channel(), cause.toString());
	}

}
