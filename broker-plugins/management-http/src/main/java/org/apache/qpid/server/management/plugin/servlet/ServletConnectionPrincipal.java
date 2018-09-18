/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.management.plugin.servlet;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.auth.ManagementConnectionPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;

public class ServletConnectionPrincipal implements ManagementConnectionPrincipal
{
    private static final long serialVersionUID = 1L;
    private static final String UTF8 = StandardCharsets.UTF_8.name();
    private static final int HASH_TRUNCATION_LENGTH = 8;

    private final InetSocketAddress _address;
    private final String _sessionId;
    private ServletRequestMetaData _metadata;

    public ServletConnectionPrincipal(HttpServletRequest request)
    {
        _address = new InetSocketAddress(request.getRemoteHost(), request.getRemotePort());
        _metadata = new ServletRequestMetaData(request);
        HttpSession session =  request.getSession(false);
        if (session != null)
        {
            MessageDigest md;
            try
            {
                md = MessageDigest.getInstance("SHA-256");
                md.update(session.getId().getBytes(UTF8));
            }
            catch (NoSuchAlgorithmException | UnsupportedEncodingException e)
            {
                throw new RuntimeException("Cannot create SHA-256 hash", e);
            }
            byte[] digest = md.digest();
            _sessionId = Base64.getEncoder().encodeToString(digest).substring(0, HASH_TRUNCATION_LENGTH);
        }
        else
        {
            _sessionId = null;
        }
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        return _address;
    }

    @Override
    public SocketConnectionMetaData getConnectionMetaData()
    {
        return _metadata;
    }

    @Override
    public String getName()
    {
        return _address.toString();
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final ServletConnectionPrincipal that = (ServletConnectionPrincipal) o;

        if (!_address.equals(that._address))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return _address.hashCode();
    }

    @Override
    public String getType()
    {
        return "HTTP";
    }

    @Override
    public String getSessionId()
    {
        return _sessionId;
    }


    private static class ServletRequestMetaData implements SocketConnectionMetaData
    {
        private final HttpServletRequest _request;

        public ServletRequestMetaData(final HttpServletRequest request)
        {
            _request = request;
        }

        @Override
        public Port getPort()
        {
            return HttpManagementUtil.getPort(_request);
        }

        @Override
        public String getLocalAddress()
        {
            return _request.getServerName() + ":" + _request.getServerPort();
        }

        @Override
        public String getRemoteAddress()
        {
            return _request.getRemoteHost() + ":" + _request.getRemotePort();
        }

        @Override
        public Protocol getProtocol()
        {
            return Protocol.HTTP;
        }

        @Override
        public Transport getTransport()
        {
            return _request.isSecure() ? Transport.SSL : Transport.TCP;
        }

        public String getHttpProtocol()
        {
            return _request.getProtocol();
        }
    }
}
