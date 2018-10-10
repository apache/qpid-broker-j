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

package org.apache.qpid.server.management.plugin.portunification;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.eclipse.jetty.io.AbstractConnection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.test.utils.UnitTestBase;

public class TlsOrPlainConnectionFactoryTest extends UnitTestBase
{
    private SslContextFactory _sslContextFactory;
    private Connector _connector;
    private EndPoint _endPoint;
    private AbstractConnection _actualConnection;

    private TlsOrPlainConnectionFactory _factory;

    @Before
    public void setUp() throws Exception
    {

        _sslContextFactory = mock(SslContextFactory.class);
        SSLEngine sslEngine = mock(SSLEngine.class);
        when(_sslContextFactory.newSSLEngine(any())).thenReturn(sslEngine);
        final SSLSession sslSession = mock(SSLSession.class);
        when(sslEngine.getSession()).thenReturn(sslSession);
        when(sslSession.getPacketBufferSize()).thenReturn(Integer.MAX_VALUE);

        _factory = new TlsOrPlainConnectionFactory(_sslContextFactory, "test");

        _actualConnection = mock(AbstractConnection.class);

        _connector = mock(Connector.class);
        when(_connector.getExecutor()).thenReturn(mock(Executor.class));
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        when(_connector.getConnectionFactory(anyString())).thenReturn(connectionFactory);
        when(connectionFactory.newConnection(any(), any())).thenReturn(_actualConnection);

        _endPoint = mock(EndPoint.class);
    }

    @Test
    public void testOnFillableForTLS() throws Exception
    {
        AtomicBoolean firstPart = new AtomicBoolean(true);
        Answer<Object> answer = (InvocationOnMock invocation) ->
        {
            ByteBuffer dst =
                    (ByteBuffer) invocation.getArguments()[0];
            if (firstPart.get())
            {
                firstPart.set(false);
                return writeBytes(dst,
                                  (byte) 22,
                                  (byte) 3,
                                  (byte) 1);
            }
            return writeBytes(dst,
                              (byte) 0,
                              (byte) 0,
                              (byte) 1);
        };
        when(_endPoint.fill(any(ByteBuffer.class))).thenAnswer(answer);

        TlsOrPlainConnectionFactory.PlainOrTlsConnection connection = _factory.newConnection(_connector, _endPoint);

        connection.onFillable();

        verify(_endPoint).fillInterested(any(Callback.class));

        connection.onFillable();

        verify(_actualConnection).onOpen();
        verify(_sslContextFactory).newSSLEngine(any());

        ByteBuffer buffer = BufferUtil.allocate(4);
        int result = connection.getEndPoint().fill(buffer);
        assertEquals((long) 4, (long) result);

        assertTrue(Arrays.equals(new byte[]{(byte) 22, (byte) 3, (byte) 1, (byte) 0}, buffer.array()));
        buffer = BufferUtil.allocate(2);

        result = connection.getEndPoint().fill(buffer);
        assertEquals((long) 2, (long) result);
        assertTrue(Arrays.equals(new byte[]{(byte) 0, (byte) 1}, buffer.array()));
        verify(_endPoint, times(3)).fill(any());
    }

    @Test
    public void testOnFillableForPlain() throws Exception
    {
        AtomicBoolean firstPart = new AtomicBoolean(true);
        Answer<Object> answer = (InvocationOnMock invocation) ->
        {
            ByteBuffer dst =
                    (ByteBuffer) invocation.getArguments()[0];
            if (firstPart.get())
            {
                firstPart.set(false);
                return writeBytes(dst,
                                  "HTTP 1".getBytes());
            }
            return writeBytes(dst,
                              ".1\n\n".getBytes());
        };
        when(_endPoint.fill(any(ByteBuffer.class))).thenAnswer(answer);

        TlsOrPlainConnectionFactory.PlainOrTlsConnection connection = _factory.newConnection(_connector, _endPoint);

        connection.onFillable();

        verify(_actualConnection).onOpen();
        verify(_sslContextFactory, times(0)).newSSLEngine(any());
        verify(_endPoint).fill(any());

        ByteBuffer buffer = BufferUtil.allocate(4);
        int result = connection.getEndPoint().fill(buffer);
        assertEquals((long) 4, (long) result);

        assertEquals("HTTP", new String(buffer.array()));

        buffer = BufferUtil.allocate(6);
        result = connection.getEndPoint().fill(buffer);
        assertEquals((long) 6, (long) result);
        assertEquals(" 1.1\n\n", new String(buffer.array()));
        verify(_endPoint, times(2)).fill(any());
    }

    private int writeBytes(ByteBuffer dst, byte... toWrite)
    {
        int written = 0;
        if (BufferUtil.space(dst) > 0)
        {
            final int pos = BufferUtil.flipToFill(dst);
            try
            {
                for (; written < toWrite.length; written++)
                {
                    dst.put(toWrite[written]);
                }
            }
            finally
            {
                BufferUtil.flipToFlush(dst, pos);
            }
        }
        return written;
    }
}