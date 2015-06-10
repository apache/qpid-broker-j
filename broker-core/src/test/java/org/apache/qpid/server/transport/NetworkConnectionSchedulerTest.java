/*
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
 */

package org.apache.qpid.server.transport;

import org.apache.qpid.server.protocol.MultiVersionProtocolEngine;
import org.apache.qpid.server.protocol.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.protocol.ServerProtocolEngine;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.NetworkTransportConfiguration;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.TransportEncryption;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class NetworkConnectionSchedulerTest extends QpidTestCase
{
    private volatile boolean _keepRunningThreads = true;

    public void testFairRead() throws IOException, InterruptedException
    {
        NetworkTransportConfiguration config = new NetworkTransportConfiguration()
        {

            @Override
            public boolean getTcpNoDelay()
            {
                return true;
            }

            @Override
            public int getReceiveBufferSize()
            {
                return 1;
            }

            @Override
            public int getSendBufferSize()
            {
                return 1;
            }

            @Override
            public int getThreadPoolSize()
            {
                return 1;
            }

            @Override
            public InetSocketAddress getAddress()
            {
                return new InetSocketAddress(0);
            }

            @Override
            public boolean needClientAuth()
            {
                return false;
            }

            @Override
            public boolean wantClientAuth()
            {
                return false;
            }

            @Override
            public Collection<String> getEnabledCipherSuites()
            {
                return Collections.emptyList();
            }

            @Override
            public Collection<String> getDisabledCipherSuites()
            {
                return Collections.emptyList();
            }
        };
        MultiVersionProtocolEngineFactory engineFactory = mock(MultiVersionProtocolEngineFactory.class);
        MultiVersionProtocolEngine verboseEngine = mock(MultiVersionProtocolEngine.class);
        MultiVersionProtocolEngine timidEngine = mock(MultiVersionProtocolEngine.class);

        when(engineFactory.newProtocolEngine(any(SocketAddress.class))).thenReturn(verboseEngine).thenReturn(timidEngine);
        when(verboseEngine.getAggregateTicker()).thenReturn(new AggregateTicker());
        when(timidEngine.getAggregateTicker()).thenReturn(new AggregateTicker());

        NonBlockingNetworkTransport transport = new NonBlockingNetworkTransport(config, engineFactory, null, EnumSet.of(TransportEncryption.NONE));

        transport.start();
        final int port = transport.getAcceptingPort();

        Socket verboseSocket = new Socket();
        verboseSocket.connect(new InetSocketAddress(port));
        final OutputStream verboseOutputStream = verboseSocket.getOutputStream();
        Thread verboseSender = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    while (_keepRunningThreads)
                    {
                        verboseOutputStream.write("Hello World".getBytes());
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        Socket timidSocket = new Socket();
        timidSocket.connect(new InetSocketAddress(port));
        final OutputStream timidOutputStream = timidSocket.getOutputStream();
        Thread timidSender = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    timidOutputStream.write("me too".getBytes());
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });

        verboseSender.start();
        Thread.sleep(500l);
        timidSender.start();
        Thread.sleep(1000l);
        verify(timidEngine, atLeast(6)).received(any(ByteBuffer.class));
        _keepRunningThreads = false;
    }


}
