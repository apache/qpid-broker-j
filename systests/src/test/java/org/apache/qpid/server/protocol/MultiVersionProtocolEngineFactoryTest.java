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
package org.apache.qpid.server.protocol;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.qpid.server.model.*;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.util.BrokerTestHelper;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.NetworkConnection;

public class MultiVersionProtocolEngineFactoryTest extends QpidTestCase
{
    private Broker _broker;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        BrokerTestHelper.setUp();
        _broker = BrokerTestHelper.createBrokerMock();

        when(_broker.getContextValue(eq(Integer.class), eq(Broker.BROKER_FRAME_SIZE))).thenReturn(0xffff);

    }

    @Override
    protected void tearDown() throws Exception
    {
            BrokerTestHelper.tearDown();
            super.tearDown();
    }

    private static final byte[] AMQP_0_8_HEADER =
        new byte[] { (byte) 'A',
                     (byte) 'M',
                     (byte) 'Q',
                     (byte) 'P',
                     (byte) 1,
                     (byte) 1,
                     (byte) 8,
                     (byte) 0
        };

    private static final byte[] AMQP_0_9_HEADER =
        new byte[] { (byte) 'A',
                     (byte) 'M',
                     (byte) 'Q',
                     (byte) 'P',
                     (byte) 1,
                     (byte) 1,
                     (byte) 0,
                     (byte) 9
        };

    private static final byte[] AMQP_0_9_1_HEADER =
        new byte[] { (byte) 'A',
                     (byte) 'M',
                     (byte) 'Q',
                     (byte) 'P',
                     (byte) 0,
                     (byte) 0,
                     (byte) 9,
                     (byte) 1
        };


    private static final byte[] AMQP_0_10_HEADER =
        new byte[] { (byte) 'A',
                     (byte) 'M',
                     (byte) 'Q',
                     (byte) 'P',
                     (byte) 1,
                     (byte) 1,
                     (byte) 0,
                     (byte) 10
        };


    private static final byte[] AMQP_1_0_0_HEADER =
            new byte[] {
                    (byte)'A',
                    (byte)'M',
                    (byte)'Q',
                    (byte)'P',
                    (byte) 0,
                    (byte) 1,
                    (byte) 0,
                    (byte) 0
            };


    protected Set<Protocol> getAllAMQPProtocols()
    {
        Set<Protocol> protocols = EnumSet.allOf(Protocol.class);
        Iterator<Protocol> protoIter = protocols.iterator();
        while(protoIter.hasNext())
        {
            Protocol protocol = protoIter.next();
            if(protocol.getProtocolType() != Protocol.ProtocolType.AMQP)
            {
                protoIter.remove();
            }
        }
        return protocols;
    }

    /**
     * Test to verify that when requesting a ProtocolEngineFactory to produce engines having a default reply to unsupported
     * version initiations, there is enforcement that the default reply is itself a supported protocol version.
     */
    public void testUnsupportedDefaultReplyCausesIllegalArgumentException()
    {
        Set<Protocol> versions = getAllAMQPProtocols();
        versions.remove(Protocol.AMQP_0_9);

        try
        {
            new MultiVersionProtocolEngineFactory(_broker, versions, Protocol.AMQP_0_9, null,
                    org.apache.qpid.server.model.Transport.TCP);
            fail("should not have been allowed to create the factory");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    private static class TestNetworkConnection implements NetworkConnection
    {
        private String _remoteHost = "127.0.0.1";
        private String _localHost = "127.0.0.1";
        private int _port = 1;
        private final ByteBufferSender _sender;

        public TestNetworkConnection()
        {
            _sender = new ByteBufferSender()
            {
                public void send(ByteBuffer msg)
                {
                }

                public void flush()
                {
                }

                public void close()
                {
                }
            };
        }

        @Override
        public SocketAddress getLocalAddress()
        {
            return new InetSocketAddress(_localHost, _port);
        }

        @Override
        public SocketAddress getRemoteAddress()
        {
            return new InetSocketAddress(_remoteHost, _port);
        }

        @Override
        public void setMaxReadIdle(int idleTime)
        {
        }

        @Override
        public Principal getPeerPrincipal()
        {
            return null;
        }

        @Override
        public Certificate getPeerCertificate()
        {
            return null;
        }

        @Override
        public int getMaxReadIdle()
        {
            return 0;
        }

        @Override
        public int getMaxWriteIdle()
        {
            return 0;
        }

        @Override
        public void setMaxWriteIdle(int idleTime)
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public ByteBufferSender getSender()
        {
            return _sender;
        }

        @Override
        public void start()
        {
        }
    }
}
