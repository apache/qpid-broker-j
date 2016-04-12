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
 *
 */
package org.apache.qpid.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.ByteArrayDataInput;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.HeartbeatBody;
import org.apache.qpid.framing.ProtocolInitiation;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.protocol.v0_10.ServerDisassembler;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.transport.network.Frame;

public class ProtocolNegotiationTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolNegotiationTest.class);
    private static final int SO_TIMEOUT = 5000;
    public static final int AMQP_HEADER_LEN = 8;
    private ProtocolVersion _expectedProtocolInit;

    public void setUp() throws Exception
    {
        // restrict broker to support only single protocol
        TestBrokerConfiguration config = getBrokerConfiguration();
        config.setObjectAttribute(Port.class,
                                  TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT,
                                  Port.PROTOCOLS,
                                  Arrays.asList(getBrokerProtocol()));
        config.setObjectAttribute(Port.class,
                                  TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT,
                                  Port.CONTEXT,
                                  Collections.singletonMap(BrokerProperties.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY, null));
        config.setBrokerAttribute(Broker.CONTEXT,
                                  Collections.singletonMap(BrokerProperties.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY, null));

        super.setUp();
        _expectedProtocolInit = convertProtocolToProtocolVersion(getBrokerProtocol());
    }

    public void testWrongProtocolHeaderSent_BrokerRespondsWithSupportedProtocol() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(SO_TIMEOUT);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            socket.getOutputStream().write("NOTANAMQPHEADER".getBytes());
            byte[] receivedHeader = new byte[AMQP_HEADER_LEN];
            int len = socket.getInputStream().read(receivedHeader);
            assertEquals("Unexpected number of bytes available from socket", receivedHeader.length, len);
            assertEquals("Expected end-of-stream from socket signifying socket closed)",
                         -1,
                         socket.getInputStream().read());

            ProtocolInitiation protocolInitiation = new ProtocolInitiation(new ByteArrayDataInput(receivedHeader));

            assertEquals("Unexpected protocol initialisation", _expectedProtocolInit, protocolInitiation.checkVersion());
        }
    }


    public void testNoProtocolHeaderSent_BrokerClosesConnection() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(SO_TIMEOUT);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            int c = 0;
            try
            {
                c = socket.getInputStream().read();
                _logger.debug("Read {}", c);

            }
            catch(SocketTimeoutException ste)
            {
                fail("Broker did not close connection with no activity within expected timeout");
            }

            assertEquals("Expected end-of-stream from socket signifying socket closed)", -1, c);
        }
    }

    public void testNoConnectionOpenSent_BrokerClosesConnection() throws Exception
    {
        setSystemProperty(Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY, "1000");

        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(5000);

            final ProtocolVersion protocolVersion = convertProtocolToProtocolVersion(getBrokerProtocol());
            ProtocolInitiation pi = new ProtocolInitiation(protocolVersion);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            final DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            final InputStream inputStream = socket.getInputStream();

            // write header
            pi.writePayload(dataOutputStream);

            // reader header
            byte[] receivedHeader = new byte[AMQP_HEADER_LEN];
            int len = inputStream.read(receivedHeader);
            assertEquals("Unexpected number of bytes available from socket", receivedHeader.length, len);

            // Send heartbeat frames to simulate a client that, although active, fails to
            // authenticate within the allowed period

            long timeout = System.currentTimeMillis() + 3000;
            boolean brokenPipe = false;
            while(timeout > System.currentTimeMillis())
            {
                if (!writeHeartbeat(dataOutputStream))
                {
                    brokenPipe = true;
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue("Expected pipe to become broken within "
                       + Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY + " timeout", brokenPipe);
        }
    }

    public void testIllegalFrameSent_BrokerClosesConnection() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(5000);

            final ProtocolVersion protocolVersion = convertProtocolToProtocolVersion(getBrokerProtocol());
            ProtocolInitiation pi = new ProtocolInitiation(protocolVersion);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            final DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            final InputStream inputStream = socket.getInputStream();

            // write header
            pi.writePayload(dataOutputStream);

            // reader header
            byte[] receivedHeader = new byte[AMQP_HEADER_LEN];
            int len = inputStream.read(receivedHeader);
            assertEquals("Unexpected number of bytes available from socket", receivedHeader.length, len);

            dataOutputStream.write("NOTANAMPQFRAME".getBytes());


        }
    }

    public void testProtocolNegotiationFromUnsupportedVersion() throws Exception
    {
        Protocol testProtocol = getBrokerProtocol();
        String testSupportedProtocols = System.getProperty("test.amqp_port_protocols");
        if (testSupportedProtocols!= null)
        {
            Set<Protocol> availableProtocols = new HashSet<>();
            List<Object> protocols = new ObjectMapper().readValue(testSupportedProtocols, List.class);
            for (Object protocol : protocols)
            {
                availableProtocols.add(Protocol.valueOf(String.valueOf(protocol)));
            }
            availableProtocols.remove(testProtocol);

            for (Protocol protocol: availableProtocols)
            {
                String version = protocol.name().substring(5).replace('_', '-');
                LOGGER.debug("Negotiation version {} represented as {}", protocol.name(), version);
                setTestSystemProperty(ClientProperties.AMQP_VERSION, version);
                AMQConnection connection = (AMQConnection)getConnection();
                LOGGER.debug("Negotiated version {}", connection.getProtocolVersion());
                assertEquals("Unexpected version negotiated: " + connection.getProtocolVersion(), _expectedProtocolInit, connection.getProtocolVersion());
                connection.close();
            }
        }
    }

    private boolean writeHeartbeat(final DataOutputStream dataOutputStream)
            throws IOException
    {
        final AtomicBoolean success = new AtomicBoolean(true);
        if (isBroker010())
        {
            ConnectionHeartbeat heartbeat = new ConnectionHeartbeat();
            ServerDisassembler serverDisassembler = new ServerDisassembler(new ByteBufferSender()
            {
                private void send(final ByteBuffer msg)
                {
                    try
                    {
                        if(msg.hasArray())
                        {
                            dataOutputStream.write(msg.array(), msg.arrayOffset() + msg.position(), msg.remaining());
                        }
                        else
                        {
                            byte[] data = new byte[msg.remaining()];
                            msg.duplicate().get(data);
                            dataOutputStream.write(data, 0, data.length);
                        }
                    }
                    catch (SocketException se)
                    {

                        success.set(false);
                    }
                    catch(IOException e)
                    {
                        throw new RuntimeException("Unexpected IOException", e);
                    }
                }

                @Override
                public void send(final QpidByteBuffer msg)
                {
                    try
                    {
                        if(msg.hasArray())
                        {
                            dataOutputStream.write(msg.array(), msg.arrayOffset() + msg.position(), msg.remaining());
                        }
                        else
                        {
                            byte[] data = new byte[msg.remaining()];
                            msg.duplicate().get(data);
                            dataOutputStream.write(data, 0, data.length);
                        }
                    }
                    catch (SocketException se)
                    {

                        success.set(false);
                    }
                    catch(IOException e)
                    {
                        throw new RuntimeException("Unexpected IOException", e);
                    }
                }

                @Override
                public void flush()
                {
                }

                @Override
                public void close()
                {
                }
            }, Frame.HEADER_SIZE + 1);
            serverDisassembler.command(null, heartbeat);
        }
        else
        {
            try
            {
                HeartbeatBody.FRAME.writePayload(dataOutputStream);
            }
            catch (SocketException se)
            {
                success.set(false);
            }
        }

        return success.get();
    }

    private ProtocolVersion convertProtocolToProtocolVersion(final Protocol p)
    {
        final ProtocolVersion protocolVersion;
        switch(p)
        {
            case AMQP_0_10:
                protocolVersion = ProtocolVersion.v0_10;
                break;
            case AMQP_0_9_1:
                protocolVersion = ProtocolVersion.v0_91;
                break;
            case AMQP_0_9:
                protocolVersion = ProtocolVersion.v0_9;
                break;
            case AMQP_0_8:
                protocolVersion = ProtocolVersion.v0_8;
                break;
            default:
                throw new IllegalArgumentException("Unexpected " + p.name());
        }
        return protocolVersion;
    }
}
