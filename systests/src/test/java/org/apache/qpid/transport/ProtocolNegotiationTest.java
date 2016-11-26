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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.HeartbeatBody;
import org.apache.qpid.framing.ProtocolInitiation;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.ServerDisassembler;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.transport.network.Frame;

public class ProtocolNegotiationTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolNegotiationTest.class);
    private static final int SO_TIMEOUT = 5000;
    public static final int AMQP_HEADER_LEN = 8;
    private Protocol _expectedProtocolInit;

    private static final Map<Protocol, List<List<Byte>>> HEADERS = new HashMap<>();

    static
    {
        HEADERS.put(Protocol.AMQP_0_8,   Collections.singletonList(Arrays.asList((byte)1, (byte)1, (byte)8, (byte)0)));
        HEADERS.put(Protocol.AMQP_0_9,   Collections.singletonList(Arrays.asList((byte)1, (byte)1, (byte)0, (byte)9)));
        HEADERS.put(Protocol.AMQP_0_9_1, Collections.singletonList(Arrays.asList((byte)0, (byte)0, (byte)9, (byte)1)));
        HEADERS.put(Protocol.AMQP_0_10,  Collections.singletonList(Arrays.asList((byte)1, (byte)1, (byte)0, (byte)10)));
        HEADERS.put(Protocol.AMQP_1_0,   Arrays.asList(Arrays.asList((byte)3, (byte)1, (byte)0, (byte)0),
                                                       Arrays.asList((byte)0, (byte)1, (byte)0, (byte)0)));

    }

    public void setUp() throws Exception
    {
        // restrict broker to support only single protocol
        TestBrokerConfiguration config = getDefaultBrokerConfiguration();
        config.setObjectAttribute(Port.class,
                                  TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT,
                                  Port.PROTOCOLS,
                                  Arrays.asList(getBrokerProtocol()));
        Map<String,String> overriddenPortContext = new HashMap<>();
        overriddenPortContext.put(AmqpPort.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY, null);
        overriddenPortContext.put(AmqpPort.PROTOCOL_HANDSHAKE_TIMEOUT, String.valueOf(AmqpPort.DEFAULT_PROTOCOL_HANDSHAKE_TIMEOUT));
        config.setObjectAttribute(Port.class,
                                  TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT,
                                  Port.CONTEXT,
                                  overriddenPortContext);
        config.setBrokerAttribute(Broker.CONTEXT,
                                  Collections.singletonMap(AmqpPort.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY, null));

        super.setUp();
        _expectedProtocolInit = getBrokerProtocol();
    }

    public void testWrongProtocolHeaderSent_BrokerRespondsWithSupportedProtocol() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(SO_TIMEOUT);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getDefaultAmqpPort());
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

            ProtocolInitiation protocolInitiation = new ProtocolInitiation(QpidByteBuffer.wrap(receivedHeader));

            assertTrue("Unexpected protocol initialisation", matchedExpectedVersion(receivedHeader));
        }
    }

    private boolean matchedExpectedVersion(byte[] header)
    {
        if(header[0] != 'A' || header[1] != 'M' || header[2] != 'Q' || header[3] != 'P')
        {
            return false;
        }
        List<Byte> version = new ArrayList<>();
        for(int i = 4; i<8; i++)
        {
            version.add(header[i]);
        }
        return HEADERS.get(_expectedProtocolInit).contains(version);
    }

    public void testNoProtocolHeaderSent_BrokerClosesConnection() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(SO_TIMEOUT);

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getDefaultAmqpPort());
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

            byte[] header = getHeaderBytesForBrokerVersion();

            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getDefaultAmqpPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            OutputStream outputStream = socket.getOutputStream();
            final TestSender sender = new TestSender(outputStream);
            final InputStream inputStream = socket.getInputStream();

            // write header
            sender.send(QpidByteBuffer.wrap(header));
            sender.flush();

            // reader header
            byte[] receivedHeader = new byte[AMQP_HEADER_LEN];
            int len = inputStream.read(receivedHeader);
            assertEquals("Unexpected number of bytes available from socket", receivedHeader.length, len);

            // Send heartbeat frames to simulate a client that, although active, fails to
            // authenticate within the allowed period

            long timeout = System.currentTimeMillis() + 3000;
            boolean brokenPipe = false;
            while (timeout > System.currentTimeMillis())
            {
                if (!writeHeartbeat(sender))
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

    private byte[] getHeaderBytesForBrokerVersion()
    {
        byte[] header = new byte[8];
        header[0] = 'A';
        header[1] = 'M';
        header[2] = 'Q';
        header[3] = 'P';
        List<Byte> version = HEADERS.get(getBrokerProtocol()).iterator().next();
        int i = 4;
        for(byte b : version)
        {
            header[i++] = b;
        }
        return header;
    }

    public void testIllegalFrameSent_BrokerClosesConnection() throws Exception
    {
        try(Socket socket = new Socket())
        {
            socket.setSoTimeout(5000);


            final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", getDefaultAmqpPort());
            _logger.debug("Making connection to {}", inetSocketAddress);

            socket.connect(inetSocketAddress);

            assertTrue("Expected socket to be connected", socket.isConnected());

            final InputStream inputStream = socket.getInputStream();

            // write header
            TestSender sender = new TestSender(socket.getOutputStream());
            sender.send(QpidByteBuffer.wrap(getHeaderBytesForBrokerVersion()));
            sender.flush();

            // reader header
            byte[] receivedHeader = new byte[AMQP_HEADER_LEN];
            int len = inputStream.read(receivedHeader);
            assertEquals("Unexpected number of bytes available from socket", receivedHeader.length, len);

            sender.send(QpidByteBuffer.wrap("NOTANAMPQFRAME".getBytes()));

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
                assertEquals("Unexpected version negotiated: " + connection.getProtocolVersion(), convertProtocolToProtocolVersion(_expectedProtocolInit), connection.getProtocolVersion());
                connection.close();
            }
        }
    }

    private boolean writeHeartbeat(final TestSender sender)
            throws IOException
    {
        if (isBroker010())
        {
            ConnectionHeartbeat heartbeat = new ConnectionHeartbeat();
            ServerDisassembler serverDisassembler = new ServerDisassembler(sender, Frame.HEADER_SIZE + 1);
            serverDisassembler.command(null, heartbeat);
        }
        else
        {
            HeartbeatBody.FRAME.writePayload(sender);

        }

        return sender.hasSuccess();
    }

    private ProtocolVersion convertProtocolToProtocolVersion(final Protocol p)
    {
        final ProtocolVersion protocolVersion;
        switch(p)
        {
            case AMQP_1_0:
                protocolVersion = null;
                break;
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

    private static class TestSender implements ByteBufferSender
    {
        private final OutputStream _output;
        private boolean _success = true;


        private TestSender(final OutputStream output)
        {
            _output = output;
        }

        @Override
        public boolean isDirectBufferPreferred()
        {
            return false;
        }

        @Override
        public void send(final QpidByteBuffer msg)
        {
            byte[] data = new byte[msg.remaining()];
            msg.get(data);
            try
            {
                _output.write(data);
            }
            catch (IOException e)
            {
                _success = false;
            }

        }

        public boolean hasSuccess()
        {
            return _success;
        }

        @Override
        public void flush()
        {

        }

        @Override
        public void close()
        {

        }

    }

}
