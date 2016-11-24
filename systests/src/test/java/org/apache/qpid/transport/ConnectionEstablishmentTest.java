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
package org.apache.qpid.transport;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.BrokerDetails;
import org.apache.qpid.codec.ClientDecoder;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.AMQFrame;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.AMQMethodBodyImpl;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ChannelOpenBody;
import org.apache.qpid.framing.ConnectionCloseBody;
import org.apache.qpid.framing.ConnectionOpenBody;
import org.apache.qpid.framing.ConnectionStartBody;
import org.apache.qpid.framing.FrameCreatingMethodProcessor;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.framing.QueueDeleteBody;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.protocol.v0_8.ProtocolEngineCreator_0_8;
import org.apache.qpid.server.protocol.v0_8.ProtocolEngineCreator_0_9;
import org.apache.qpid.server.protocol.v0_8.ProtocolEngineCreator_0_9_1;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.transport.network.Assembler;
import org.apache.qpid.transport.network.Disassembler;
import org.apache.qpid.transport.network.InputHandler;

public class ConnectionEstablishmentTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionEstablishmentTest.class);
    private static final int NUMBER_OF_MESSAGES = 2;
    private Connection _utilityConnection;
    private Destination _testQueue;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _utilityConnection = getConnection();
        Session session = _utilityConnection.createSession(false, Session.SESSION_TRANSACTED);
        _testQueue = getTestQueue();
        session.createConsumer(_testQueue).close();
        sendMessage(session, _testQueue, NUMBER_OF_MESSAGES);
        session.close();
    }

    public void testAuthenticationBypass() throws Exception
    {
        String virtualHost = TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST;
        ConnectionURL connectionURL = ((AMQConnectionFactory)getConnectionFactory()).getConnectionURL();
        BrokerDetails brokerDetails = connectionURL.getAllBrokerDetails().get(0);
        try (Socket socket = new Socket(brokerDetails.getHost(), brokerDetails.getPort());
             OutputStream os = socket.getOutputStream())
        {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);
            openConnectionBypassingAuthenticationAndDeleteQueue(virtualHost, socket, os);
        }

        // verify that queue still exists
        _utilityConnection.start();
        setSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");
        Session session = _utilityConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(_testQueue);
        Message message = consumer.receive(RECEIVE_TIMEOUT);
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
        {
            assertNotNull("Message [" + i + "] not received", message);
        }
    }

    private void openConnectionBypassingAuthenticationAndDeleteQueue(final String virtualHost,
                                                                     final Socket socket,
                                                                     final OutputStream os)
            throws Exception
    {
        if (isBroker010())
        {
            open010ConnectionBypassingAuthenticationAndDeleteQueue(socket, os, virtualHost);
        }
        else
        {
            openNon010ConnectionBypassingAuthenticationAndDeleteQueue(socket, os, virtualHost);
        }
    }

    private void open010ConnectionBypassingAuthenticationAndDeleteQueue(Socket socket,
                                                                        OutputStream os,
                                                                        final String virtualHost)
            throws IOException
    {
        TestSender sender = new TestSender(os);
        Disassembler disassembler = new Disassembler(sender, Constant.MIN_MAX_FRAME_SIZE);
        disassembler.send(new ProtocolHeader(1, 0, 10));
        disassembler.send(new ConnectionOpen(virtualHost, null, Option.INSIST));
        byte[] sessionName = "test".getBytes();
        disassembler.send(new SessionAttach(sessionName));
        disassembler.send(new SessionAttached(sessionName));
        disassembler.send(new SessionRequestTimeout(0));
        disassembler.send(new SessionCommandPoint(0, 0));
        disassembler.send(new QueueDelete(getTestQueueName(), Option.SYNC));
        disassembler.send(new SessionFlush());
        disassembler.send(new ExecutionSync(Option.SYNC));
        sender.flush();

        TestEventReceiver receiver = new TestEventReceiver();
        Assembler assembler = new Assembler(receiver);
        InputHandler inputHandler = new InputHandler(assembler, false);
        try (InputStream is = socket.getInputStream())
        {
            byte[] buffer = new byte[1024];
            int size;
            try
            {
                while ((size = is.read(buffer)) > 0)
                {
                    inputHandler.received(ByteBuffer.wrap(buffer, 0, size));
                    if (receiver.isClosed() || receiver.getThrowable() != null || receiver.getEvents().size() > 3)
                    {
                        // expected at most 3 events: Header, ConnectionStart, ConnectionClose
                        break;
                    }
                }
            }
            catch (SocketTimeoutException e)
            {
                // ignore
            }
        }

        boolean closeFound = receiver.isClosed() || receiver.getThrowable() != null;
        for (ProtocolEvent event : receiver.getEvents())
        {
            if (event instanceof ConnectionClose)
            {
                closeFound = true;
                break;
            }
        }

        assertTrue("Unauthenticated connection was not closed", closeFound);
    }

    private void openNon010ConnectionBypassingAuthenticationAndDeleteQueue(final Socket socket,
                                                                           final OutputStream os,
                                                                           final String virtualHost)
            throws IOException, AMQFrameDecodingException
    {

        byte[] protocolHeader = getProtocolHeaderBytes();
        String versionName = getBrokerProtocol().name().substring(5);
        if (versionName.equals("0_9_1"))
        {
            versionName = "0-91";
        }
        ProtocolVersion protocolVersion = ProtocolVersion.parse(versionName.replace('_', '-'));

        os.write(protocolHeader);
        try (InputStream is = socket.getInputStream())
        {
            receiveFrameOfType(protocolVersion, is, ConnectionStartBody.class, 1);
            TestSender sender = new TestSender(os);

            new AMQFrame(0, new ConnectionOpenBody(AMQShortString.valueOf(virtualHost),
                                                   AMQShortString.EMPTY_STRING,
                                                   true)).writePayload(sender);
            new AMQFrame(1, new ChannelOpenBody()).writePayload(sender);
            new AMQFrame(1, new QueueDeleteBody(2,
                                                AMQShortString.valueOf(getTestQueueName()),
                                                false,
                                                false,
                                                true)).writePayload(sender);
            sender.flush();

            receiveFrameOfType(protocolVersion, is, ConnectionCloseBody.class, 1);
        }
    }

    private byte[] getProtocolHeaderBytes()
    {
        byte[] protocolHeader;
        Protocol protocol = getBrokerProtocol();
        switch (protocol)
        {
            case AMQP_0_8:
                protocolHeader = (ProtocolEngineCreator_0_8.getInstance().getHeaderIdentifier());
                break;
            case AMQP_0_9:
                protocolHeader = (ProtocolEngineCreator_0_9.getInstance().getHeaderIdentifier());
                break;
            case AMQP_0_9_1:
                protocolHeader = (ProtocolEngineCreator_0_9_1.getInstance().getHeaderIdentifier());
                break;
            default:
                throw new RuntimeException("Unexpected Protocol Version: " + protocol);
        }
        return protocolHeader;
    }

    private void receiveFrameOfType(ProtocolVersion protocolVersion, InputStream is,
                                    Class<? extends AMQMethodBodyImpl> expectedFrameClass,
                                    int maxNumberOfExpectedFrames) throws IOException, AMQFrameDecodingException
    {
        final FrameCreatingMethodProcessor methodProcessor = new FrameCreatingMethodProcessor(protocolVersion);
        ClientDecoder decoder = new ClientDecoder(methodProcessor);
        byte[] buffer = new byte[1024];
        int size;
        while ((size = is.read(buffer)) > 0)
        {

            decoder.decodeBuffer(ByteBuffer.wrap(buffer, 0, size));

            List<AMQDataBlock> responseData = methodProcessor.getProcessedMethods();
            LOGGER.info("RECV:" + responseData);
            if (containsFrame(responseData, expectedFrameClass) || responseData.size() > maxNumberOfExpectedFrames)
            {
                break;
            }
        }

        List<AMQDataBlock> processedMethods = methodProcessor.getProcessedMethods();
        assertTrue("Expected frame  of type " + expectedFrameClass.getSimpleName() + " is not received",
                   containsFrame(processedMethods, expectedFrameClass));
    }

    private boolean containsFrame(final List<AMQDataBlock> frames,
                                  final Class<? extends AMQMethodBodyImpl> frameClass)
    {
        for (AMQDataBlock block : frames)
        {
            AMQFrame frame = (AMQFrame) block;
            if (frameClass.isInstance(frame.getBodyFrame()))
            {
                return true;
            }
        }
        return false;
    }

    private class TestEventReceiver implements ProtocolEventReceiver
    {
        private volatile boolean _closed;
        private volatile Throwable _throwable;
        private final List<ProtocolEvent> _events = new ArrayList<>();

        @Override
        public void received(final ProtocolEvent msg)
        {
            LOGGER.info("RECV:" + msg);
            _events.add(msg);
        }

        @Override
        public void exception(final Throwable t)
        {
            _throwable = t;
        }

        @Override
        public void closed()
        {
            _closed = true;
        }

        public boolean isClosed()
        {
            return _closed;
        }

        public Collection<ProtocolEvent> getEvents()
        {
            return Collections.unmodifiableCollection(_events);
        }

        public Throwable getThrowable()
        {
            return _throwable;
        }
    }
}
