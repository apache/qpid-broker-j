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
package org.apache.qpid.tests.protocol.v0_8;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQVersionAwareProtocolSession;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ProtocolTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "4.2.2",
            description = "If the server does not recognise the first 5 octets of data on the socket [...], it MUST "
                          + "write a valid protocol header to the socket, [...] and then close the socket connection.")
    public void unrecognisedProtocolHeader() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            assumeThat(transport.getProtocolVersion(), is(equalTo(ProtocolVersion.v0_91)));

            final Interaction interaction = transport.newInteraction();

            byte[] unknownHeader = "NOTANAMQPHEADER".getBytes(StandardCharsets.UTF_8);
            byte[] expectedResponse = "AMQP\000\000\011\001".getBytes(StandardCharsets.UTF_8);
            final byte[] response = interaction.protocolHeader(unknownHeader)
                                               .negotiateProtocol()
                                               .consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals("Unexpected protocol header response", expectedResponse, response);
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "4.2.2",
            description = "If the server [...] does not support the specific protocol version that the client "
                          + "requests, it MUST write a valid protocol header to the socket, [...] and then close "
                          + "the socket connection.")
    public void unrecognisedProtocolVersion() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            assumeThat(transport.getProtocolVersion(), is(equalTo(ProtocolVersion.v0_91)));

            final Interaction interaction = transport.newInteraction();

            byte[] unknownAmqpVersion = "AMQP\000\000\010\002".getBytes(StandardCharsets.UTF_8);
            byte[] expectedResponse = "AMQP\000\000\011\001".getBytes(StandardCharsets.UTF_8);
            final byte[] response = interaction.protocolHeader(unknownAmqpVersion)
                                               .negotiateProtocol()
                                               .consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals("Unexpected protocol header response", expectedResponse, response);
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "4.2.2", description = "The server either accepts [...] the protocol header")
    public void validProtocolVersion() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol()
                       .consumeResponse().getLatestResponse(ConnectionStartBody.class);

        }
    }

    @Test
    @SpecificationTest(section = "4.2.2",
            description = "If a peer receives a frame with a type that is not one of these defined types, it MUST "
                          + "treat this as a fatal protocol error and close the connection without sending any "
                          + "further data on it")
    public void unrecognisedFrameType() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol()
                       .consumeResponse(ConnectionStartBody.class)
                       .sendPerformative(new AMQBody()
                       {
                           @Override
                           public byte getFrameType()
                           {
                               return (byte)5;  // Spec defines 1 - 4 only.
                           }

                           @Override
                           public int getSize()
                           {
                               return 0;
                           }

                           @Override
                           public long writePayload(final ByteBufferSender sender)
                           {
                               return 0;
                           }

                           @Override
                           public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
                           {
                               throw new UnsupportedOperationException();
                           }
                       }).sync();
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
