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
package org.apache.qpid.tests.protocol.v0_10;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

import org.hamcrest.core.IsEqual;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStart;
import org.apache.qpid.tests.protocol.HeaderResponse;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ProtocolTest extends BrokerAdminUsingTestBase
{
    private static final String DEFAULT_LOCALE = "en_US";

    @Test
    @SpecificationTest(section = "4.3. Version Negotiation",
            description = "When the client opens a new socket connection to an AMQP server,"
                          + " it MUST send a protocol header with the client's preferred protocol version."
                          + "If the requested protocol version is supported, the server MUST send its own protocol"
                          + " header with the requested version to the socket, and then implement the protocol accordingly")
    public void versionNegotiation() throws Exception
    {
        assumeThat(getBrokerAdmin().isAnonymousSupported(), is(equalTo(true)));
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.ANONYMOUS_AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            Response<?> response = interaction.negotiateProtocol().consumeResponse().getLatestResponse();
            assertThat(response, is(instanceOf(HeaderResponse.class)));
            assertThat(response.getBody(), is(IsEqual.equalTo(transport.getProtocolHeader())));

            ConnectionStart connectionStart = interaction.consumeResponse().getLatestResponse(ConnectionStart.class);
            assertThat(connectionStart.getMechanisms(), is(notNullValue()));
            assertThat(connectionStart.getMechanisms(), contains(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS));
            assertThat(connectionStart.getLocales(), is(notNullValue()));
            assertThat(connectionStart.getLocales(), contains(DEFAULT_LOCALE));
        }
    }

    @Test
    @SpecificationTest(section = "4.3. Version Negotiation",
            description = "If the server can't parse the protocol header, the server MUST send a valid protocol "
                          + "header with a supported protocol version and then close the socket.")
    public void unrecognisedProtocolHeader() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {

            final Interaction interaction = transport.newInteraction();

            byte[] unknownHeader = "NOTANAMQPHEADER".getBytes(StandardCharsets.UTF_8);
            byte[] expectedResponse = "AMQP\001\001\000\012".getBytes(StandardCharsets.UTF_8);
            final byte[] response = interaction.protocolHeader(unknownHeader)
                                               .negotiateProtocol()
                                               .consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals("Unexpected protocol header response", expectedResponse, response);
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "4.3. Version Negotiation",
            description = "If the requested protocol version is not supported, the server MUST send a protocol "
                          + "header with a supported protocol version and then close the socket.")
    public void unrecognisedProtocolVersion() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {


            final Interaction interaction = transport.newInteraction();

            byte[] unknownAmqpVersion = "AMQP\001\001\000\013".getBytes(StandardCharsets.UTF_8);
            byte[] expectedResponse = "AMQP\001\001\000\012".getBytes(StandardCharsets.UTF_8);
            final byte[] response = interaction.protocolHeader(unknownAmqpVersion)
                                               .negotiateProtocol()
                                               .consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals("Unexpected protocol header response", expectedResponse, response);
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "8. Domains", description = "valid values for the frame type indicator.")
    public void invalidSegmentType() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol().consumeResponse()
                       .consumeResponse(ConnectionStart.class);

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(bos))
            {
                dos.writeByte(0);   /* flags */
                dos.writeByte(4);   /* segment type - undefined value in 0-10 */
                dos.writeShort(12); /* size */
                dos.writeByte(0);
                dos.writeByte(0);   /* track */
                dos.writeShort(0);  /* channel */
                dos.writeByte(0);
                dos.writeByte(0);
                dos.writeByte(0);
                dos.writeByte(0);

                transport.sendBytes(bos.toByteArray());
            }
            transport.flush();
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
