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

package org.apache.qpid.tests.protocol.v1_0.transport;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ProtocolHeaderTest extends BrokerAdminUsingTestBase
{
    @Test
    @SpecificationTest(section = "2.2",
            description = "Prior to sending any frames on a connection, each peer MUST start by sending a protocol header that indicates "
                          + "the protocol version used on the connection. The protocol header consists of the upper case ASCII letters “AMQP” "
                          + "followed by a protocol id of zero, followed by three unsigned bytes representing the major, minor, and revision of "
                          + "the protocol version (currently 1 (MAJOR), 0 (MINOR), 0 (REVISION)).")
    public void successfulHeaderExchange() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.ANONYMOUS_AMQP).connect())
        {
            byte[] protocolHeader = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
            final byte[] response = transport.newInteraction()
                                             .protocolHeader(protocolHeader)
                                             .negotiateProtocol().consumeResponse()
                                             .getLatestResponse(byte[].class);
            assertArrayEquals("Unexpected protocol header response", protocolHeader, response);
        }
    }

    @Test
    @SpecificationTest(section = "2.2",
            description = " A client might request use of a protocol id that is unacceptable to a server. [...]"
                          + "In this case, the server MUST send a protocol header with an acceptable protocol id"
                          + "(and version) and then close the socket.")
    public void unacceptableProtocolIdSent_SaslAcceptable() throws Exception
    {
        assumeThat(getBrokerAdmin().isSASLSupported(), is(equalTo(true)));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {

            byte[] rawHeaderBytes = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
            final byte[] response = transport.newInteraction()
                                             .protocolHeader(rawHeaderBytes)
                                             .negotiateProtocol().consumeResponse()
                                             .getLatestResponse(byte[].class);
            byte[] expectedSaslHeaderBytes = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);
            assertArrayEquals("Unexpected protocol header response", expectedSaslHeaderBytes, response);

            transport.assertNoMoreResponses();
        }
    }
}
