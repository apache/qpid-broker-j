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


import static org.junit.Assert.assertArrayEquals;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.HeaderResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;


/*

TODO

logging - log per test?
protocol assertions
admin factory
performative test
embedded broker per test admin impl that creates broker per test
embedded broker per class admin impl creates/destroys vhost per test
queue creation?
 */


public class ProtocolHeaderTest extends ProtocolTestBase
{
    @Test
    @SpecificationTest(section = "2.2",
            description = "Prior to sending any frames on a connection, each peer MUST start by sending a protocol header that indicates "
                          + "the protocol version used on the connection. The protocol header consists of the upper case ASCII letters “AMQP” "
                          + "followed by a protocol id of zero, followed by three unsigned bytes representing the major, minor, and revision of "
                          + "the protocol version (currently 1 (MAJOR), 0 (MINOR), 0 (REVISION)).")
    public void successfulHeaderExchange() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr))
        {
            byte[] bytes = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
            transport.sendProtocolHeader(bytes);
            HeaderResponse response = (HeaderResponse) transport.getNextResponse();
            assertArrayEquals("Unexpected protocol header response", bytes, response.getHeader());
        }

    }
}
