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

package org.apache.qpid.tests.protocol.v1_0.transport.session;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class BeginTest extends ProtocolTestBase
{
    @Test
    @SpecificationTest(section = "1.3.4",
            description = "Begin without mandatory fields should result in a decoding error.")
    public void emptyBegin() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try(FrameTransport transport = new FrameTransport(addr))
        {
            transport.doOpenConnection();
            Begin begin = new Begin();
            transport.sendPerformative(begin, UnsignedShort.valueOf((short) 37));
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Close.class)));
            Close responseClose = (Close) response.getFrameBody();
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(AmqpError.DECODE_ERROR));
        }
    }

    @Test
    @SpecificationTest(section = "2.5.1",
            description = "Sessions are established by creating a session endpoint, assigning it to an unused channel number, "
                          + "and sending a begin announcing the association of the session endpoint with the outgoing channel.")
    public void successfulBegin() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr))
        {
            transport.doOpenConnection();
            Begin begin = new Begin();
            begin.setNextOutgoingId(UnsignedInteger.ZERO);
            begin.setIncomingWindow(UnsignedInteger.ZERO);
            begin.setOutgoingWindow(UnsignedInteger.ZERO);

            UnsignedShort channel = UnsignedShort.valueOf((short) 37);
            transport.sendPerformative(begin, channel);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Begin.class)));
            Begin responseBegin = (Begin) response.getFrameBody();
            assertThat(responseBegin.getRemoteChannel(), equalTo(channel));
            assertThat(responseBegin.getIncomingWindow(), is(instanceOf(UnsignedInteger.class)));
            assertThat(responseBegin.getOutgoingWindow(), is(instanceOf(UnsignedInteger.class)));
            assertThat(responseBegin.getNextOutgoingId(), is(instanceOf(UnsignedInteger.class)));

            transport.doCloseConnection();
        }
    }
}
