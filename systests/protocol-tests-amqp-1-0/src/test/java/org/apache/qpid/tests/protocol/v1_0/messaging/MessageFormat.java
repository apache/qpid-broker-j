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

package org.apache.qpid.tests.protocol.v1_0.messaging;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.Response;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class MessageFormat extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Ignore("QPID-7823")
    @Test
    @SpecificationTest(section = "2.7.5",
            description = "message-format: "
                          + "This field MUST be specified for the first transfer of a multi-transfer message and can "
                          + "only be omitted for continuation transfers. It is an error if the message-format on a "
                          + "continuation transfer differs from the message-format on the first transfer of a delivery.")
    public void differentMessageFormatOnSameDeliveryFails() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ZERO;
            transport.doAttachSendingLink(linkHandle, BrokerAdmin.TEST_QUEUE_NAME);

            MessageEncoder messageEncoder = new MessageEncoder();
            messageEncoder.addData("foo");

            Transfer transfer = new Transfer();
            transfer.setHandle(linkHandle);
            transfer.setDeliveryTag(new Binary("testDeliveryTag".getBytes(StandardCharsets.UTF_8)));
            transfer.setDeliveryId(UnsignedInteger.ONE);
            transfer.setMore(true);
            transfer.setMessageFormat(UnsignedInteger.valueOf(0));
            transfer.setPayload(messageEncoder.getPayload());
            transport.sendPerformative(transfer);

            final Response<?> response = transport.getNextResponse();
            if (response != null)
            {
                assertThat(response.getBody(), is(instanceOf(Disposition.class)));
            }

            messageEncoder.addData("bar");
            transfer.setMessageFormat(UnsignedInteger.valueOf(1));
            transport.sendPerformative(transfer);

            final Response<?> response2 = transport.getNextResponse();
            assertThat(response2, is(notNullValue()));
            final Object responseBody = response2.getBody();
            final Error error;
            if (responseBody instanceof Detach)
            {
                error = ((Detach) responseBody).getError();
            }
            else if (responseBody instanceof End)
            {
                error = ((End) responseBody).getError();
            }
            else if (responseBody instanceof Close)
            {
                error = ((Close) responseBody).getError();
            }
            else
            {
                fail(String.format("Expected response of either Detach, End, or Close. Got '%s'", responseBody));
                error = null;
            }

            assertThat(error, is(notNullValue()));
        }
    }
}
