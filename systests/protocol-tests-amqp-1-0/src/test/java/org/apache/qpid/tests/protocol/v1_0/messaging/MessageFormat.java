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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Response;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class MessageFormat extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

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
            QpidByteBuffer[] payloads = Utils.splitPayload("testData", 2);

            final Response<?> latestResponse = transport.newInteraction()
                                                        .negotiateProtocol().consumeResponse()
                                                        .open().consumeResponse(Open.class)
                                                        .begin().consumeResponse(Begin.class)
                                                        .attachRole(Role.SENDER)
                                                        .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                        .attach().consumeResponse(Attach.class)
                                                        .consumeResponse(Flow.class)
                                                        .transferMore(true)
                                                        .transferMessageFormat(UnsignedInteger.ZERO)
                                                        .transferPayload(payloads[0])
                                                        .transfer()
                                                        .consumeResponse(null, Flow.class, Disposition.class)
                                                        .transferDeliveryTag(null)
                                                        .transferDeliveryId(null)
                                                        .transferMore(false)
                                                        .transferMessageFormat(UnsignedInteger.ONE)
                                                        .transferPayload(payloads[1])
                                                        .transfer()
                                                        .consumeResponse(Detach.class, End.class, Close.class)
                                                        .getLatestResponse();

            for (final QpidByteBuffer payload : payloads)
            {
                payload.dispose();
            }
            assertThat(latestResponse, is(notNullValue()));
            final Object responseBody = latestResponse.getBody();
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
