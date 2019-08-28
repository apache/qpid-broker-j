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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class MessageFormatTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
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
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload(getTestName(), 2);

            transport.newInteraction()
                     .negotiateOpen()
                     .begin().consumeResponse(Begin.class)
                     .attachRole(Role.SENDER)
                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                     .attach().consumeResponse(Attach.class)
                     .consumeResponse(Flow.class)
                     .transferMore(true)
                     .transferMessageFormat(UnsignedInteger.ZERO)
                     .transferPayload(payloads[0])
                     .transferSettled(true)
                     .transfer()
                     .consumeResponse(null, Flow.class, Disposition.class)
                     .transferDeliveryTag(null)
                     .transferDeliveryId(null)
                     .transferMore(false)
                     .transferMessageFormat(UnsignedInteger.ONE)
                     .transferPayload(payloads[1])
                     .transfer()
                     .sync();

            for (final QpidByteBuffer payload : payloads)
            {
                payload.dispose();
            }
        }

        final String testMessage = getTestName() + "_2";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, testMessage);
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(testMessage)));
    }
}
