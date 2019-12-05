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
package org.apache.qpid.tests.protocol.v0_8.extension.queue;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "queue.behaviourOnUnknownDeclareArgument", value = "IGNORE")
public class QueueDeclareInvalidOptionBehaviourTest extends BrokerAdminUsingTestBase
{
    private static final String TEST_QUEUE = "testQueue";

    @Test
    public void queueDeclareInvalidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            QueueDeclareOkBody response = interaction.negotiateOpen()
                                                     .channel()
                                                     .open()
                                                     .consumeResponse(ChannelOpenOkBody.class)
                                                     .queue()
                                                     .declareName(TEST_QUEUE)
                                                     .declareArguments(Collections.singletonMap("foo", "bar"))
                                                     .declare()
                                                     .consumeResponse()
                                                     .getLatestResponse(QueueDeclareOkBody.class);


            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(TEST_QUEUE))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));

        }
    }
}
