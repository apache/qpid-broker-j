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
package org.apache.qpid.tests.protocol.v1_0.extensions.qpid.message;


import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "broker.flowToDiskThreshold", value = "1")
@ConfigItem(name = "connection.maxUncommittedInMemorySize", value = "1")
public class MalformedMessageTest extends BrokerAdminUsingTestBase
{
    private static final String CONTENT_TEXT = "Test";

    @Test
    public void malformedMessage() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);

            final Flow flow = interaction.getLatestResponse(Flow.class);
            assertThat(flow.getLinkCredit().intValue(), Matchers.is(greaterThan(1)));

            final QpidByteBuffer payload = generateMalformed();
            interaction.transferSettled(true)
                       .transferPayload(payload)
                       .transferSettled(true)
                       .transfer();

            final Detach responseDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(true));
            assertThat(responseDetach.getError(), is(notNullValue()));
            assertThat(responseDetach.getError().getCondition(), is(equalTo(AmqpError.DECODE_ERROR)));

            interaction.doCloseConnection();
        }
    }

    private QpidByteBuffer generateMalformed()
    {
        final List<QpidByteBuffer> payload = new ArrayList<>();

        final Properties properties = new Properties();
        properties.setTo(BrokerAdmin.TEST_QUEUE_NAME);
        PropertiesSection propertiesSection = properties.createEncodingRetainingSection();
        final QpidByteBuffer props = propertiesSection.getEncodedForm();
        payload.add(props);
        propertiesSection.dispose();

        final AmqpValue amqpValue = new AmqpValue(CONTENT_TEXT);
        final AmqpValueSection dataSection = amqpValue.createEncodingRetainingSection();

        final QpidByteBuffer encodedData = dataSection.getEncodedForm();
        payload.add(encodedData.view(0, encodedData.remaining() - 1));
        encodedData.dispose();
        dataSection.dispose();

        final QpidByteBuffer combined = QpidByteBuffer.concatenate(payload);
        payload.forEach(QpidByteBuffer::dispose);
        return combined;
    }

}
