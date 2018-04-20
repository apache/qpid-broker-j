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
package org.apache.qpid.tests.protocol.v1_0.extensions.bindmapjms;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class JMSMessagePropertiesFilterTest extends BrokerAdminUsingTestBase
{
    private static final String TEST_MESSAGE_CONTENT = "testContent";
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter."
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSMessageID representation for UUID values : "
                          + "'ID:AMQP_UUID:<string representation of uuid>'")
    public void selectorWithJMSMessageIDAsUUID() throws Exception
    {
        final UUID messageId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        perform(properties, String.format("JMSMessageID='ID:AMQP_UUID:%s'", messageId), "messageId", messageId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSMessageID representation for unsigned long values : "
                          + "'ID:AMQP_ULONG:<string representation of ulong>'")
    public void selectorWithJMSMessageIDAsUnsignedLong() throws Exception
    {
        final UnsignedLong messageId = UnsignedLong.ONE;
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        perform(properties,
                String.format("JMSMessageID='ID:AMQP_ULONG:%d'", messageId.longValue()),
                "messageId",
                messageId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSMessageID representation for Binary values : "
                          + "'ID:AMQP_BINARY:<hex representation of bytes>'")
    public void selectorWithJMSMessageIDAsBinary() throws Exception
    {
        byte[] data = {1, 2, 3};
        final Binary messageId = new Binary(data);
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        perform(properties,
                String.format("JMSMessageID='ID:AMQP_BINARY:%s'",
                              AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(data)),
                "messageId",
                messageId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSMessageID representation for String values without prefix 'ID:' : "
                          + "'ID:AMQP_NO_PREFIX:<original-string>'")
    public void selectorWithJMSMessageIDAsStringWithoutPrefix() throws Exception
    {
        final String messageId = "testId";
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        perform(properties, String.format("JMSMessageID='ID:AMQP_NO_PREFIX:%s'", messageId), "messageId", messageId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSMessageID representation for String values with prefix 'ID:' : "
                          + "'ID:<original-string>'")
    public void selectorWithJMSMessageIDAsStringWithPrefix() throws Exception
    {
        final String messageId = "ID:testId";
        final Properties properties = new Properties();
        properties.setMessageId(messageId);
        perform(properties, String.format("JMSMessageID='%s'", messageId), "messageId", messageId);
    }


    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter."
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSCorrelationID representation for UUID values : "
                          + "'ID:AMQP_UUID:<string representation of uuid>'")
    public void selectorWithJMSCorrelationIDAsUUID() throws Exception
    {
        final UUID correlationId = UUID.randomUUID();
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        perform(properties,
                String.format("JMSCorrelationID='ID:AMQP_UUID:%s'", correlationId),
                "correlationId",
                correlationId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSCorrelationID representation for unsigned long values : "
                          + "'ID:AMQP_ULONG:<string representation of ulong>'")
    public void selectorWithJMSCorrelationIDAsUnsignedLong() throws Exception
    {
        final UnsignedLong correlationId = UnsignedLong.ONE;
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        perform(properties,
                String.format("JMSCorrelationID='ID:AMQP_ULONG:%d'", correlationId.longValue()),
                "correlationId",
                correlationId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSCorrelationID representation for Binary values : "
                          + "'ID:AMQP_BINARY:<hex representation of bytes>'")
    public void selectorWithJMSCorrelationIDAsBinary() throws Exception
    {
        byte[] data = {1, 2, 3};
        final Binary correlationId = new Binary(data);
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        perform(properties,
                String.format("JMSCorrelationID='ID:AMQP_BINARY:%s'",
                              AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(data)),
                "correlationId",
                correlationId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSCorrelationID representation for String values without prefix 'ID:' : "
                          + "'<original-string>'")
    public void selectorWithJMSCorrelationIDAsStringWithoutPrefix() throws Exception
    {
        final String correlationId = "testId";
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        perform(properties,
                String.format("JMSCorrelationID='%s'", correlationId),
                "correlationId",
                correlationId);
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1",
            description = "A source can restrict the messages transferred from a source by specifying a filter.\n"
                          + ""
                          + "AMQP JMS Mapping\n"
                          + ""
                          + "3.2.1.1 JMSMessageID And JMSCorrelationID Handling\n"
                          + "JMSCorrelationID representation for String values with prefix 'ID:' : "
                          + "'ID:<original-string>'")
    public void selectorWithJMSCorrelationIDAsStringWithPrefix() throws Exception
    {
        final String correlationId = "ID:testId";
        final Properties properties = new Properties();
        properties.setCorrelationId(correlationId);
        perform(properties, String.format("JMSCorrelationID='%s'", correlationId), "correlationId", correlationId);
    }

    private void perform(final Properties properties,
                         final String selector,
                         final String propertyName,
                         final Object propertyValue) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);
            Flow flow = interaction.getLatestResponse(Flow.class);
            assumeThat("insufficient credit for the test", flow.getLinkCredit().intValue(), is(greaterThan(1)));

            for (int i = 0; i < 2; i++)
            {

                QpidByteBuffer payload =
                        generateMessagePayloadWithMessageProperties(properties,
                                                                    TEST_MESSAGE_CONTENT);
                interaction.transferPayload(payload)
                           .transferSettled(true)
                           .transfer();
            }
            interaction.detachClose(true).detach().close().sync();
        }

        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.RECEIVER)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                       .attachSourceFilter(Collections.singletonMap(Symbol.valueOf("selector-filter"),
                                                                    new JMSSelectorFilter(selector)))
                       .attach().consumeResponse()
                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow();

            Object data = interaction.receiveDelivery().decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(TEST_MESSAGE_CONTENT)));

            Properties deliveryProperties = interaction.getLatestDeliveryProperties();
            assertThat(deliveryProperties, is(notNullValue()));

            Method getter = Properties.class.getMethod("get"
                                                       + Character.toUpperCase(propertyName.charAt(0))
                                                       + propertyName.substring(1));
            assertThat(getter.invoke(deliveryProperties), is(equalTo(propertyValue)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Accepted())
                       .disposition();
            interaction.close().sync();
        }
    }

    private QpidByteBuffer generateMessagePayloadWithMessageProperties(final Properties properties, String content)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        messageEncoder.setProperties(properties);
        messageEncoder.addData(content);
        return messageEncoder.getPayload();
    }
}
