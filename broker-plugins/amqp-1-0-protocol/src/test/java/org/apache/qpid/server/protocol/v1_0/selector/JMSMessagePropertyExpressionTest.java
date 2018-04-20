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
package org.apache.qpid.server.protocol.v1_0.selector;

import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.AMQP_BINARY_PREFIX;
import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.AMQP_NO_PREFIX;
import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.AMQP_STRING_PREFIX;
import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.AMQP_ULONG_PREFIX;
import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.AMQP_UUID_PREFIX;
import static org.apache.qpid.server.protocol.v1_0.selector.AmqpMessageIdHelper.JMS_ID_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Test;

import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.filter.PropertyExpression;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;

public class JMSMessagePropertyExpressionTest
{

    @Test
    public void evaluateJMSMessageIDForStringWithIDPrefix()
    {
        final String id = "ID:testID";
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(id)));
    }

    @Test
    public void evaluateJMSMessageIDForStringWithoutIDPrefix()
    {
        final String id = "testID";
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_NO_PREFIX + id)));
    }

    @Test
    public void evaluateJMSMessageIDForUUID()
    {
        final UUID id = UUID.randomUUID();
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_UUID_PREFIX + id)));
    }

    @Test
    public void evaluateJMSMessageIDForUnsignedLong()
    {
        final UnsignedLong id = UnsignedLong.valueOf(42);
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_ULONG_PREFIX + id)));
    }

    @Test
    public void evaluateJMSMessageIDForBinary()
    {
        byte[] data = {1, 2, 3};
        final Binary id = new Binary(data);
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_BINARY_PREFIX + AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(data))));
    }

    @Test
    public void evaluateJMSMessageIDForStringWithAMQPBindingPrefixes()
    {
        final String id = "ID:AMQP_ULONG:string-id";
        final Filterable message = createFilterableWithMessageId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_MESSAGE_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_STRING_PREFIX + id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForStringWithIDPrefix()
    {
        final String id = "ID:testID";
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForStringWithoutIDPrefix()
    {
        final String id = "testID";
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForUUID()
    {
        final UUID id = UUID.randomUUID();
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_UUID_PREFIX + id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForUnsignedLong()
    {
        final UnsignedLong id = UnsignedLong.valueOf(42);
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_ULONG_PREFIX + id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForBinary()
    {
        byte[] data = {1, 2, 3};
        final Binary id = new Binary(data);
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value,
                   is(equalTo(JMS_ID_PREFIX
                              + AMQP_BINARY_PREFIX
                              + AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(data))));
    }

    @Test
    public void evaluateJMSCorrelationIDForStringWithAMQPBindingPrefixes()
    {
        final String id = "ID:AMQP_ULONG:string-id";
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMS_ID_PREFIX + AMQP_STRING_PREFIX + id)));
    }

    @Test
    public void evaluateJMSCorrelationIDForStringWithAMQPTypePrefixes()
    {
        final String id = "AMQP_ULONG:foo";
        final Filterable message = createFilterableWithCorrelationId(id);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_CORRELATION_ID);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(id)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void evaluateJMSDestination()
    {
        final String destinationName = "testDestination";
        final Message_1_0 message_1_0 = mock(Message_1_0.class);
        when(message_1_0.getTo()).thenReturn(destinationName);
        final Filterable message = mock(Filterable.class);
        when(message.getServerMessage()).thenReturn((ServerMessage) message_1_0);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_DESTINATION);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(destinationName)));
    }

    @Test
    public void evaluateJMSReplyTo()
    {
        final String replyTo = "testReplyTo";
        final Filterable message = mock(Filterable.class);
        when(message.getReplyTo()).thenReturn(replyTo);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_REPLY_TO);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(replyTo)));
    }

    @Test
    public void evaluateJMSType()
    {
        final String type = "testType";
        final Filterable message = mock(Filterable.class);
        when(message.getType()).thenReturn(type);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_TYPE);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(type)));
    }

    @Test
    public void evaluateJMSDeliveryModeForPersistentMessage()
    {
        final Filterable message = mock(Filterable.class);
        when(message.isPersistent()).thenReturn(true);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_DELIVERY_MODE);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMSMessagePropertyExpression.JMSDeliveryMode.PERSISTENT.toString())));
    }

    @Test
    public void evaluateJMSDeliveryModeForNonPersistentMessage()
    {
        final Filterable message = mock(Filterable.class);
        when(message.isPersistent()).thenReturn(false);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_DELIVERY_MODE);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(JMSMessagePropertyExpression.JMSDeliveryMode.NON_PERSISTENT.toString())));
    }

    @Test
    public void evaluateJMSPriority()
    {
        int priority = 5;
        final Filterable message = mock(Filterable.class);
        when(message.getPriority()).thenReturn((byte) priority);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_PRIORITY);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(priority)));
    }

    @Test
    public void evaluateJMSTimestamp()
    {
        long timestamp = System.currentTimeMillis();
        final Filterable message = mock(Filterable.class);
        when(message.getTimestamp()).thenReturn(timestamp);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_TIMESTAMP);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(timestamp)));
    }

    @Test
    public void evaluateJMSExpiration()
    {
        long expiration = System.currentTimeMillis() + 10000;
        final Filterable message = mock(Filterable.class);
        when(message.getExpiration()).thenReturn(expiration);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_EXPIRATION);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(expiration)));
    }

    @Test
    public void evaluateJMSRedelivered()
    {
        boolean redelivered = true;
        final Filterable message = mock(Filterable.class);
        when(message.isRedelivered()).thenReturn(redelivered);

        PropertyExpression<Filterable> expression =
                JMSMessagePropertyExpression.FACTORY.createPropertyExpression(JMSMessagePropertyExpression.JMS_REDELIVERED);
        Object value = expression.evaluate(message);

        assertThat(value, is(equalTo(redelivered)));
    }

    Filterable createFilterableWithCorrelationId(final Object id)
    {
        final Properties properties = mock(Properties.class);
        when(properties.getCorrelationId()).thenReturn(id);
        return createFilterable(properties);
    }

    Filterable createFilterableWithMessageId(final Object id)
    {
        final Properties properties = mock(Properties.class);
        when(properties.getMessageId()).thenReturn(id);
        return createFilterable(properties);
    }

    @SuppressWarnings("unchecked")
    Filterable createFilterable(final Properties properties)
    {
        final Filterable message = mock(Filterable.class);
        final Message_1_0 message_1_0 = mock(Message_1_0.class);
        final PropertiesSection propertiesSection = mock(PropertiesSection.class);
        when(propertiesSection.getValue()).thenReturn(properties);
        when(message_1_0.getPropertiesSection()).thenReturn(propertiesSection);
        when(message.getServerMessage()).thenReturn((ServerMessage) message_1_0);
        return message;
    }
}
