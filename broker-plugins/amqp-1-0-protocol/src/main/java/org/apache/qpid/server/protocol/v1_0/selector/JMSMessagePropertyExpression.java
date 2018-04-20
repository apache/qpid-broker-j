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

import java.util.HashMap;

import org.apache.qpid.server.filter.Expression;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.filter.PropertyExpression;
import org.apache.qpid.server.filter.PropertyExpressionFactory;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;

public class JMSMessagePropertyExpression implements PropertyExpression<Filterable>
{
    public static final PropertyExpressionFactory<Filterable> FACTORY = JMSMessagePropertyExpression::new;

    static final String JMS_MESSAGE_ID = "JMSMessageID";
    static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    static final String JMS_DESTINATION = "JMSDestination";
    static final String JMS_REPLY_TO = "JMSReplyTo";
    static final String JMS_TYPE = "JMSType";
    static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    static final String JMS_PRIORITY = "JMSPriority";
    static final String JMS_TIMESTAMP = "JMSTimestamp";
    static final String JMS_EXPIRATION = "JMSExpiration";
    static final String JMS_REDELIVERED = "JMSRedelivered";

    private static final HashMap<String, Expression> JMS_PROPERTY_EXPRESSIONS = new HashMap<>();

    static
    {
        JMS_PROPERTY_EXPRESSIONS.put(JMS_DESTINATION,
                                     (Expression<Filterable>) message -> message.getServerMessage().getTo());
        JMS_PROPERTY_EXPRESSIONS.put(JMS_REPLY_TO, (Expression<Filterable>) Filterable::getReplyTo);
        JMS_PROPERTY_EXPRESSIONS.put(JMS_TYPE, (Expression<Filterable>) Filterable::getType);
        JMS_PROPERTY_EXPRESSIONS.put(JMS_DELIVERY_MODE,
                                     (Expression<Filterable>) message -> (message.isPersistent()
                                             ? JMSDeliveryMode.PERSISTENT
                                             : JMSDeliveryMode.NON_PERSISTENT).toString());
        JMS_PROPERTY_EXPRESSIONS.put(JMS_PRIORITY, (Expression<Filterable>) message -> (int) message.getPriority());
        JMS_PROPERTY_EXPRESSIONS.put(JMS_MESSAGE_ID, new MessageIDExpression());
        JMS_PROPERTY_EXPRESSIONS.put(JMS_TIMESTAMP, (Expression<Filterable>) Filterable::getTimestamp);
        JMS_PROPERTY_EXPRESSIONS.put(JMS_CORRELATION_ID, new CorrelationIdExpression());
        JMS_PROPERTY_EXPRESSIONS.put(JMS_EXPIRATION, (Expression<Filterable>) Filterable::getExpiration);
        JMS_PROPERTY_EXPRESSIONS.put(JMS_REDELIVERED, (Expression<Filterable>) Filterable::isRedelivered);
    }

    private final String name;
    private final Expression jmsPropertyExpression;

    private JMSMessagePropertyExpression(String name)
    {
        this.name = name;
        jmsPropertyExpression = JMS_PROPERTY_EXPRESSIONS.get(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(Filterable message)
    {
        if (jmsPropertyExpression != null)
        {
            return jmsPropertyExpression.evaluate(message);
        }
        else
        {
            return message.getHeader(name);
        }
    }

    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return (o != null)
               && this.getClass().equals(o.getClass())
               && name.equals(((JMSMessagePropertyExpression) o).name);
    }

    private static class MessageIDExpression implements Expression<Filterable>
    {
        @Override
        public Object evaluate(Filterable message)
        {
            Message_1_0 original = (Message_1_0) message.getServerMessage();
            PropertiesSection propertiesSection = original.getPropertiesSection();
            String id = null;
            if (propertiesSection != null)
            {
                Properties properties = propertiesSection.getValue();
                if (properties != null)
                {
                    id = AmqpMessageIdHelper.INSTANCE.toMessageIdString(properties.getMessageId());
                }
            }
            return id;
        }
    }

    private static class CorrelationIdExpression implements Expression<Filterable>
    {
        @Override
        public Object evaluate(Filterable message)
        {

            Message_1_0 original = (Message_1_0) message.getServerMessage();
            PropertiesSection propertiesSection = original.getPropertiesSection();
            String id = null;
            if (propertiesSection != null)
            {
                Properties properties = propertiesSection.getValue();
                if (properties != null)
                {
                    id = AmqpMessageIdHelper.INSTANCE.toCorrelationIdString(properties.getCorrelationId());
                }
            }
            return id;
        }
    }

    // Constants - defined the same as JMS
    enum JMSDeliveryMode
    {
        NON_PERSISTENT, PERSISTENT
    }
}
