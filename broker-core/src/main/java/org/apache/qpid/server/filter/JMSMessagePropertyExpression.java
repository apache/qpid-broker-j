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
package org.apache.qpid.server.filter;
//
// Based on like named file from r450141 of the Apache ActiveMQ project <http://www.activemq.org/site/home.html>
//


import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a property  expression
 */
public class JMSMessagePropertyExpression implements PropertyExpression<FilterableMessage>
{
    public static final PropertyExpressionFactory<FilterableMessage> FACTORY = new PropertyExpressionFactory<FilterableMessage>()
    {
        @Override
        public PropertyExpression<FilterableMessage> createPropertyExpression(final String value)
        {
            return new JMSMessagePropertyExpression(value);
        }
    };

    // Constants - defined the same as JMS
    private static enum JMSDeliveryMode { NON_PERSISTENT, PERSISTENT }

    private static final int DEFAULT_PRIORITY = 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(JMSMessagePropertyExpression.class);

    private static final HashMap<String, Expression> JMS_PROPERTY_EXPRESSIONS = new HashMap<String, Expression>();
    static
    {
        JMS_PROPERTY_EXPRESSIONS.put("JMSDestination", new Expression<FilterableMessage>()
                                     {
                                         @Override
                                         public Object evaluate(FilterableMessage message)
                                         {
                                             //TODO
                                             return null;
                                         }
                                     });
        JMS_PROPERTY_EXPRESSIONS.put("JMSReplyTo", new ReplyToExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSType", new TypeExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSDeliveryMode", new DeliveryModeExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSPriority", new PriorityExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSMessageID", new MessageIDExpression());

        JMS_PROPERTY_EXPRESSIONS.put("AMQMessageID", new MessageIDExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSTimestamp", new TimestampExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSCorrelationID", new CorrelationIdExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSExpiration", new ExpirationExpression());

        JMS_PROPERTY_EXPRESSIONS.put("JMSRedelivered", new Expression<FilterableMessage>()
                                     {
                                         @Override
                                         public Object evaluate(FilterableMessage message)
                                         {
                                             return message.isRedelivered();
                                         }
                                     });
    }

    private final String name;
    private final Expression jmsPropertyExpression;

    public boolean outerTest()
    {
        return false;
    }

    private JMSMessagePropertyExpression(String name)
    {
        this.name = name;

        jmsPropertyExpression = JMS_PROPERTY_EXPRESSIONS.get(name);
    }

    @Override
    public Object evaluate(FilterableMessage message)
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

    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        return name;
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object o)
    {

        if ((o == null) || !this.getClass().equals(o.getClass()))
        {
            return false;
        }

        return name.equals(((JMSMessagePropertyExpression) o).name);

    }

    private static class ReplyToExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {
            String replyTo = message.getReplyTo();
            return replyTo;
        }

    }

    private static class TypeExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {

                String type = message.getType();
                return type;

        }
    }

    private static class DeliveryModeExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {
                JMSDeliveryMode mode = message.isPersistent() ? JMSDeliveryMode.PERSISTENT :
                                                                JMSDeliveryMode.NON_PERSISTENT;
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("JMSDeliveryMode is :" + mode);
                }

                return mode.toString();
        }
    }

    private static class PriorityExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {
            byte priority = message.getPriority();
            return (int) priority;
        }
    }

    private static class MessageIDExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {

            String messageId = message.getMessageId();

            return messageId;

        }
    }

    private static class TimestampExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {
            long timestamp = message.getTimestamp();
            return timestamp;
        }
    }

    private static class CorrelationIdExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {

            String correlationId = message.getCorrelationId();

            return correlationId;
        }
    }

    private static class ExpirationExpression implements Expression<FilterableMessage>
    {
        @Override
        public Object evaluate(FilterableMessage message)
        {
            long expiration = message.getExpiration();
            return expiration;

        }
    }
}
