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
package org.apache.qpid.client;

import java.net.URISyntaxException;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Topic;

import org.apache.qpid.client.util.JMSExceptionHelper;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.messaging.Address;
import org.apache.qpid.url.BindingURL;

public class AMQTopic extends AMQDestination implements Topic
{

    private static final long serialVersionUID = -4773561540716587036L;

    public AMQTopic(String address) throws URISyntaxException
    {
        super(address);
    }

    public AMQTopic(Address address)
    {
        super(address);
    }

    public AMQTopic()
    {
        super();
    }

    /**
     * Constructor for use in creating a topic using a BindingURL.
     *
     * @param binding The binding url object.
     */
    public AMQTopic(BindingURL binding)
    {
        super(binding);
    }

    public AMQTopic(String exchange, String routingKey, String queueName)
    {
        super(exchange, ExchangeDefaults.TOPIC_EXCHANGE_CLASS, routingKey, true, true, queueName, false);
    }

    public AMQTopic(String exchange, String routingKey, String queueName, String[] bindingKeys)
    {
        super(exchange, ExchangeDefaults.TOPIC_EXCHANGE_CLASS, routingKey, true, true, queueName, false, bindingKeys);
    }

    public AMQTopic(AMQConnection conn, String routingKey)
    {
        this(conn.getDefaultTopicExchangeName(), routingKey);
    }

    public AMQTopic(String exchangeName, String routingKey)
    {
        this(exchangeName, routingKey, null);
    }

    public AMQTopic(String exchangeName, String name, boolean isAutoDelete, String queueName, boolean isDurable)
    {
        super(exchangeName, ExchangeDefaults.TOPIC_EXCHANGE_CLASS, name, true, isAutoDelete, queueName, isDurable);
    }


    protected AMQTopic(String exchangeName, String exchangeClass, String name, boolean isAutoDelete, String queueName, boolean isDurable)
    {
        super(exchangeName, exchangeClass, name, true, isAutoDelete, queueName, isDurable);
    }

    protected AMQTopic(String exchangeName, String exchangeClass, String routingKey, boolean isExclusive,
                               boolean isAutoDelete, String queueName, boolean isDurable)
    {
        super(exchangeName, exchangeClass, routingKey, isExclusive, isAutoDelete, queueName, isDurable );
    }

    protected AMQTopic(String exchangeName, String exchangeClass, String routingKey, boolean isExclusive,
            boolean isAutoDelete, String queueName, boolean isDurable, String[] bindingKeys)
    {
        super(exchangeName, exchangeClass, routingKey, isExclusive, isAutoDelete, queueName, isDurable, bindingKeys);
    }

    public static AMQTopic createDurableTopic(Topic topic, String subscriptionName, AMQConnection connection)
            throws JMSException
    {
        if (topic instanceof AMQDestination)
        {
            AMQDestination qpidTopic = (AMQDestination)topic;
            if (qpidTopic.getDestSyntax() == DestSyntax.ADDR)
            {
                try
                {
                    AMQTopic t = new AMQTopic(qpidTopic.getAddress());
                    String queueName = getDurableTopicQueueName(subscriptionName, connection);
                    // link is never null if dest was created using an address string.
                    t.getLink().setName(queueName);
                    t.getLink().getSubscriptionQueue().setAutoDelete(false);
                    t.getLink().setDurable(true);

                    // The legacy fields are also populated just in case.
                    t.setQueueName(queueName);
                    t.setAutoDelete(false);
                    t.setDurable(true);
                    return t;
                }
                catch(Exception e)
                {
                    throw JMSExceptionHelper.chainJMSException(new JMSException("Error creating durable topic"), e);
                }
            }
            else
            {
                return new AMQTopic(qpidTopic.getExchangeName(), qpidTopic.getExchangeClass(), qpidTopic.getRoutingKey(), false,
                                getDurableTopicQueueName(subscriptionName, connection),
                                true);
            }
        }
        else
        {
            throw new InvalidDestinationException("The destination object used is not from this provider or of type javax.jms.Topic");
        }
    }

    public static String getDurableTopicQueueName(String subscriptionName, AMQConnection connection) throws JMSException
    {
        return connection.getClientID() + ":" + subscriptionName;
    }

    public String getTopicName() throws JMSException
    {
        if (getRoutingKey() != null)
        {
            return getRoutingKey();
        }
        else if (getSubject() != null)
        {
            return getSubject();
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getExchangeName()
    {
        if (super.getExchangeName() == null && super.getAddressName() != null)
        {
            return super.getAddressName();
        }
        else
        {
            return super.getExchangeName();
        }
    }

    public String getRoutingKey()
    {
        if (super.getRoutingKey() != null)
        {
            return super.getRoutingKey();
        }
        else if (getSubject() != null)
        {
            return getSubject();
        }
        else
        {
            setRoutingKey("");
            setSubject("");
            return super.getRoutingKey();
        }
    }

    public boolean isNameRequired()
    {
        return !isDurable();
    }

    public boolean equals(Object o)
    {
        if (getDestSyntax() == DestSyntax.ADDR)
        {
            return super.equals(o);
        }
        else
        {
            return (o instanceof AMQTopic)
               && ((AMQTopic)o).getExchangeName().equals(getExchangeName())
               && ((AMQTopic)o).getRoutingKey().equals(getRoutingKey());
        }
    }

    public int hashCode()
    {
        if (getDestSyntax() == DestSyntax.ADDR)
        {
            return super.hashCode();
        }
        else
        {
            return getExchangeName().hashCode() + getRoutingKey().hashCode();
        }
    }
}
