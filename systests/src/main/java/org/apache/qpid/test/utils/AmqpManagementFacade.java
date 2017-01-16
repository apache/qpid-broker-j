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

package org.apache.qpid.test.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

public class AmqpManagementFacade
{
    private final QpidBrokerTestCase _qpidBrokerTestCase;

    public AmqpManagementFacade(QpidBrokerTestCase _qpidBrokerTestCase)
    {
        this._qpidBrokerTestCase = _qpidBrokerTestCase;
    }

    public void createEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        createEntityUsingAmqpManagement(name, session, type, Collections.<String, Object>emptyMap());
    }

    public void createEntityUsingAmqpManagement(final String name,
                                                final Session session,
                                                final String type,
                                                Map<String, Object> attributes)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_qpidBrokerTestCase.isBroker10()
                                                                                      ? "$management"
                                                                                      : "ADDR:$management"));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "CREATE");
        createMessage.setString("name", name);
        createMessage.setString("object-path", name);
        for (Map.Entry<String, Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
    }

    public void deleteEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_qpidBrokerTestCase.isBroker10()
                                                                                      ? "$management"
                                                                                      : "ADDR:$management"));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "DELETE");
        createMessage.setStringProperty("index", "object-path");

        createMessage.setStringProperty("key", name);
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
    }

    public void performOperationUsingAmqpManagement(final String name,
                                                    final String operation,
                                                    final Session session,
                                                    final String type,
                                                    Map<String, Object> arguments)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_qpidBrokerTestCase.isBroker10()
                                                                                      ? "$management"
                                                                                      : "ADDR:$management"));

        MapMessage opMessage = session.createMapMessage();
        opMessage.setStringProperty("type", type);
        opMessage.setStringProperty("operation", operation);
        opMessage.setStringProperty("index", "object-path");

        opMessage.setStringProperty("key", name);
        for (Map.Entry<String, Object> argument : arguments.entrySet())
        {
            opMessage.setObjectProperty(argument.getKey(), argument.getValue());
        }

        producer.send(opMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
    }

    public List managementQueryObjects(final Session session, final String type) throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue("$management"));
        final TemporaryQueue responseQ = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(responseQ);
        MapMessage message = session.createMapMessage();
        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setStringProperty("entityType", type);
        message.setString("attributeNames", "[]");
        message.setJMSReplyTo(responseQ);

        producer.send(message);

        Message response = consumer.receive();
        try
        {
            if (response instanceof MapMessage)
            {
                return (List) ((MapMessage) response).getObject("results");
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    return (List) ((Map) body).get("results");
                }
            }
            throw new IllegalArgumentException("Cannot parse the results from a management query");
        }
        finally
        {
            consumer.close();
            responseQ.delete();
        }
    }
}