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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
        producer.close();
    }

    public void updateEntityUsingAmqpManagement(final String name,
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
        createMessage.setStringProperty("operation", "UPDATE");
        createMessage.setStringProperty("index", "object-path");
        createMessage.setStringProperty("key", name);
        for (Map.Entry<String, Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if (session.getTransacted())
        {
            session.commit();
        }
        producer.close();
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

    public Object performOperationUsingAmqpManagement(final String name,
                                                                         final String operation,
                                                                         final Session session,
                                                                         final String type,
                                                                         Map<String, Object> arguments)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_qpidBrokerTestCase.isBroker10()
                                                                                      ? "$management"
                                                                                      : "ADDR:$management"));
        final TemporaryQueue responseQ = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(responseQ);
        MapMessage opMessage = session.createMapMessage();
        opMessage.setStringProperty("type", type);
        opMessage.setStringProperty("operation", operation);
        opMessage.setStringProperty("index", "object-path");
        opMessage.setJMSReplyTo(responseQ);

        opMessage.setStringProperty("key", name);
        for (Map.Entry<String, Object> argument : arguments.entrySet())
        {
            Object value = argument.getValue();
            if (value.getClass().isPrimitive() || value instanceof String)
            {
                opMessage.setObjectProperty(argument.getKey(), value);
            }
            else
            {
                ObjectMapper objectMapper = new ObjectMapper();
                String jsonifiedValue = null;
                try
                {
                    jsonifiedValue = objectMapper.writeValueAsString(value);
                }
                catch (JsonProcessingException e)
                {
                    throw new IllegalArgumentException(String.format(
                            "Cannot convert the argument '%s' to JSON to meet JMS type restrictions", argument.getKey()));
                }
                opMessage.setObjectProperty(argument.getKey(), jsonifiedValue);
            }
        }

        producer.send(opMessage);
        if (session.getTransacted())
        {
            session.commit();
        }

        Message response = consumer.receive(5000);
        try
        {
            if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                Map<String, Object> result = new TreeMap<>();
                Enumeration mapNames = bodyMap.getMapNames();
                while (mapNames.hasMoreElements())
                {
                    String key = (String) mapNames.nextElement();
                    result.put(key, bodyMap.getObject(key));
                }
                return result;
            }
            else if (response instanceof ObjectMessage)
            {
                return ((ObjectMessage) response).getObject();
            }
            else if (response instanceof BytesMessage)
            {
                BytesMessage bytesMessage = (BytesMessage) response;
                if (bytesMessage.getBodyLength() == 0)
                {
                    return null;
                }
                else
                {
                    byte[] buf = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(buf);
                    return buf;
                }
            }
            throw new IllegalArgumentException("Cannot parse the results from a management operation.  JMS response message : " + response);
        }
        finally
        {
            if (session.getTransacted())
            {
                session.commit();
            }
            consumer.close();
            responseQ.delete();
        }
    }

    public List<Map<String, Object>> managementQueryObjects(final Session session, final String type) throws JMSException
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

        Message response = consumer.receive(5000);
        try
        {
            if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                List<String> attributeNames = (List<String>) bodyMap.getObject("attributeNames");
                List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.getObject("results");
                return getResultsAsMaps(attributeNames, attributeValues);
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    Map<String, ?> bodyMap = (Map<String, ?>) body;
                    List<String> attributeNames = (List<String>) bodyMap.get("attributeNames");
                    List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.get("results");
                    return getResultsAsMaps(attributeNames, attributeValues);
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

    public Map<String, Object> readEntityUsingAmqpManagement(final Session session,
                                                             final String type,
                                                             final String name,
                                                             final boolean actuals) throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(_qpidBrokerTestCase.isBroker10()
                                                                                      ? "$management"
                                                                                      : "ADDR:$management"));

        final TemporaryQueue responseQueue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(responseQueue);

        MapMessage request = session.createMapMessage();
        request.setStringProperty("type", type);
        request.setStringProperty("operation", "READ");
        request.setString("name", name);
        request.setString("object-path", name);
        request.setStringProperty("index", "object-path");
        request.setStringProperty("key", name);
        request.setBooleanProperty("actuals", actuals);
        request.setJMSReplyTo(responseQueue);

        producer.send(request);
        if (session.getTransacted())
        {
            session.commit();
        }

        Message response = consumer.receive(5000);
        if (session.getTransacted())
        {
            session.commit();
        }
        try
        {
            if (response instanceof MapMessage)
            {
                MapMessage bodyMap = (MapMessage) response;
                Map<String, Object> data = new HashMap<>();
                Enumeration<String> keys = bodyMap.getMapNames();
                while (keys.hasMoreElements())
                {
                    String key = keys.nextElement();
                    data.put(key, bodyMap.getObject(key));
                }
                return data;
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    Map<String, ?> bodyMap = (Map<String, ?>) body;
                    return new HashMap<>(bodyMap);
                }
            }
            throw new IllegalArgumentException("Management read failed : " + response.getStringProperty("statusCode") + " - " + response.getStringProperty("statusDescription"));
        }
        finally
        {
            consumer.close();
            responseQueue.delete();
        }
    }

    private List<Map<String, Object>> getResultsAsMaps(final List<String> attributeNames, final List<List<Object>> attributeValues)
    {
        List<Map<String, Object>> results = new ArrayList<>();
        for (List<Object> resultObject : attributeValues)
        {
            Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < attributeNames.size(); ++i)
            {
                result.put(attributeNames.get(i), resultObject.get(i));
            }
            results.add(result);
        }
        return results;
    }
}