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
package org.apache.qpid.systest.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.jms.ListMessage;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class MessagesRestTest extends QpidRestTestCase
{

    /**
     * Message number to publish into queue
     */
    private static final int MESSAGE_NUMBER = 12;
    private static final String STRING_PROP = "shortstring";
    // Dollar Pound Euro: 1 byte, 2 byte, and 3 byte UTF-8 encodings respectively
    private static final String STRING_VALUE = "\u0024 \u00A3 \u20AC";
    private static final String GET_MESSAGE_CONTENT_BY_ID =
            "queue/test/test/%s/getMessageContent?returnJson=%s&messageId=%d";
    private static final String GET_MESSAGE_INFO_BY_ID = "queue/test/test/%s/getMessageInfoById?messageId=%d";
    private static final String GET_MESSAGE_INFO = "queue/test/test/%s/getMessageInfo";

    private Connection _connection;
    private Session _session;
    private MessageProducer _producer;
    private long _startTime;
    private long _ttl;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _startTime = System.currentTimeMillis();
        _connection = getConnection();
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination queue = createTestQueue(_session);
        _session.commit();
        _producer = _session.createProducer(queue);

        _ttl = TimeUnit.DAYS.toMillis(1);
        for (int i = 0; i < MESSAGE_NUMBER; i++)
        {
            Message m = _session.createTextMessage("Test-" + i);
            m.setIntProperty("index", i);
            if (i % 2 == 0)
            {
                _producer.send(m);
            }
            else
            {
                _producer.send(m, DeliveryMode.NON_PERSISTENT, 5, _ttl);
            }
        }
        _session.commit();
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        // Allow retrieval of message information on an insecure (non-tls) connection
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT,
                                                           HttpPort.ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS,
                                                           true);
    }

    public void testGet() throws Exception
    {
        String queueName = getTestQueueName();
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER, messages.size());
        int position = 0;
        for (Map<String, Object> message : messages)
        {
            assertMessage(position, message);
            position++;
        }
    }

    public void testGetMessageContent() throws Exception
    {
        String queueName = getTestQueueName();

        // add bytes message
        Message textMessage = _session.createTextMessage(STRING_VALUE);
        textMessage.setStringProperty(STRING_PROP, STRING_VALUE);
        _producer.send(textMessage);
        _session.commit();

        // get message IDs
        Long lastMessageId = getLastMessageIdAndVerifyMimeType(queueName, "text/plain");

        Map<String, Object> message = getRestTestHelper().getJsonAsMap(String.format(GET_MESSAGE_INFO_BY_ID,
                                                                                     queueName,
                                                                                     lastMessageId));
        @SuppressWarnings("unchecked")
        Map<String, Object> messageHeader = (Map<String, Object>) message.get("headers");
        assertNotNull("Message headers are not found", messageHeader);
        assertEquals("Unexpected message header value", STRING_VALUE, messageHeader.get(STRING_PROP));

        // get content
        byte[] data = getRestTestHelper().getBytes(String.format(GET_MESSAGE_CONTENT_BY_ID,
                                                                 queueName,
                                                                 false,
                                                                 lastMessageId));
        assertTrue("Unexpected message for id " + lastMessageId + ":" + data.length,
                   Arrays.equals(STRING_VALUE.getBytes(StandardCharsets.UTF_8), data));
    }

    public void testGetMapMessageContentAsJson() throws Exception
    {
        String queueName = getTestQueueName();
        MapMessage mapMessage = _session.createMapMessage();
        mapMessage.setString("testStringProperty", "My String");
        mapMessage.setInt("testIntProperty", 999999);
        _producer.send(mapMessage);
        _session.commit();

        Long lastMessageId = getLastMessageIdAndVerifyMimeType(queueName, "amqp/map");

        Map<String, Object> jsonMessageData = getRestTestHelper().getJsonAsMap(String.format(
                GET_MESSAGE_CONTENT_BY_ID,
                queueName,
                true,
                lastMessageId));
        assertEquals("Unexpected map content size", 2, jsonMessageData.size());
        assertEquals("Unexpected testStringProperty", "My String", jsonMessageData.get("testStringProperty"));
        assertEquals("Unexpected testIntProperty", 999999, jsonMessageData.get("testIntProperty"));
    }

    public void testGetStreamMessageContentAsJson() throws Exception
    {
        String queueName = getTestQueueName();
        StreamMessage streamMessage = _session.createStreamMessage();
        streamMessage.writeLong(Long.MAX_VALUE);
        streamMessage.writeBoolean(true);
        streamMessage.writeString("Hello world");
        _producer.send(streamMessage);
        _session.commit();

        Long lastMessageId = getLastMessageIdAndVerifyMimeType(queueName, "jms/stream-message");

        List<Object> jsonMessageData = getRestTestHelper().getJsonAsSimpleList(String.format(
                GET_MESSAGE_CONTENT_BY_ID,
                queueName,
                true,
                lastMessageId));
        assertEquals("Unexpected list content size", 3, jsonMessageData.size());
        assertEquals("Unexpected value at index 0", Long.MAX_VALUE, jsonMessageData.get(0));
        assertEquals("Unexpected value at index 1", true, jsonMessageData.get(1));
        assertEquals("Unexpected value at index 2", "Hello world", jsonMessageData.get(2));
    }

    public void testGetBytesMessageContentAsJson() throws Exception
    {
        String queueName = getTestQueueName();
        BytesMessage bytesMessage = _session.createBytesMessage();
        final byte[] payloadBytes = {1, 2, 3, 4};
        bytesMessage.writeBytes(payloadBytes);
        _producer.send(bytesMessage);
        _session.commit();

        Long lastMessageId = getLastMessageIdAndVerifyMimeType(queueName, "application/octet-stream");

        List<Object> jsonMessageData = getRestTestHelper().getJsonAsSimpleList(String.format(
                GET_MESSAGE_CONTENT_BY_ID,
                queueName,
                true,
                lastMessageId));
        assertEquals("Unexpected list content size", payloadBytes.length, jsonMessageData.size());
        for(int i = 0; i < payloadBytes.length; i++)
        {
            assertEquals("Unexpected value at index " + i, ((Number)payloadBytes[i]).intValue(), jsonMessageData.get(i));
        }
    }

    public void testGetListMessageContentAsJson() throws Exception
    {
        String queueName = getTestQueueName();
        ListMessage listMessage = ((org.apache.qpid.jms.Session) _session).createListMessage();
        listMessage.add(999999);
        listMessage.add("My String");
        _producer.send(listMessage);
        _session.commit();

        Long lastMessageId = getLastMessageIdAndVerifyMimeType(queueName, "amqp/list");

        List<Object> jsonMessageData = getRestTestHelper().getJsonAsSimpleList(String.format(
                GET_MESSAGE_CONTENT_BY_ID,
                queueName,
                true,
                lastMessageId));
        assertEquals("Unexpected list size", 2, jsonMessageData.size());
        assertEquals("Unexpected value at index 0", 999999, jsonMessageData.get(0));
        assertEquals("Unexpected value at index 1", "My String", jsonMessageData.get(1));
    }

    public void testPostMoveMessages() throws Exception
    {
        String queueName = getTestQueueName();
        String queueName2 = queueName + "_2";
        Destination queue2 = createTestQueue(_session, queueName2);
        _session.commit();

        // get message IDs
        List<Long> ids = getMesssageIds(queueName);

        // move half of the messages
        int movedNumber = ids.size() / 2;
        List<Long> movedMessageIds = new ArrayList<>();
        for (int i = 0; i < movedNumber; i++)
        {
            movedMessageIds.add(ids.remove(i));
        }

        // move messages

        Map<String, Object> messagesData = new HashMap<>();
        messagesData.put("messageIds", movedMessageIds);
        messagesData.put("destination", queueName2);

        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/moveMessages",
                                          "POST",
                                          messagesData,
                                          HttpServletResponse.SC_OK);

        // check messages on target queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName2));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", movedMessageIds.size(), messages.size());
        for (Long id : movedMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }

        // check messages on original queue
        messages = getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", ids.size(), messages.size());
        for (Long id : ids)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }
        for (Long id : movedMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertNull("Moved message " + id + " is found on original queue", message);
        }
    }

    public void testPostMoveMessagesWithSelector() throws Exception
    {
        String queueName = getTestQueueName();
        String queueName2 = queueName + "_2";
        createTestQueue(_session, queueName2);
        _session.commit();
        // move messages

        Map<String, Object> messagesData = new HashMap<>();
        messagesData.put("selector", "index % 2 = 0");
        messagesData.put("destination", queueName2);

        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/moveMessages",
                                          "POST",
                                          messagesData,
                                          HttpServletResponse.SC_OK);

        // check messages on target queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName2));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER / 2, messages.size());
        final List<Long> movedMessageIds = getMesssageIds(queueName2);
        for (Long id : movedMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
            assertMessageAttributeValues(message, true);
        }

        // check messages on original queue
        messages = getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER / 2, messages.size());

        for (Long id : getMesssageIds(queueName))
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
            assertMessageAttributeValues(message, false);
        }
        for (Long id : movedMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertNull("Moved message " + id + " is found on original queue", message);
        }
    }


    public void testPostMoveAllMessages() throws Exception
    {
        String queueName = getTestQueueName();
        String queueName2 = queueName + "_2";
        createTestQueue(_session, queueName2);
        _session.commit();
        // get message IDs
        List<Long> ids = getMesssageIds(queueName);

        // move messages

        Map<String, Object> messagesData = new HashMap<>();
        messagesData.put("destination", queueName2);

        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/moveMessages",
                                          "POST",
                                          messagesData,
                                          HttpServletResponse.SC_OK);

        // check messages on target queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName2));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER, messages.size());
        final List<Long> movedMessageIds = getMesssageIds(queueName2);
        for (Long id : movedMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }

        // check messages on original queue
        messages = getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", 0, messages.size());
    }


    public void testPostCopyMessages() throws Exception
    {
        String queueName = getTestQueueName();
        String queueName2 = queueName + "_2";
        Destination queue2 = createTestQueue(_session, queueName2);
        _session.commit();
        // get message IDs
        List<Long> ids = getMesssageIds(queueName);

        // copy half of the messages
        int copyNumber = ids.size() / 2;
        List<Long> copyMessageIds = new ArrayList<>();
        for (int i = 0; i < copyNumber; i++)
        {
            copyMessageIds.add(ids.remove(i));
        }

        // copy messages
        Map<String, Object> messagesData = new HashMap<>();
        messagesData.put("messageIds", copyMessageIds);
        messagesData.put("destination", queueName2);

        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/copyMessages",
                                          "POST",
                                          messagesData,
                                          HttpServletResponse.SC_OK);

        // check messages on target queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName2));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", copyMessageIds.size(), messages.size());
        for (Long id : copyMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }

        // check messages on original queue
        messages = getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER, messages.size());
        for (Long id : ids)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }
        for (Long id : copyMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }
    }

    public void testPostCopyMessagesWithSelectorAndLimit() throws Exception
    {
        String queueName = getTestQueueName();
        String queueName2 = queueName + "_2";
        Destination queue2 = createTestQueue(_session, queueName2);
        _session.commit();

        // copy messages
        Map<String, Object> messagesData = new HashMap<>();
        messagesData.put("selector", "index % 2 = 0");
        messagesData.put("limit", 1);
        messagesData.put("destination", queueName2);

        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/copyMessages",
                                          "POST",
                                          messagesData,
                                          HttpServletResponse.SC_OK);

        // check messages on target queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName2));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", 1, messages.size());
        for (Long id : getMesssageIds(queueName2))
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
            assertMessageAttributeValues(message, true);
        }

        // check messages on original queue
        messages = getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER, messages.size());
    }


    public void testDeleteMessages() throws Exception
    {
        String queueName = getTestQueueName();

        // get message IDs
        List<Long> ids = getMesssageIds(queueName);

        // delete half of the messages
        int deleteNumber = ids.size() / 2;
        List<Long> deleteMessageIds = new ArrayList<>();
        for (int i = 0; i < deleteNumber; i++)
        {
            Long id = ids.remove(i);
            deleteMessageIds.add(id);
        }

        // delete messages
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("messageIds", deleteMessageIds);
        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/deleteMessages",
                                          "POST",
                                          parameters,
                                          HttpServletResponse.SC_OK);

        // check messages on queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", ids.size(), messages.size());
        for (Long id : ids)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertMessageAttributes(message);
        }
        for (Long id : deleteMessageIds)
        {
            Map<String, Object> message = getRestTestHelper().find("id", id.intValue(), messages);
            assertNull("Message with id " + id + " was not deleted", message);
        }
    }

    public void testDeleteMessagesWithLimit() throws Exception
    {
        String queueName = getTestQueueName();

        // get message IDs
        List<Long> ids = getMesssageIds(queueName);

        // delete half of the messages
        int deleteNumber = MESSAGE_NUMBER / 2;

        // delete messages
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("limit", deleteNumber);
        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/deleteMessages",
                                          "POST",
                                          parameters,
                                          HttpServletResponse.SC_OK);

        // check messages on queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", MESSAGE_NUMBER / 2, messages.size());
    }


    public void testClearQueue() throws Exception
    {
        String queueName = getTestQueueName();

        // clear queue
        getRestTestHelper().submitRequest("queue/test/test/" + queueName + "/clearQueue", "POST",
                                          Collections.<String, Object>emptyMap(), HttpServletResponse.SC_OK);

        // check messages on queue
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        assertNotNull("Messages are not found", messages);
        assertEquals("Unexpected number of messages", 0, messages.size());
    }


    private List<Long> getMesssageIds(String queueName) throws IOException
    {
        List<Map<String, Object>> messages =
                getRestTestHelper().getJsonAsList(String.format(GET_MESSAGE_INFO, queueName));
        List<Long> ids = new ArrayList<>();
        for (Map<String, Object> message : messages)
        {
            ids.add(((Number) message.get("id")).longValue());
        }
        return ids;
    }

    private void assertMessage(int position, Map<String, Object> message)
    {
        assertMessageAttributes(message);

        assertEquals("Unexpected message attribute size", position < 10 ? 6 : 7, message.get("size"));
        boolean even = position % 2 == 0;
        assertMessageAttributeValues(message, even);
    }

    private void assertMessageAttributeValues(Map<String, Object> message, boolean even)
    {
        if (even)
        {
            assertNull("Unexpected message attribute expirationTime", message.get("expirationTime"));
            assertEquals("Unexpected message attribute priority", 4, message.get("priority"));
            assertEquals("Unexpected message attribute persistent", Boolean.TRUE, message.get("persistent"));
        }
        else
        {
            assertEquals("Unexpected message attribute expirationTime", ((Number) message.get("timestamp")).longValue()
                                                                        + _ttl, message.get("expirationTime"));
            assertEquals("Unexpected message attribute priority", 5, message.get("priority"));
            assertEquals("Unexpected message attribute persistent", Boolean.FALSE, message.get("persistent"));
        }
        assertEquals("Unexpected message attribute mimeType", "text/plain", message.get("mimeType"));
        assertEquals("Unexpected message attribute userId", "guest", message.get("userId"));
        assertEquals("Unexpected message attribute deliveryCount", 0, message.get("deliveryCount"));
        assertEquals("Unexpected message attribute state", "Available", message.get("state"));
    }

    private void assertMessageAttributes(Map<String, Object> message)
    {
        assertNotNull("Message map cannot be null", message);
        assertNotNull("Unexpected message attribute deliveryCount", message.get("deliveryCount"));
        assertNotNull("Unexpected message attribute state", message.get("state"));
        assertNotNull("Unexpected message attribute id", message.get("id"));
        assertNotNull("Message arrivalTime cannot be null", message.get("arrivalTime"));
        assertNotNull("Message timestamp cannot be null", message.get("timestamp"));
        assertTrue("Message arrivalTime cannot be null",
                   ((Number) message.get("arrivalTime")).longValue() > _startTime);
        assertNotNull("Message messageId cannot be null", message.get("messageId"));
        assertNotNull("Unexpected message attribute mimeType", message.get("mimeType"));
        assertNotNull("Unexpected message attribute userId", message.get("userId"));
        assertNotNull("Message priority cannot be null", message.get("priority"));
        assertNotNull("Message persistent cannot be null", message.get("persistent"));
    }


    private Long getLastMessageIdAndVerifyMimeType(final String queueName, final String mimeType) throws IOException
    {
        List<Long> ids = getMesssageIds(queueName);
        int lastMessageIndex = ids.size() - 1;
        List<Map<String, Object>> messages = getRestTestHelper().getJsonAsList("queue/test/test/"
                                                                               + queueName
                                                                               + "/getMessageInfo?first="
                                                                               + lastMessageIndex
                                                                               + "&last="
                                                                               + lastMessageIndex);
        assertEquals("Unexpected message number returned", 1, messages.size());
        Map<String, Object> message = messages.get(0);
        assertEquals("Unexpected message attribute mimeType", mimeType, message.get("mimeType"));
        assertMessageAttributes(message);

        return ids.get(lastMessageIndex);
    }
}
