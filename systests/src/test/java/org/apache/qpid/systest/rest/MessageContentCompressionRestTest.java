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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class MessageContentCompressionRestTest extends QpidBrokerTestCase
{
    private static String VIRTUAL_HOST = "test";
    private RestTestHelper _restTestHelper;

    @Override
    public void startDefaultBroker()
    {
        // tests are starting the broker
    }

    public void doActualSetUp() throws Exception
    {
        TestBrokerConfiguration config = getDefaultBrokerConfiguration();
        config.addHttpManagementConfiguration();
        config.setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT,
                                  Port.ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS,
                                  true);
        super.startDefaultBroker();

        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
    }

    @Override
    protected void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            _restTestHelper.tearDown();
        }
    }

    public void testGetContentViaRestForCompressedMessageWithAgentNotSupportingCompression() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        String messageText = createMessageText();
        Connection senderConnection = getConnection(true);
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue testQueue = createTestQueue(session);

        publishMessage(senderConnection, messageText, testQueue);

        String queueRelativePath =  "queue/" + VIRTUAL_HOST  + "/" + VIRTUAL_HOST + "/" + testQueue.getQueueName();

        List<Map<String, Object>> messages = _restTestHelper.getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertEquals("Unexpected number of messages", 1, messages.size());
        long id = ((Number) messages.get(0).get("id")).longValue();

        byte[] messageBytes = _restTestHelper.getBytes(queueRelativePath + "/getMessageContent?messageId=" + id);
        String content = new String(messageBytes, StandardCharsets.UTF_8);
        assertEquals("Unexpected message content :" + content, messageText, content);

        messageBytes = _restTestHelper.getBytes(queueRelativePath + "/getMessageContent?limit=1024&decompressBeforeLimiting=true&messageId=" + id);
        content = new String(messageBytes, StandardCharsets.UTF_8);
        assertEquals("Unexpected message content :" + content, messageText.substring(0, 1024), content);
    }

    public void testGetContentViaRestForCompressedMessageWithAgentSupportingCompression() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        String messageText = createMessageText();
        Connection senderConnection = getConnection(true);
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue testQueue = createTestQueue(session);

        publishMessage(senderConnection, messageText, testQueue);

        String queueRelativePath =  "queue/" + VIRTUAL_HOST  + "/" + VIRTUAL_HOST + "/" + testQueue.getQueueName();

        List<Map<String, Object>> messages = _restTestHelper.getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertEquals("Unexpected number of messages", 1, messages.size());
        long id = ((Number) messages.get(0).get("id")).longValue();

        _restTestHelper.setAcceptEncoding("gzip, deflate, br");
        String content = getDecompressedContent(queueRelativePath + "/getMessageContent?messageId=" + id);
        assertEquals("Unexpected message content :" + content, messageText, content);

        content = getDecompressedContent(queueRelativePath + "/getMessageContent?limit=1024&decompressBeforeLimiting=true&messageId=" + id);
        assertEquals("Unexpected message content :" + content, messageText.substring(0, 1024), content);
    }

    public void testGetTruncatedContentViaRestForCompressedMessage() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        String messageText = createMessageText();
        Connection senderConnection = getConnection(true);
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue testQueue = createTestQueue(session);

        publishMessage(senderConnection, messageText, testQueue);

        String queueRelativePath = "queue/" + VIRTUAL_HOST  + "/" + VIRTUAL_HOST + "/" + testQueue.getQueueName();

        List<Map<String, Object>> messages = _restTestHelper.getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertEquals("Unexpected number of messages", 1, messages.size());
        long id = ((Number) messages.get(0).get("id")).longValue();

        _restTestHelper.setAcceptEncoding("gzip");
        try
        {
            getDecompressedContent(queueRelativePath + "/getMessageContent?limit=1024&messageId=" + id);
            fail("Should not be able to decompress truncated gzip");
        }
        catch (EOFException e)
        {
            // pass
        }
    }

    private String getDecompressedContent(final String url) throws IOException
    {
        HttpURLConnection connection = _restTestHelper.openManagementConnection(url, "GET");
        connection.connect();
        return decompressInputStream(connection);
    }

    public void testGetContentViaRestForCompressedMapMessageWithAgentNotSupportingCompression() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        Connection senderConnection = getConnection(true);
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue testQueue = createTestQueue(session);

        Map<String, Object> mapToSend = createMapToSend();
        publishMapMessage(senderConnection, mapToSend, testQueue);

        String queueRelativePath =  "queue/" + VIRTUAL_HOST  + "/" + VIRTUAL_HOST + "/" + testQueue.getQueueName();

        List<Map<String, Object>> messages = _restTestHelper.getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertEquals("Unexpected number of messages", 1, messages.size());
        long id = ((Number) messages.get(0).get("id")).longValue();

        Map<String, Object> content =
                _restTestHelper.getJsonAsMap(queueRelativePath + "/getMessageContent?returnJson=true&messageId=" + id);
        assertEquals("Unexpected message content: difference " + Maps.difference(mapToSend, content),
                     new HashMap<>(mapToSend),
                     new HashMap<>(content));
    }

    public void testGetContentViaRestForCompressedMapMessageWithAgentSupportingCompression() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        Connection senderConnection = getConnection(true);
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue testQueue = createTestQueue(session);

        Map<String, Object> mapToSend = createMapToSend();
        publishMapMessage(senderConnection, mapToSend, testQueue);


        String queueRelativePath =  "queue/" + VIRTUAL_HOST  + "/" + VIRTUAL_HOST + "/" + testQueue.getQueueName();

        List<Map<String, Object>> messages = _restTestHelper.getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertEquals("Unexpected number of messages", 1, messages.size());
        long id = ((Number) messages.get(0).get("id")).longValue();

        _restTestHelper.setAcceptEncoding("gzip, deflate, br");
        HttpURLConnection connection =
                _restTestHelper.openManagementConnection(queueRelativePath
                                                         + "/getMessageContent?returnJson=true&messageId="
                                                         + id,
                                                         "GET");
        connection.connect();

        String content = decompressInputStream(connection);
        Map<String, Object> mapContent = new ObjectMapper().readValue(content, Map.class);
        assertEquals("Unexpected message content: difference " + Maps.difference(mapToSend, mapContent),
                     new HashMap<>(mapToSend),
                     new HashMap<>(mapContent));
    }

    private Map<String, Object> createMapToSend()
    {
        Map<String, Object> mapToSend = new HashMap<>();

        String message = "This is a sample message";
        int i = 0, l = message.length();
        do
        {
            mapToSend.put("text" + i, message);
            i++;
        }
        while (i * l < 2048 * 1024);

        mapToSend.put("int", 1);
        return mapToSend;
    }

    private String decompressInputStream(final HttpURLConnection connection) throws IOException
    {
        String content;
        try (InputStream is = new GZIPInputStream(connection.getInputStream());
             ByteArrayOutputStream baos = new ByteArrayOutputStream())
        {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = is.read(buffer)) != -1)
            {
                baos.write(buffer, 0, len);
            }
            content = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }
        return content;
    }

    private void publishMapMessage(final Connection senderConnection,
                                   final Map<String, Object> mapData,
                                   final Queue testQueue)
            throws JMSException, org.apache.qpid.QpidException
    {
        Session session = senderConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(testQueue);
        MapMessage sentMessage = session.createMapMessage();
        sentMessage.setStringProperty("bar", "foo");
        for(Map.Entry<String,Object> entry: mapData.entrySet())
        {
            String key =  entry.getKey();
            Object value =  entry.getValue();
            if (value instanceof String)
            {
                sentMessage.setString(key, (String) value);
            }
            else if (value instanceof Integer)
            {
                sentMessage.setInt(key, (Integer) value);
            }
            else
            {
                throw new RuntimeException("Setting value of type " + value.getClass() + " is not implemented yet");
            }
        }

        producer.send(sentMessage);
        session.commit();
    }

    private void publishMessage(final Connection senderConnection, final String messageText, final Queue testQueue)
            throws JMSException, org.apache.qpid.QpidException
    {
        Session session = senderConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(testQueue);
        TextMessage sentMessage = session.createTextMessage(messageText);
        sentMessage.setStringProperty("bar", "foo");

        producer.send(sentMessage);
        session.commit();
    }

    private String createMessageText()
    {
        StringBuilder stringBuilder = new StringBuilder();
        while(stringBuilder.length() < 2048*1024)
        {
            stringBuilder.append("This should compress easily. ");
        }
        return stringBuilder.toString();
    }

    private Connection getConnection(final boolean compress) throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_COMPRESS_MESSAGES,String.valueOf(compress));
        return getConnectionWithOptions(options);
    }

}
