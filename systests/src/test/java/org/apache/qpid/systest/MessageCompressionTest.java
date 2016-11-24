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
package org.apache.qpid.systest;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.url.URLSyntaxException;

public class MessageCompressionTest extends QpidBrokerTestCase
{
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
                                  HttpPort.ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS,
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

    public void testSenderCompressesReceiverUncompresses() throws Exception
    {
        doTestCompression(true, true, true);
    }

    public void testSenderCompressesOnly() throws Exception
    {
        doTestCompression(true, false, true);

    }

    public void testReceiverUncompressesOnly() throws Exception
    {
        doTestCompression(false, true, true);

    }

    public void testNoCompression() throws Exception
    {
        doTestCompression(false, false, true);

    }


    public void testDisablingCompressionAtBroker() throws Exception
    {
        doTestCompression(true, true, false);
    }


    private void doTestCompression(final boolean senderCompresses,
                                   final boolean receiverUncompresses,
                                   final boolean brokerCompressionEnabled) throws Exception
    {

        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(brokerCompressionEnabled));

        doActualSetUp();

        String messageText = createMessageText();
        Connection senderConnection = getConnection(senderCompresses);
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        publishMessage(senderConnection, messageText);

        // get the number of bytes received at the broker on the connection
        List<Map<String, Object>> connectionRestOutput = _restTestHelper.getJsonAsList("/api/latest/connection");
        assertEquals(1, connectionRestOutput.size());
        Map statistics = (Map) connectionRestOutput.get(0).get("statistics");
        int bytesIn = (Integer) statistics.get("bytesIn");

        // if sending compressed then the bytesIn statistic for the connection should reflect the compressed size of the
        // message
        if(senderCompresses && brokerCompressionEnabled)
        {
            assertTrue("Message was not sent compressed", bytesIn < messageText.length());
        }
        else
        {
            assertFalse("Message was incorrectly sent compressed", bytesIn < messageText.length());
        }
        senderConnection.close();

        // receive the message
        Connection consumerConnection = getConnection(receiverUncompresses);
        Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(getTestQueue());
        consumerConnection.start();

        TextMessage message = (TextMessage) consumer.receive(500l);
        assertNotNull("Message was not received", message);
        assertEquals("Message was corrupted", messageText, message.getText());
        assertEquals("Header was corrupted", "foo", message.getStringProperty("bar"));

        // get the number of bytes sent by the broker
        connectionRestOutput = _restTestHelper.getJsonAsList("/api/latest/connection");
        assertEquals(1, connectionRestOutput.size());
        statistics = (Map) connectionRestOutput.get(0).get("statistics");
        int bytesOut = (Integer) statistics.get("bytesOut");

        // if receiving compressed the bytes out statistic from the connection should reflect the compressed size of the
        // message
        if(receiverUncompresses && brokerCompressionEnabled)
        {
            assertTrue("Message was not received compressed", bytesOut < messageText.length());
        }
        else
        {
            assertFalse("Message was incorrectly received compressed", bytesOut < messageText.length());
        }

        consumerConnection.close();
    }

    public void testGetContentViaRestForCompressedMessageWithAgentNotSupportingCompression() throws Exception
    {
        setTestSystemProperty(Broker.BROKER_MESSAGE_COMPRESSION_ENABLED, String.valueOf(true));

        doActualSetUp();

        String messageText = createMessageText();
        Connection senderConnection = getConnection(true);
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        publishMessage(senderConnection, messageText);

        String queueRelativePath = "queue" + virtualPath + virtualPath + "/" + testQueueName;

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
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        publishMessage(senderConnection, messageText);

        String queueRelativePath = "queue" + virtualPath + virtualPath + "/" + testQueueName;

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
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        publishMessage(senderConnection, messageText);

        String queueRelativePath = "queue" + virtualPath + virtualPath + "/" + testQueueName;

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
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        Map<String, Object> mapToSend = createMapToSend();
        publishMapMessage(senderConnection, mapToSend);

        String queueRelativePath = "queue" + virtualPath + virtualPath + "/" + testQueueName;

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
        String virtualPath = ((AMQConnectionFactory)getConnectionFactory()).getVirtualPath();
        String testQueueName = getTestQueueName();
        createAndBindQueue(virtualPath, testQueueName);

        Map<String, Object> mapToSend = createMapToSend();
        publishMapMessage(senderConnection, mapToSend);

        String queueRelativePath = "queue" + virtualPath + virtualPath + "/" + testQueueName;

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

    private void publishMapMessage(final Connection senderConnection, final Map<String, Object> mapData)
            throws JMSException, org.apache.qpid.QpidException
    {
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(getTestQueue());
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
        ((AMQSession) session).sync();
    }

    private void publishMessage(final Connection senderConnection, final String messageText)
            throws JMSException, org.apache.qpid.QpidException
    {
        Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(getTestQueue());
        TextMessage sentMessage = session.createTextMessage(messageText);
        sentMessage.setStringProperty("bar", "foo");

        producer.send(sentMessage);
        ((AMQSession) session).sync();
    }

    private void createAndBindQueue(final String virtualPath, final String testQueueName) throws IOException
    {
        // create the queue using REST and bind it
        assertEquals(201,
                     _restTestHelper.submitRequest("/api/latest/queue"
                                                   + virtualPath
                                                   + virtualPath
                                                   + "/"
                                                   + testQueueName, "PUT", Collections.<String, Object>emptyMap()));
        assertEquals(201,
                     _restTestHelper.submitRequest("/api/latest/binding"
                                                   + virtualPath
                                                   + virtualPath
                                                   + "/amq.direct/"
                                                   + testQueueName
                                                   + "/"
                                                   + testQueueName, "PUT", Collections.<String, Object>emptyMap()));
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

    private Connection getConnection(final boolean compress) throws URLSyntaxException, NamingException, JMSException
    {
        AMQConnectionURL url = new AMQConnectionURL(((AMQConnectionFactory)getConnectionFactory()).getConnectionURLString());

        url.setOption(ConnectionURL.OPTIONS_COMPRESS_MESSAGES,String.valueOf(compress));
        url = new AMQConnectionURL(url.toString());
        url.setUsername(GUEST_USERNAME);
        url.setPassword(GUEST_PASSWORD);
        url.setOption(ConnectionURL.OPTIONS_COMPRESS_MESSAGES,String.valueOf(compress));
        return getConnection(url);
    }

}
