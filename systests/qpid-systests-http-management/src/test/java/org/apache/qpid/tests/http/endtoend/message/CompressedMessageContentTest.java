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
package org.apache.qpid.tests.http.endtoend.message;

import static org.apache.qpid.server.model.Broker.BROKER_MESSAGE_COMPRESSION_ENABLED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@HttpRequestConfig
@ConfigItem(name = BROKER_MESSAGE_COMPRESSION_ENABLED, value = "true")
public class CompressedMessageContentTest extends HttpTestBase
{
    private static final String TEST_QUEUE = "testQueue";

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
        getHelper().setTls(true);
        getBrokerAdmin().createQueue(TEST_QUEUE);
    }

    @Test
    public void getCompressedMessageContent_noCompressionSupported() throws Exception
    {
        final String messageText = sendCompressibleTextMessage();

        String queueRelativePath = String.format("queue/%s", TEST_QUEUE);

        List<Map<String, Object>> messages = getHelper().getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertThat(messages.size(), is(equalTo(1)));
        final Map<String, Object> message = messages.get(0);
        assertThat(message.get("encoding"), is(equalTo("gzip")));
        long id = ((Number) message.get("id")).longValue();

        byte[] messageBytes = getHelper().getBytes(queueRelativePath + "/getMessageContent?messageId=" + id);
        String content = new String(messageBytes, StandardCharsets.UTF_8);
        assertThat("Unexpected message content", content, is(equalTo(messageText)));

        messageBytes = getHelper().getBytes(queueRelativePath + "/getMessageContent?limit=1024&decompressBeforeLimiting=true&messageId=" + id);
        content = new String(messageBytes, StandardCharsets.UTF_8);
        assertThat("Unexpected limited message content", content, is(equalTo(messageText.substring(0, 1024))));
    }

    @Test
    public void getCompressedMessageContent_compressionSupported() throws Exception
    {
        final String messageText = sendCompressibleTextMessage();

        String queueRelativePath = String.format("queue/%s", TEST_QUEUE);

        List<Map<String, Object>> messages = getHelper().getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertThat(messages.size(), is(equalTo(1)));

        final Map<String, Object> message = messages.get(0);
        assertThat(message.get("encoding"), is(equalTo("gzip")));
        long id = ((Number) message.get("id")).longValue();

        getHelper().setAcceptEncoding("gzip, deflate, br");
        String content = getDecompressedContent(queueRelativePath + "/getMessageContent?messageId=" + id);
        assertThat("Unexpected message content", content, is(equalTo(messageText)));

        content = getDecompressedContent(queueRelativePath + "/getMessageContent?limit=1024&decompressBeforeLimiting=true&messageId=" + id);
        assertThat("Unexpected limited message content", content, is(equalTo(messageText.substring(0, 1024))));
    }

    @Test
    public void getCompressedMapMessage_noCompressionSupported() throws Exception
    {
        final Map<String, Object> mapToSend = sendCompressibleMapMessage();

        String queueRelativePath = String.format("queue/%s", TEST_QUEUE);

        List<Map<String, Object>> messages = getHelper().getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertThat(messages.size(), is(equalTo(1)));

        final Map<String, Object> message = messages.get(0);
        assertThat(message.get("encoding"), is(equalTo("gzip")));
        long id = ((Number) message.get("id")).longValue();


        Map<String, Object> content = getHelper().getJsonAsMap(queueRelativePath + "/getMessageContent?returnJson=true&messageId=" + id);
        assertThat("Unexpected message content: difference ", new HashMap<>(content), is(equalTo(new HashMap<>(mapToSend))));
    }

    @Test
    public void getCompressedMapMessage_compressionSupported() throws Exception
    {
        final Map<String, Object> mapToSend = sendCompressibleMapMessage();

        String queueRelativePath = String.format("queue/%s", TEST_QUEUE);

        List<Map<String, Object>> messages = getHelper().getJsonAsList(queueRelativePath + "/getMessageInfo");
        assertThat(messages.size(), is(equalTo(1)));

        final Map<String, Object> message = messages.get(0);
        assertThat(message.get("encoding"), is(equalTo("gzip")));
        long id = ((Number) message.get("id")).longValue();

        getHelper().setAcceptEncoding("gzip, deflate, br");
        HttpURLConnection connection =
                getHelper().openManagementConnection(queueRelativePath
                                                     + "/getMessageContent?returnJson=true&messageId="
                                                     + id,
                                                     "GET");
        connection.connect();

        String content = decompressInputStream(connection);
        Map<String, Object> mapContent = new ObjectMapper().readValue(content, new TypeReference<Map<String, Object>>() {});

        assertThat("Unexpected message content ", new HashMap<>(mapContent), is(equalTo(new HashMap<>(mapToSend))));
    }

    private String getDecompressedContent(final String url) throws IOException
    {
        HttpURLConnection connection = getHelper().openManagementConnection(url, "GET");
        connection.connect();
        return decompressInputStream(connection);
    }

    private String decompressInputStream(final HttpURLConnection connection) throws IOException
    {
        try (InputStream is = new GZIPInputStream(connection.getInputStream());
             ByteArrayOutputStream baos = new ByteArrayOutputStream())
        {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = is.read(buffer)) != -1)
            {
                baos.write(buffer, 0, len);
            }
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private String sendCompressibleTextMessage() throws Exception
    {
        final String messageText = createCompressibleMessageText();
        Connection connection = getConnection(true);
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(session.createQueue(TEST_QUEUE));
            TextMessage sentMessage = session.createTextMessage(messageText);
            producer.send(sentMessage);
        }
        finally
        {
            connection.close();
        }
        return messageText;
    }

    private Map<String, Object> sendCompressibleMapMessage() throws Exception
    {
        final Map<String, Object> mapToSend = createCompressibleMapMessage();

        Connection senderConnection = getConnection(true);
        try
        {
            Session session = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(session.createQueue(TEST_QUEUE));
            MapMessage sentMessage = session.createMapMessage();
            for(Map.Entry<String,Object> entry: mapToSend.entrySet())
            {
                String key =  entry.getKey();
                Object value =  entry.getValue();
                sentMessage.setObject(key, value);
            }

            producer.send(sentMessage);
        }
        finally
        {
            senderConnection.close();
        }
        return mapToSend;
    }

    private Map<String, Object> createCompressibleMapMessage()
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

    private String createCompressibleMessageText()
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
       return getConnectionBuilder().setCompress(compress).build();
    }

}
