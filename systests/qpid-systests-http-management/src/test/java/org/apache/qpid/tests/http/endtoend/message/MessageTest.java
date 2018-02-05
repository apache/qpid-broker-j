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

package org.apache.qpid.tests.http.endtoend.message;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.ByteStreams;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class MessageTest extends HttpTestBase
{

    private static final String QUEUE_NAME = "myqueue";
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE_REF =
            new TypeReference<List<Map<String, Object>>>() {};
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() {};
    private static final TypeReference<List<Object>> LIST_TYPE_REF =
            new TypeReference<List<Object>>() {};

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
    }

    @Test
    public void getJmsMessage() throws Exception
    {
        getHelper().setTls(true);

        final String messageProperty = "myProp";
        final String messagePropertyValue = "myValue";

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            Message message = session.createMessage();
            message.setStringProperty(messageProperty, messagePropertyValue);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders", Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));

        Map<String, Object> message = messages.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) message.get("headers");
        assertThat(headers.get(messageProperty), is(equalTo(messagePropertyValue)));
    }

    @Test
    public void getJmsMapMessage() throws Exception
    {
        getHelper().setTls(true);
        final String mapKey = "key";
        final String mapKeyValue = "value";

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            MapMessage message = session.createMapMessage();
            message.setString(mapKey, mapKeyValue);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders", Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        Map<String, Object> contentParams = new HashMap<>();
        contentParams.put("messageId", messageId);
        contentParams.put("returnJson", Boolean.TRUE);

        Map<String, Object> content = getHelper().postJson("queue/myqueue/getMessageContent",
                                                                  contentParams,
                                                                  MAP_TYPE_REF, SC_OK);
        assertThat(content.size(), is(equalTo(1)));
        assertThat(content.get(mapKey), is(equalTo(mapKeyValue)));
    }

    @Test
    public void getJmsStreamMessage() throws Exception
    {
        getHelper().setTls(true);

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            StreamMessage message = session.createStreamMessage();
            message.writeLong(Long.MAX_VALUE);
            message.writeBoolean(true);
            message.writeString("Hello World");
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders", Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        Map<String, Object> contentParams = new HashMap<>();
        contentParams.put("messageId", messageId);
        contentParams.put("returnJson", Boolean.TRUE);

        List<Object> content = getHelper().postJson("queue/myqueue/getMessageContent",
                                                                  contentParams,
                                                                  LIST_TYPE_REF, SC_OK);
        assertThat(content.size(), is(equalTo(3)));
        assertThat(content.get(0), is(equalTo(Long.MAX_VALUE)));
        assertThat(content.get(1), is(equalTo(Boolean.TRUE)));
        assertThat(content.get(2), is(equalTo("Hello World")));
    }

    @Test
    public void getJmsBytesMessage() throws Exception
    {
        getHelper().setTls(true);

        final byte[] content = new byte[512];
        IntStream.range(0, content.length).forEachOrdered(i -> content[i] = (byte) (i % 256));

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(content);
            producer.send(message);
        }
        finally
        {
            connection.close();
        }

        List<Map<String, Object>> messages = getHelper().postJson("queue/myqueue/getMessageInfo",
                                                                  Collections.singletonMap("includeHeaders", Boolean.TRUE),
                                                                  LIST_MAP_TYPE_REF, SC_OK);
        assertThat(messages.size(), is(equalTo(1)));
        Map<String, Object> message = messages.get(0);
        int messageId = (int) message.get("id");

        HttpURLConnection httpCon = getHelper().openManagementConnection(String.format(
                "queue/myqueue/getMessageContent?messageId=%d", messageId), "GET");
        httpCon.connect();

        byte[] receivedContent;
        try(InputStream is = httpCon.getInputStream())
        {
            receivedContent = ByteStreams.toByteArray(is);
        }

        assumeThat("AMQP1.0 messages return the AMQP type",
                   getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        assertThat(receivedContent, is(equalTo(content)));
    }


}
