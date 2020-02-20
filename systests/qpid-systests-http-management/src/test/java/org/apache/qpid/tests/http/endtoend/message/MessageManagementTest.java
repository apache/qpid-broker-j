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

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.queue.PriorityQueue;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class MessageManagementTest extends HttpTestBase
{
    private static final String SOURCE_QUEUE_NAME = "sourceQueue";
    private static final String DESTINATION_QUEUE_NAME = "destinationQueue";
    private static final String INDEX = "index";
    private Set<String> _messageIds;

    @Before
    public void setUp() throws Exception
    {
        getBrokerAdmin().createQueue(SOURCE_QUEUE_NAME);
        getBrokerAdmin().createQueue(DESTINATION_QUEUE_NAME);

        getHelper().setTls(true);

        final Map<String, Object> odd = Collections.singletonMap(INDEX, 1);
        final Map<String, Object> even = Collections.singletonMap(INDEX, 0);

        _messageIds = Stream.generate(UUID::randomUUID).map(UUID::toString).limit(4).collect(Collectors.toSet());

        int i = 0;
        for (final String uuid : _messageIds)
        {
            publishMessage(SOURCE_QUEUE_NAME, uuid, i % 2 == 0 ? even : odd);
            i++;
        }
    }

    @Test
    public void moveMessagesByInternalIdRange() throws Exception
    {
        final Set<Long> ids = new HashSet<>(getMesssageIds(SOURCE_QUEUE_NAME));

        Iterator<Long> iterator = ids.iterator();
        Set<Long> toMove = new HashSet<>();

        toMove.add(iterator.next());
        iterator.remove();

        toMove.add(iterator.next());
        iterator.remove();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("messageIds", toMove);
        parameters.put("destination", DESTINATION_QUEUE_NAME);

        getHelper().submitRequest(String.format("queue/%s/moveMessages", SOURCE_QUEUE_NAME),
                                  "POST",
                                  parameters,
                                  HttpServletResponse.SC_OK);

        Set<Long> destQueueContents = getMesssageIds(DESTINATION_QUEUE_NAME);
        assertThat("Unexpected dest queue contents after move", destQueueContents, is(equalTo(toMove)));

        Set<Long> sourceQueueContents = getMesssageIds(SOURCE_QUEUE_NAME);
        assertThat("Unexpected source queue contents after move", sourceQueueContents, is(equalTo(ids)));
    }

    @Test
    public void moveMessagesWithSelector() throws Exception
    {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("selector", "index % 2 = 0");
        parameters.put("destination", DESTINATION_QUEUE_NAME);

        getHelper().submitRequest(String.format("queue/%s/moveMessages", SOURCE_QUEUE_NAME),
                                  "POST",
                                  parameters,
                                  HttpServletResponse.SC_OK);

        List<Map<String, Object>> destQueueMessages = getMessageDetails(DESTINATION_QUEUE_NAME);

        for (Map<String, Object> message : destQueueMessages)
        {
            assertThat(message, is(notNullValue()));
            @SuppressWarnings("unchecked") final Map<String, Object> headers =
                    (Map<String, Object>) message.get("headers");
            assertThat(headers, hasEntry(INDEX, 0));
        }

        List<Map<String, Object>> sourceQueueMessages = getMessageDetails(SOURCE_QUEUE_NAME);

        for (Map<String, Object> message : sourceQueueMessages)
        {
            assertThat(message, is(notNullValue()));
            @SuppressWarnings("unchecked")
            final Map<String, Object> headers = (Map<String, Object>) message.get("headers");
            assertThat(headers, hasEntry(INDEX, 1));
        }
    }

    @Test
    public void copyAllMessages() throws Exception
    {
        final int sourceQueueDepthMessagesBefore = getBrokerAdmin().getQueueDepthMessages(SOURCE_QUEUE_NAME);
        assertThat(sourceQueueDepthMessagesBefore, is(equalTo(_messageIds.size())));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("destination", DESTINATION_QUEUE_NAME);

        getHelper().submitRequest(String.format("queue/%s/copyMessages", SOURCE_QUEUE_NAME),
                                  "POST",
                                  parameters,
                                  HttpServletResponse.SC_OK);

        final int sourceQueueDepthMessagesAfter = getBrokerAdmin().getQueueDepthMessages(SOURCE_QUEUE_NAME);
        final int destQueueDepthMessagesAfter = getBrokerAdmin().getQueueDepthMessages(DESTINATION_QUEUE_NAME);
        assertThat(sourceQueueDepthMessagesAfter, is(equalTo(sourceQueueDepthMessagesBefore)));
        assertThat(destQueueDepthMessagesAfter, is(equalTo(sourceQueueDepthMessagesBefore)));
    }

    @Test
    public void deleteMessagesByInternalId() throws Exception
    {
        final Set<Long> ids = new HashSet<>(getMesssageIds(SOURCE_QUEUE_NAME));
        Iterator<Long> iterator = ids.iterator();
        Set<Long> toDelete = Collections.singleton(iterator.next());
        iterator.remove();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("messageIds", toDelete);
        getHelper().submitRequest(String.format("queue/%s/deleteMessages", SOURCE_QUEUE_NAME),
                                  "POST",
                                  parameters,
                                  HttpServletResponse.SC_OK);

        Set<Long> remainIds = getMesssageIds(SOURCE_QUEUE_NAME);
        assertThat("Unexpected queue contents after delete", remainIds, is(equalTo(ids)));
    }

    @Test
    public void testDeleteMessagesWithLimit() throws Exception
    {
        final int totalMessage = _messageIds.size();
        final int numberToMove = totalMessage / 2;
        final int remainingMessages = totalMessage - numberToMove;

        // delete messages
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("limit", numberToMove);

        getHelper().submitRequest(String.format("queue/%s/deleteMessages", SOURCE_QUEUE_NAME),
                                  "POST",
                                  parameters,
                                  HttpServletResponse.SC_OK);

        assertThat(getBrokerAdmin().getQueueDepthMessages(SOURCE_QUEUE_NAME), is(equalTo(remainingMessages)));
    }

    @Test
    public void testClearQueue() throws Exception
    {
        getHelper().submitRequest(String.format("queue/%s/clearQueue", SOURCE_QUEUE_NAME), "POST",
                                  Collections.emptyMap(), HttpServletResponse.SC_OK);

        assertThat(getBrokerAdmin().getQueueDepthMessages(SOURCE_QUEUE_NAME), is(equalTo(0)));
    }

    @Test
    public void testReenqueueMessageForPriorityChange() throws Exception
    {
        final String queueName = "priorityQueue";
        createPriorityQueue(queueName, 10);
        publishPriorityMessage(queueName, "1", 5);
        publishPriorityMessage(queueName, "2", 6);
        publishPriorityMessage(queueName, "3", 1);

        final List<Map<String, Object>> messages =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo", queueName));

        assertThat(messages.size(), is(equalTo(3)));
        final Map<String, Object> message1 = messages.get(0);
        final Map<String, Object> message2 = messages.get(1);
        final Map<String, Object> message3 = messages.get(2);
        assertThat(message1.get("messageId"), is(equalTo("2")));
        assertThat(message2.get("messageId"), is(equalTo("1")));
        assertThat(message3.get("messageId"), is(equalTo("3")));

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("messageId", message3.get("id"));
        parameters.put("newPriority", 10);
        Long result = getHelper().postJson(String.format("queue/%s/reenqueueMessageForPriorityChange", queueName),
                                           parameters,
                                           new TypeReference<Long>()
                                           {
                                           },
                                           HttpServletResponse.SC_OK);

        assertThat(result, is(not(equalTo(-1L))));

        final List<Map<String, Object>> messages2 =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo", queueName));

        assertThat(messages.size(), is(equalTo(3)));
        final Map<String, Object> message1AfterChange = messages2.get(0);
        final Map<String, Object> message2AfterChange = messages2.get(1);
        final Map<String, Object> message3AfterChange = messages2.get(2);
        assertThat(message1AfterChange.get("messageId"), is(equalTo("3")));
        assertThat(message2AfterChange.get("messageId"), is(equalTo("2")));
        assertThat(message3AfterChange.get("messageId"), is(equalTo("1")));
        assertThat(message1AfterChange.get("priority"), is(equalTo(10)));
    }

    @Test
    public void testReenqueueMessagesForPriorityChange() throws Exception
    {
        final String queueName = "priorityQueue";
        createPriorityQueue(queueName, 10);
        publishPriorityMessage(queueName, "1", 5);
        publishPriorityMessage(queueName, "2", 6);
        publishPriorityMessage(queueName, "3", 1);

        final List<Map<String, Object>> messages =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo", queueName));

        assertThat(messages.size(), is(equalTo(3)));
        final Map<String, Object> message1 = messages.get(0);
        final Map<String, Object> message2 = messages.get(1);
        final Map<String, Object> message3 = messages.get(2);
        assertThat(message1.get("messageId"), is(equalTo("2")));
        assertThat(message2.get("messageId"), is(equalTo("1")));
        assertThat(message3.get("messageId"), is(equalTo("3")));

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("selector", String.format("id in ('%s', '%s')",
                                                 message3.get("messageId"),
                                                 message2.get("messageId")));
        parameters.put("newPriority", 10);
        final List<Long> result =
                getHelper().postJson(String.format("queue/%s/reenqueueMessagesForPriorityChange", queueName),
                                     parameters,
                                     new TypeReference<List<Long>>()
                                     {
                                     },
                                     HttpServletResponse.SC_OK);

        assertThat(result.size(), is(equalTo(2)));

        final List<Map<String, Object>> messages2 =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo", queueName));

        assertThat(messages.size(), is(equalTo(3)));
        final Map<String, Object> message1AfterChange = messages2.get(0);
        final Map<String, Object> message2AfterChange = messages2.get(1);
        final Map<String, Object> message3AfterChange = messages2.get(2);
        assertThat(message1AfterChange.get("messageId"), is(equalTo("1")));
        assertThat(message2AfterChange.get("messageId"), is(equalTo("3")));
        assertThat(message3AfterChange.get("messageId"), is(equalTo("2")));
        assertThat(message1AfterChange.get("priority"), is(equalTo(10)));
        assertThat(message2AfterChange.get("priority"), is(equalTo(10)));
    }

    private List<Map<String, Object>> getMessageDetails(final String queueName) throws IOException
    {
        List<Map<String, Object>> destQueueMessages =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo?includeHeaders=true",
                                                        queueName));
        assertThat(destQueueMessages, is(notNullValue()));
        return destQueueMessages;
    }

    private Set<Long> getMesssageIds(final String queueName) throws IOException
    {
        List<Map<String, Object>> messages =
                getHelper().getJsonAsList(String.format("queue/%s/getMessageInfo", queueName));
        Set<Long> ids = new HashSet<>();
        for (Map<String, Object> message : messages)
        {
            ids.add(((Number) message.get("id")).longValue());
        }
        return ids;
    }

    private void publishMessage(final String queueName, final String messageId, final Map<String, Object> headers) throws Exception
    {
        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", queueName);
        messageBody.put("messageId", messageId);
        messageBody.put("headers", headers);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);
    }

    private void publishPriorityMessage(final String queueName, final String messageId, int priority) throws Exception
    {
        final Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("address", queueName);
        messageBody.put("messageId", messageId);
        messageBody.put("headers", Collections.singletonMap("id", messageId));
        messageBody.put("priority", priority);

        getHelper().submitRequest("virtualhost/publishMessage",
                                  "POST",
                                  Collections.singletonMap("message", messageBody),
                                  SC_OK);
    }

    private void createPriorityQueue(final String queueName, int priorities) throws IOException
    {
        final Map<String, Object> data = new HashMap<>();
        data.put(ConfiguredObject.TYPE, "priority");
        data.put(PriorityQueue.PRIORITIES, priorities);
        getHelper().submitRequest(String.format("queue/%s", queueName), "PUT", data, SC_CREATED);
    }
}
