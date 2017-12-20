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
package org.apache.qpid.tests.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.*;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class LargeApplicationHeadersTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void applicationHeadersSentOverManyFrames() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final String subscriberName = "testSubscriber";
            byte[] sessionName = "test".getBytes(UTF_8);

            ConnectionTune tune = interaction.negotiateProtocol()
                                             .consumeResponse()
                                             .consumeResponse(ConnectionStart.class)
                                             .connection()
                                             .startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS)
                                             .startOk()
                                             .consumeResponse()
                                             .getLatestResponse(ConnectionTune.class);

            int headerPropertySize = ((1<<16) - 1);
            Map<String, Object> applicationHeaders = createApplicationHeadersThatExceedSingleFrame(headerPropertySize,
                                                                                                   tune.getMaxFrameSize());

            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setApplicationHeaders(applicationHeaders);

            interaction.connection().tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOk.class);

            interaction.channelId(1)
                       .attachSession(sessionName)
                       .message()
                       .subscribeDestination(subscriberName)
                       .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .subscribeId(0)
                       .subscribe()
                       .message()
                       .flowId(1)
                       .flowDestination(subscriberName)
                       .flowUnit(MessageCreditUnit.MESSAGE)
                       .flowValue(1)
                       .flow()
                       .message()
                       .flowId(2)
                       .flowDestination(subscriberName)
                       .flowUnit(MessageCreditUnit.BYTE)
                       .flowValue(-1)
                       .flow()
                       .message()
                       .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                       .transferHeader(null, messageProperties)
                       .transferId(0)
                       .transfer()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse()
                       .getLatestResponse(SessionCompleted.class);


            MessageTransfer transfer = consumeResponse(interaction,
                                                       MessageTransfer.class,
                                                       SessionCompleted.class,
                                                       SessionCommandPoint.class,
                                                       SessionConfirmed.class);

            assertThat(transfer.getBodySize(), is(0));

            Header header = transfer.getHeader();
            assertThat(header, is(notNullValue()));

            MessageProperties receivedMessageProperties = header.getMessageProperties();
            assertThat(receivedMessageProperties, is(notNullValue()));

            Map<String, Object> receivedApplicationHeaders = receivedMessageProperties.getApplicationHeaders();
            assertThat(receivedApplicationHeaders, is(notNullValue()));
            assertThat(receivedApplicationHeaders, is(equalTo(applicationHeaders)));
        }
    }

    private <T extends Method> T consumeResponse(final Interaction interaction,
                                                 final Class<T> expected,
                                                 final Class<? extends Method>... ignore)
            throws Exception
    {
        List<Class<? extends Method>> possibleResponses = new ArrayList<>(Arrays.asList(ignore));
        possibleResponses.add(expected);

        T completed = null;
        do
        {
            interaction.consumeResponse(possibleResponses.toArray(new Class[possibleResponses.size()]));
            Response<?> response = interaction.getLatestResponse();
            if (expected.isAssignableFrom(response.getBody().getClass()))
            {
                completed = (T) response.getBody();
            }
        }
        while (completed == null);
        return completed;
    }

    private Map<String, Object> createApplicationHeadersThatExceedSingleFrame(final int headerPropertySize, final int maxFrameSize)
    {
        Map<String, Object> applicationHeaders = new HashMap<>();
        int i = 0;
        do
        {
            String propertyName = "string_" + i;
            String propertyValue = generateLongString(headerPropertySize);
            applicationHeaders.put(propertyName, propertyValue);
            ++i;
        }
        while (applicationHeaders.size() * headerPropertySize < 2 * maxFrameSize);
        return applicationHeaders;
    }

    private String generateLongString(final int count)
    {
        String pattern = "abcde";
        String str = String.join("", Collections.nCopies(count / pattern.length(), pattern)) + pattern.substring(0, count % pattern.length());
        assertThat(str.length(), is(equalTo(count)));
        return str;
    }
}
