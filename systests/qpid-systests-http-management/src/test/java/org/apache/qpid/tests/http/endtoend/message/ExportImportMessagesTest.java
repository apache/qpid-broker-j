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

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.server.util.DataUrlUtils;

@HttpRequestConfig
public class ExportImportMessagesTest extends HttpTestBase
{
    private static final String TEST_QUEUE = "testQueue";

    @Before
    public void setUp() throws Exception
    {
        getHelper().setTls(true);
    }

    @Test
    public void exportImport() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        getBrokerAdmin().createQueue(TEST_QUEUE);

        final TextMessage sentMessage = putMessageOnQueue();

        changeVirtualHostState("STOPPED");

        byte[] extractedBytes = getHelper().getBytes("virtualhost/exportMessageStore");
        String extractedBytesAsDataUrl = DataUrlUtils.getDataUrlForBytes(extractedBytes);

        changeVirtualHostState("ACTIVE");

        getBrokerAdmin().deleteQueue(TEST_QUEUE);
        getBrokerAdmin().createQueue(TEST_QUEUE);

        changeVirtualHostState("STOPPED");

        Map<String, Object> importArgs = Collections.singletonMap("source", extractedBytesAsDataUrl);
        getHelper().postJson("virtualhost/importMessageStore", importArgs, new TypeReference<Void>() {}, SC_OK);

        changeVirtualHostState("ACTIVE");
        verifyMessagesOnQueue(sentMessage);
    }

    private void changeVirtualHostState(final String desiredState) throws Exception
    {
        Map<String, Object> attributes = Collections.singletonMap(VirtualHost.DESIRED_STATE, desiredState);
        getHelper().submitRequest("virtualhost", "POST", attributes, SC_OK);
    }

    private TextMessage putMessageOnQueue() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            List<Message> _expectedMessages = Utils.sendMessages(session, session.createQueue(TEST_QUEUE), 1);
            return (TextMessage) _expectedMessages.get(0);
        }
        finally
        {
            connection.close();
        }
    }

    private void verifyMessagesOnQueue(final TextMessage expectedMessage) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(session.createQueue(TEST_QUEUE));

            final TextMessage receivedMessage = (TextMessage) consumer.receive(getReceiveTimeout());
            assertThat(receivedMessage, is(notNullValue()));
            assertThat(receivedMessage.getJMSMessageID(), is(equalTo(expectedMessage.getJMSMessageID())));
            assertThat(receivedMessage.getText(), is(equalTo(expectedMessage.getText())));
        }
        finally
        {
            connection.close();
        }
    }
}
