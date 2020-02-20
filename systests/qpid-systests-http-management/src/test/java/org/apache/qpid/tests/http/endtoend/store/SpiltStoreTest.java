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
package org.apache.qpid.tests.http.endtoend.store;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.virtualhost.ProvidedStoreVirtualHost;
import org.apache.qpid.server.virtualhost.ProvidedStoreVirtualHostImpl;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig(useVirtualHostAsHost = false)
public class SpiltStoreTest extends HttpTestBase
{
    private static final String TEST_QUEUE = "testQueue";

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));
    }

    @Test
    public void splitStore() throws Exception
    {
        final String url = String.format("virtualhost/%s/%s", getVirtualHostNode(), getVirtualHost());
        getHelper().submitRequest(url, "DELETE", SC_OK);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHost.TYPE, System.getProperty("virtualhostnode.type"));
        getHelper().submitRequest(url, "PUT", attributes, SC_CREATED);

        final Map<String, Object> virtualHost = getHelper().getJson(url,
                                                                    new TypeReference<Map<String, Object>>() {},
                                                                    SC_OK);
        final String storePath = (String) virtualHost.get("storePath");
        assertThat(new File(storePath).exists(), is(equalTo(true)));

        getBrokerAdmin().createQueue(TEST_QUEUE);
        final TextMessage sentMessage = putMessageOnQueue();

        getBrokerAdmin().restart();

        verifyMessagesOnQueue(sentMessage);

        getHelper().submitRequest(url, "DELETE", SC_OK);
        assertThat(new File(storePath).exists(), is(equalTo(false)));

        attributes.put(VirtualHost.TYPE, ProvidedStoreVirtualHostImpl.VIRTUAL_HOST_TYPE);
        getHelper().submitRequest(url, "PUT", attributes, SC_CREATED);

        getBrokerAdmin().createQueue(TEST_QUEUE);
        verifyMessagesOnQueue( putMessageOnQueue());
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

    private void changeState(final String url, final String desiredState) throws Exception
    {
        Map<String, Object> attributes = Collections.singletonMap(ConfiguredObject.DESIRED_STATE, desiredState);
        getHelper().submitRequest(url, "POST", attributes, SC_OK);
    }

    private void assertState(final String url, final String expectedActualState) throws Exception
    {
        Map<String, Object> object = getHelper().getJson(url, new TypeReference<Map<String, Object>>() {}, SC_OK);
        final String actualState = (String) object.get(ConfiguredObject.STATE);
        assertThat(actualState, is(equalTo(expectedActualState)));
    }

}
