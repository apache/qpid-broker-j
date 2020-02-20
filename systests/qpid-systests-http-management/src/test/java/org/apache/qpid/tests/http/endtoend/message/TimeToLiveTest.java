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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class TimeToLiveTest extends HttpTestBase
{
    private static final String QUEUE_NAME = "testQueue";
    private static final long HOUSE_KEEPING_CHECK_PERIOD = 100;
    private static final long ONE_DAY_MILLISECONDS = 24 * 60 * 60 * 1000L;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        System.setProperty("virtualhost.housekeepingCheckPeriod", String.valueOf(HOUSE_KEEPING_CHECK_PERIOD));
    }

    @AfterClass
    public static void tearDownClass()
    {
        System.clearProperty("virtualhost.housekeepingCheckPeriod");
    }

    @Test
    public void queueTimeToLiveUpdateIsAppliedToEnqueuedMessages() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            producer.send(session.createTextMessage("A"), DeliveryMode.NON_PERSISTENT, 4, ONE_DAY_MILLISECONDS);
        }
        finally
        {
            connection.close();
        }

        getHelper().submitRequest(String.format("queue/%s", QUEUE_NAME),
                                  "POST",
                                  Collections.singletonMap("maximumMessageTtl", 1),
                                  SC_OK);

        Thread.sleep(HOUSE_KEEPING_CHECK_PERIOD * 2);

        getHelper().submitRequest(String.format("queue/%s", QUEUE_NAME),
                                  "POST",
                                  Collections.singletonMap("maximumMessageTtl", 0),
                                  SC_OK);

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            producer.send(session.createTextMessage("B") , DeliveryMode.NON_PERSISTENT, 4, 0);
            Message message = consumer.receive(getReceiveTimeout());
            assertThat(message, is(notNullValue()));
            assertThat(message, is(instanceOf(TextMessage.class)));
            assertThat(((TextMessage)message).getText(), is(equalTo("B")));
        }
        finally
        {
            connection2.close();
        }
    }
}
