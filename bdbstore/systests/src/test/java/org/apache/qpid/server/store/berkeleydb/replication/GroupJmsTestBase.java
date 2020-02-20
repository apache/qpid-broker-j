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
package org.apache.qpid.server.store.berkeleydb.replication;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.test.utils.UnitTestBase;

public class GroupJmsTestBase extends UnitTestBase
{
    private static final int FAILOVER_CYCLECOUNT = 40;
    private static final int FAILOVER_CONNECTDELAY = 1000;
    static final int SHORT_FAILOVER_CYCLECOUNT = 2;
    static final int SHORT_FAILOVER_CONNECTDELAY = 200;

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupJmsTestBase.class);
    private static JmsProvider _jmsProvider;
    private static GroupBrokerAdmin _groupBrokerAdmin;
    private static AtomicReference<Class<?>> _testClass = new AtomicReference<>();

    @BeforeClass
    public static void setUpTestBase()
    {
        assumeThat(System.getProperty("virtualhostnode.type", "BDB"), is(equalTo("BDB")));

        _jmsProvider = Utils.getJmsProvider();
    }

    @AfterClass
    public static void tearDownTestBase()
    {
        Class<?> testClass = _testClass.get();
        if (testClass != null && _testClass.compareAndSet(testClass, null))
        {
            _groupBrokerAdmin.afterTestClass(testClass);
        }
    }

    @Rule
    public final ExternalResource resource = new ExternalResource()
    {
        @Override
        protected void before()
        {
            if (_testClass.compareAndSet(null, GroupJmsTestBase.this.getClass() ))
            {
                _groupBrokerAdmin = new GroupBrokerAdmin();
                _groupBrokerAdmin.beforeTestClass(GroupJmsTestBase.this.getClass());
            }
        }

        @Override
        protected void after()
        {

        }
    };

    @Before
    public void beforeTestMethod() throws Exception
    {
        _groupBrokerAdmin.beforeTestMethod(getClass(), getClass().getMethod(getTestName()));
    }

    @After
    public void afterTestMethod() throws Exception
    {
        _groupBrokerAdmin.afterTestMethod(getClass(), getClass().getMethod(getTestName()));
    }

    GroupBrokerAdmin getBrokerAdmin()
    {
        return _groupBrokerAdmin;
    }

    ConnectionBuilder getConnectionBuilder()
    {
        final ConnectionBuilder connectionBuilder = _jmsProvider.getConnectionBuilder()
                                                                    .setClientId(getTestName())
                                                                    .setFailoverReconnectDelay(FAILOVER_CONNECTDELAY)
                                                                    .setFailoverReconnectAttempts(FAILOVER_CYCLECOUNT)
                                                                    .setVirtualHost("test")
                                                                    .setFailover(true)
                                                                    .setHost(getBrokerAdmin().getHost());
        int[] ports = getBrokerAdmin().getGroupAmqpPorts();
        for (int i = 0; i < ports.length; i++)
        {
            int port = ports[i];
            if (i == 0)
            {
                connectionBuilder.setPort(port);
            }
            else
            {
                connectionBuilder.addFailoverPort(port);
            }
        }
        return connectionBuilder;
    }

    void assertProduceConsume(final Queue queue) throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));
        }
        finally
        {
            connection.close();
        }
    }

    JmsProvider getJmsProvider()
    {
        return _jmsProvider;
    }


    Queue createTestQueue(final Connection connection) throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return getJmsProvider().createQueue(session, getTestName());
        }
        finally
        {
            session.close();
        }
    }

}
