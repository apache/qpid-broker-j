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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.Utils;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class GroupJmsTestBase
{
    private static final int FAILOVER_CYCLECOUNT = 40;
    private static final int FAILOVER_CONNECTDELAY = 1000;
    static final int SHORT_FAILOVER_CYCLECOUNT = 2;
    static final int SHORT_FAILOVER_CONNECTDELAY = 200;
    private static final AtomicReference<Class<?>> _testClass = new AtomicReference<>();

    private static JmsProvider _jmsProvider;
    private static GroupBrokerAdmin _groupBrokerAdmin;

    private String _testName;

    @BeforeAll
    public static void setUpTestBase()
    {
        assumeTrue("BDB".equals(System.getProperty("virtualhostnode.type", "BDB")),
                "VirtualHostNodeStoreType should be BDB");

        _jmsProvider = Utils.getJmsProvider();
    }

    @AfterAll
    public static void tearDownTestBase()
    {
        Class<?> testClass = _testClass.get();
        if (testClass != null && _testClass.compareAndSet(testClass, null))
        {
            _groupBrokerAdmin.afterTestClass(testClass);
        }
    }

    @BeforeEach
    public void beforeTestMethod(final TestInfo testInfo) throws Exception
    {
        if (_testClass.compareAndSet(null, GroupJmsTestBase.this.getClass() ))
        {
            _groupBrokerAdmin = new GroupBrokerAdmin();
            _groupBrokerAdmin.beforeTestClass(GroupJmsTestBase.this.getClass());
        }
        _testName = testInfo.getTestMethod()
                .orElseThrow(() -> new RuntimeException("Failed to resolve test method"))
                .getName();
        _groupBrokerAdmin.beforeTestMethod(getClass(), getClass().getMethod(_testName));
    }

    @AfterEach
    public void afterTestMethod() throws Exception
    {
        _groupBrokerAdmin.afterTestMethod(getClass(), getClass().getMethod(_testName));
    }

    GroupBrokerAdmin getBrokerAdmin()
    {
        return _groupBrokerAdmin;
    }

    ConnectionBuilder getConnectionBuilder()
    {
        final ConnectionBuilder connectionBuilder = _jmsProvider.getConnectionBuilder()
                                                                    .setClientId(_testName)
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
            return getJmsProvider().createQueue(session, _testName);
        }
        finally
        {
            session.close();
        }
    }

}
