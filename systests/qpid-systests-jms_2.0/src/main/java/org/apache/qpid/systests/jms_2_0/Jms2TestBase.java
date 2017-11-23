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
package org.apache.qpid.systests.jms_2_0;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Session;
import javax.naming.NamingException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.qpid.test.utils.AmqpManagementFacade;
import org.apache.qpid.test.utils.ConnectionBuilder;
import org.apache.qpid.test.utils.JmsProvider;
import org.apache.qpid.test.utils.QpidJmsClientProvider;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.url.URLSyntaxException;

public abstract class Jms2TestBase extends BrokerAdminUsingTestBase
{
    private static JmsProvider _jmsProvider;
    private static final AmqpManagementFacade _managementFacade = new AmqpManagementFacade("$management");

    @Rule
    public final TestName _testName = new TestName();
    private final List<Connection> _connections = new ArrayList<>();

    @BeforeClass
    public static void setUpTestBase()
    {
        _jmsProvider = new QpidJmsClientProvider(_managementFacade);
    }

    @After
    public void tearDown()
    {
        List<JMSException> exceptions = new ArrayList<>();
        for (Connection connection : _connections)
        {
            try
            {
                connection.close();
            }
            catch (JMSException e)
            {
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty())
        {
            JMSRuntimeException jmsRuntimeException = new JMSRuntimeException("Exception(s) occurred during closing of JMS connections.");
            for (JMSException exception : exceptions)
            {
                jmsRuntimeException.addSuppressed(exception);
            }
            throw jmsRuntimeException;
        }
    }

    protected ConnectionBuilder getConnectionBuilder()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsProvider.getConnectionBuilder()
                           .setHost(brokerAddress.getHostName())
                           .setPort(brokerAddress.getPort())
                           .setUsername(getBrokerAdmin().getValidUsername())
                           .setPassword(getBrokerAdmin().getValidPassword());
    }

    protected void createEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes)
            throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            _managementFacade.createEntityUsingAmqpManagement(entityName, session, entityType, attributes);
        }
    }

    protected Object performOperationUsingAmqpManagement(final String name,
                                                         final String operation,
                                                         final String type,
                                                         Map<String, Object> arguments)
            throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            return _managementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
        }
    }

    protected Connection getConnection() throws JMSException, NamingException, URLSyntaxException
    {
        return getConnectionBuilder().build();
    }

    protected long getReceiveTimeout()
    {
        return Long.getLong("qpid.test_receive_timeout", 1000L);
    }

    protected String getVirtualHostName()
    {
        return getClass().getSimpleName() + "_" + _testName.getMethodName();
    }

    protected String getTestName()
    {
        return _testName.getMethodName();
    }
}
