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
package org.apache.qpid.systests;

import java.net.InetSocketAddress;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public abstract class JmsTestBase extends BrokerAdminUsingTestBase
{
    private static JmsProvider _jmsProvider;
    private static AmqpManagementFacade _managementFacade;

    @Rule
    public final TestName _testName = new TestName();

    @BeforeClass
    public static void setUpTestBase()
    {
        if ("1.0".equals(System.getProperty("broker.version", "1.0")))
        {
            _managementFacade = new AmqpManagementFacade("$management");
            _jmsProvider = new QpidJmsClientProvider(_managementFacade);
        }
        else
        {
            _managementFacade = new AmqpManagementFacade("ADDR:$management");
            _jmsProvider = new QpidJmsClient0xProvider();
        }
    }

    protected ConnectionBuilder getConnectionBuilder()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsProvider.getConnectionBuilder()
                           .setHost(brokerAddress.getHostName())
                           .setPort(brokerAddress.getPort())
                           .setUsername(getBrokerAdmin().getValidUsername())
                           .setPassword(getBrokerAdmin().getValidPassword())
                ;
    }

    protected void createEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            _managementFacade.createEntityUsingAmqpManagement(entityName, session, entityType, attributes);
        }
        finally
        {
            connection.close();
        }
    }

    protected Object performOperationUsingAmqpManagement(final String name,
                                                         final String operation,
                                                         final String type,
                                                         Map<String, Object> arguments)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            return _managementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
        }
        finally
        {
            connection.close();
        }
    }

    protected Connection getConnection() throws JMSException, NamingException
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

    protected Topic createTopic(final String topicName) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            return _jmsProvider.createTopic(connection, topicName);
        }
        finally
        {
            connection.close();
        }
    }
}
