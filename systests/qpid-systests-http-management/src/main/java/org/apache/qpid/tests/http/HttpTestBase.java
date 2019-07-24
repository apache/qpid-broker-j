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

package org.apache.qpid.tests.http;

import static org.apache.qpid.systests.Utils.getJmsProvider;

import java.net.InetSocketAddress;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.After;
import org.junit.Before;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public abstract class HttpTestBase extends BrokerAdminUsingTestBase
{
    public static final String DEFAULT_BROKER_CONFIG = "classpath:config-http-management-tests.json";

    private HttpTestHelper _helper;

    private JmsProvider _jmsProvider;

    @Before
    public void setUpTestBase() throws Exception
    {
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");

        HttpRequestConfig config = getHttpRequestConfig();

        _helper = new HttpTestHelper(getBrokerAdmin(),
                                     config != null && config.useVirtualHostAsHost() ? getVirtualHost() : null);

        _jmsProvider = getJmsProvider();
    }

    @After
    public void tearDownTestBase()
    {
        System.clearProperty("sun.net.http.allowRestrictedHeaders");
    }

    protected String getVirtualHost()
    {
        return getClass().getSimpleName() + "_" + getTestName();
    }

    protected String getVirtualHostNode()
    {
        return getClass().getSimpleName() + "_" + getTestName();
    }

    public HttpTestHelper getHelper()
    {
        return _helper;
    }

    protected Connection getConnection() throws JMSException, NamingException
    {
        return getConnectionBuilder().build();
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

    private HttpRequestConfig getHttpRequestConfig() throws Exception
    {
        HttpRequestConfig config = getClass().getMethod(getTestName(), new Class[]{}).getAnnotation(HttpRequestConfig.class);
        if (config == null)
        {
            config = getClass().getAnnotation(HttpRequestConfig.class);
        }

        return config;
    }

    protected static long getReceiveTimeout()
    {
        return Utils.getReceiveTimeout();
    }

    protected static Protocol getProtocol()
    {
        return Utils.getProtocol();
    }

}
