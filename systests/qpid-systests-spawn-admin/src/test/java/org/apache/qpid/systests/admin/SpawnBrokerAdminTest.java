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
package org.apache.qpid.systests.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.apache.qpid.systests.Utils.getJmsProvider;
import static org.apache.qpid.systests.Utils.getReceiveTimeout;
import static org.apache.qpid.systests.admin.SpawnBrokerAdmin.SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class SpawnBrokerAdminTest extends UnitTestBase
{

    @BeforeClass
    public static void appendLocalClassPath() throws Exception
    {
        String file = System.getProperty(SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE);

        // append test classpath in order to locate logback configuration for spawn broker
        final String appendedClasspath = System.getProperty("path.separator")
                                         + System.getProperty("java.class.path");
        Files.write(new File(file).toPath(),
                    appendedClasspath.getBytes(UTF_8),
                    APPEND);
    }


    @Test
    public void beforeTestClass() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            Connection connection = getJmsProvider()
                    .getConnectionBuilder()
                    .setUsername(admin.getValidUsername())
                    .setPassword(admin.getValidPassword())
                    .setVirtualHost("$management")
                    .setHost(brokerAddress.getHostName())
                    .setPort(brokerAddress.getPort())
                    .build();
            try
            {
                assertThat(connection.createSession(false, Session.AUTO_ACKNOWLEDGE), is(notNullValue()));
            }
            finally
            {
                connection.close();
            }
        }
    }

    @Test
    public void beforeTestMethod() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("beforeTestMethod"));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            Connection connection = getJmsProvider()
                    .getConnectionBuilder()
                    .setUsername(admin.getValidUsername())
                    .setPassword(admin.getValidPassword())
                    .setHost(brokerAddress.getHostName())
                    .setPort(brokerAddress.getPort())
                    .setVirtualHost(admin.getVirtualHostName())
                    .build();
            try
            {
                assertThat(connection.createSession(false, Session.AUTO_ACKNOWLEDGE), is(notNullValue()));
            }
            finally
            {
                connection.close();
            }
        }
    }

    @Test
    public void afterTestMethod() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("afterTestMethod"));
            admin.afterTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("afterTestMethod"));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            try
            {
                getJmsProvider()
                        .getConnectionBuilder()
                        .setUsername(admin.getValidUsername())
                        .setPassword(admin.getValidPassword())
                        .setHost(brokerAddress.getHostName())
                        .setPort(brokerAddress.getPort())
                        .setVirtualHost(admin.getVirtualHostName())
                        .build();
                fail("Exception is expected");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
    }

    @Test
    public void afterTestClass() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("afterTestClass"));
            admin.afterTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("afterTestClass"));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            admin.afterTestClass(SpawnBrokerAdmin.class);
            try
            {
                getJmsProvider()
                        .getConnectionBuilder()
                        .setUsername(admin.getValidUsername())
                        .setPassword(admin.getValidPassword())
                        .setHost(brokerAddress.getHostName())
                        .setPort(brokerAddress.getPort())
                        .setVirtualHost("$management")
                        .build();
                fail("Exception is expected");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
    }


    @Test
    public void createQueue() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("createQueue"));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            admin.createQueue(getTestName());
            Connection connection = getJmsProvider()
                    .getConnectionBuilder()
                    .setUsername(admin.getValidUsername())
                    .setPassword(admin.getValidPassword())
                    .setHost(brokerAddress.getHostName())
                    .setPort(brokerAddress.getPort())
                    .setVirtualHost(admin.getVirtualHostName())
                    .build();
            try
            {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(getTestName());
                MessageProducer messageProducer = session.createProducer(destination);
                messageProducer.send(session.createTextMessage("Test"));

                MessageConsumer consumer = session.createConsumer(destination);
                connection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertThat(message, is(notNullValue()));
                assertThat(message, instanceOf(TextMessage.class));
                assertThat(((TextMessage) message).getText(), is(equalTo("Test")));
            }
            finally
            {
                connection.close();
            }
        }
    }

    @Test
    public void deleteQueue() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("deleteQueue"));
            admin.createQueue(getTestName());
            admin.deleteQueue(getTestName());
            try
            {
                admin.getQueueDepthMessages(getTestName());
                fail("Get queue depth should fail as queue does not exists");
            }
            catch (BrokerAdminException e)
            {
                assertThat(e.getCause(), is(instanceOf(AmqpManagementFacade.OperationUnsuccessfulException.class)));
                assertThat(((AmqpManagementFacade.OperationUnsuccessfulException) e.getCause()).getStatusCode(), is(equalTo(404)));
            }
        }
    }

    @Test
    public void putMessageOnQueue() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("putMessageOnQueue"));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            admin.createQueue(getTestName());
            admin.putMessageOnQueue(getTestName(), "Test");
            Connection connection = getJmsProvider()
                    .getConnectionBuilder()
                    .setUsername(admin.getValidUsername())
                    .setPassword(admin.getValidPassword())
                    .setHost(brokerAddress.getHostName())
                    .setPort(brokerAddress.getPort())
                    .setVirtualHost(admin.getVirtualHostName())
                    .build();
            try
            {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(getTestName());
                MessageConsumer consumer = session.createConsumer(destination);
                connection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertThat(message, is(notNullValue()));
                assertThat(message, instanceOf(TextMessage.class));
                assertThat(((TextMessage) message).getText(), is(equalTo("Test")));
            }
            finally
            {
                connection.close();
            }
        }
    }

    @Test
    public void getQueueDepthMessages() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("getQueueDepthMessages"));
            admin.createQueue(getTestName());
            admin.putMessageOnQueue(getTestName(), "Test");
            assertThat(admin.getQueueDepthMessages(getTestName()), is(equalTo(1)));
        }
    }

    @Test
    public void restart() throws Exception
    {
        try (SpawnBrokerAdmin admin = new SpawnBrokerAdmin())
        {
            admin.beforeTestClass(SpawnBrokerAdminTest.class);
            admin.beforeTestMethod(SpawnBrokerAdminTest.class, getClass().getMethod("restart"));
            assumeThat(admin.supportsRestart(), is(equalTo(true)));
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            admin.createQueue(getTestName());
            Connection connection = getJmsProvider()
                    .getConnectionBuilder()
                    .setUsername(admin.getValidUsername())
                    .setPassword(admin.getValidPassword())
                    .setHost(brokerAddress.getHostName())
                    .setPort(brokerAddress.getPort())
                    .setVirtualHost(admin.getVirtualHostName())
                    .setSyncPublish(true)
                    .build();
            try
            {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(getTestName());
                MessageProducer messageProducer = session.createProducer(destination);
                messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                messageProducer.send(session.createTextMessage("Test"));
            }
            finally
            {
                connection.close();
            }

            assertThat(admin.getQueueDepthMessages(getTestName()), is(equalTo(1)));
            admin.restart();

            assertThat(admin.getQueueDepthMessages(getTestName()), is(equalTo(0)));
        }
    }
}
