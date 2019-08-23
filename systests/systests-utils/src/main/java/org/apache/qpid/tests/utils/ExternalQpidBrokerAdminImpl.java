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

package org.apache.qpid.tests.utils;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.PluggableService;

@SuppressWarnings("unused")
@PluggableService
public class ExternalQpidBrokerAdminImpl implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalQpidBrokerAdminImpl.class);
    private static final String EXTERNAL_BROKER = "EXTERNAL_BROKER";
    private static final String KIND_BROKER_UNKNOWN = "unknown";

    private final QueueAdmin _queueAdmin;
    private final Set<String> _createdQueues;

    public ExternalQpidBrokerAdminImpl()
    {
       this(new QueueAdminFactory().create());
    }

    ExternalQpidBrokerAdminImpl(QueueAdmin queueAdmin)
    {
        _queueAdmin = queueAdmin;
        _createdQueues = new HashSet<>();
    }
    @Override
    public void beforeTestClass(final Class testClass)
    {
        LOGGER.debug("beforeTestClass");
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        LOGGER.debug("beforeTestMethod");
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        LOGGER.debug("afterTestMethod");
        new HashSet<>(_createdQueues).forEach(this::deleteQueue);
        _createdQueues.clear();
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        LOGGER.debug("afterTestClass");
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        Integer port;
        switch (portType)
        {
            case AMQP:
                port = Integer.getInteger("qpid.tests.protocol.broker.external.port.standard");
                break;
            case ANONYMOUS_AMQP:
                port = Integer.getInteger("qpid.tests.protocol.broker.external.port.anonymous");
                break;
            case ANONYMOUS_AMQPWS:
                port = Integer.getInteger("qpid.tests.protocol.broker.external.port.websocket.anonymous");
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown port type '%s'", portType));
        }
        return new InetSocketAddress( "127.0.0.1", port);
    }

    @Override
    public void createQueue(final String queueName)
    {
        _queueAdmin.createQueue(this, queueName);
        _createdQueues.add(queueName);
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        _queueAdmin.deleteQueue(this, queueName);
        _createdQueues.remove(queueName);
    }

    @Override
    public void putMessageOnQueue(final String queueName, final String... messages)
    {
        _queueAdmin.putMessageOnQueue(this, queueName, messages);
    }

    @Override
    public int getQueueDepthMessages(final String testQueueName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsRestart()
    {
        return false;
    }

    @Override
    public ListenableFuture<Void> restart()
    {
        throw new UnsupportedOperationException("External Qpid Broker does not support restart.");
    }

    @Override
    public boolean isAnonymousSupported()
    {
        return Boolean.parseBoolean(System.getProperty("qpid.tests.protocol.broker.external.anonymousSupported", "true"));
    }

    @Override
    public boolean isSASLSupported()
    {
        return Boolean.parseBoolean(System.getProperty("qpid.tests.protocol.broker.external.saslSupported", "true"));
    }

    @Override
    public boolean isWebSocketSupported()
    {
        return Boolean.parseBoolean(System.getProperty("qpid.tests.protocol.broker.external.webSocketSupported", "false"));
    }

    @Override
    public boolean isQueueDepthSupported()
    {
        return false;
    }

    @Override
    public boolean isManagementSupported()
    {
        return Boolean.parseBoolean(System.getProperty("qpid.tests.protocol.broker.external.managementSupported", "false"));
    }

    @Override
    public boolean isSASLMechanismSupported(final String mechanismName)
    {
        final String supportedSaslMechanisms = System.getProperty(
                "qpid.tests.protocol.broker.external.supportedSaslMechanisms",
                "PLAIN,CRAM-MD5").toUpperCase();
        return Arrays.asList(supportedSaslMechanisms.split(",")).contains(mechanismName.toUpperCase());
    }

    @Override
    public String getValidUsername()
    {
        return System.getProperty("qpid.tests.protocol.broker.external.username");
    }

    @Override
    public String getValidPassword()
    {
        return System.getProperty("qpid.tests.protocol.broker.external.password");
    }

    @Override
    public String getKind()
    {
        return  System.getProperty("qpid.tests.protocol.broker.external.kind", KIND_BROKER_UNKNOWN);
    }

    @Override
    public String getType()
    {
        return EXTERNAL_BROKER;
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return _queueAdmin.isPutMessageOnQueueSupported();
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return _queueAdmin.isDeleteQueueSupported();
    }

}
