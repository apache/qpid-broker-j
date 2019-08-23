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
package org.apache.qpid.tests.utils;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.LogbackPropertyValueDiscriminator;

public class LoggingBrokerAdminDecorator implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingBrokerAdminDecorator.class);
    private BrokerAdmin _delegate;

    public LoggingBrokerAdminDecorator(final BrokerAdmin delegate)
    {
        _delegate = delegate;
    }

    @Override
    public void beforeTestClass(final Class testClass)
    {
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= starting broker for test class : " + testClass.getSimpleName());
        _delegate.beforeTestClass(testClass);
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        LOGGER.info("========================= prepare test environment for test : " + testClass.getSimpleName() + "#" + method.getName());

        _delegate.beforeTestMethod(testClass, method);

        LOGGER.info("========================= executing test : " + testClass.getSimpleName() + "#" + method.getName());
        setClassQualifiedTestName(testClass.getName() + "." + method.getName());
        LOGGER.info("========================= start executing test : " + testClass.getSimpleName() + "#" + method.getName());
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        LOGGER.info("========================= stop executing test : " + testClass.getSimpleName() + "#" + method.getName());
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= cleaning up test environment for test : " + testClass.getSimpleName() + "#" + method.getName());

        _delegate.afterTestMethod(testClass, method);

        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= cleaning done for test : " + testClass.getSimpleName() + "#" + method.getName());
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        LOGGER.info("========================= stopping broker for test class: " + testClass.getSimpleName());

        _delegate.afterTestClass(testClass);

        LOGGER.info("========================= stopping broker done for test class : " + testClass.getSimpleName());
        setClassQualifiedTestName(null);
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        return _delegate.getBrokerAddress(portType);
    }

    @Override
    public void createQueue(final String queueName)
    {
        _delegate.createQueue(queueName);
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        _delegate.deleteQueue(queueName);
    }

    @Override
    public void putMessageOnQueue(final String queueName, final String... messages)
    {
        _delegate.putMessageOnQueue(queueName, messages);
    }

    @Override
    public int getQueueDepthMessages(final String testQueueName)
    {
        return _delegate.getQueueDepthMessages(testQueueName);
    }

    @Override
    public boolean supportsRestart()
    {
        return _delegate.supportsRestart();
    }

    @Override
    public ListenableFuture<Void> restart()
    {
        return _delegate.restart();
    }

    @Override
    public boolean isAnonymousSupported()
    {
        return _delegate.isAnonymousSupported();
    }

    @Override
    public boolean isSASLSupported()
    {
        return _delegate.isSASLSupported();
    }

    @Override
    public boolean isSASLMechanismSupported(final String mechanismName)
    {
        return _delegate.isSASLMechanismSupported(mechanismName);
    }

    @Override
    public boolean isWebSocketSupported()
    {
        return _delegate.isWebSocketSupported();
    }

    @Override
    public boolean isQueueDepthSupported()
    {
        return _delegate.isQueueDepthSupported();
    }

    @Override
    public boolean isManagementSupported()
    {
        return _delegate.isManagementSupported();
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return _delegate.isPutMessageOnQueueSupported();
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return _delegate.isDeleteQueueSupported();
    }

    @Override
    public String getValidUsername()
    {
        return _delegate.getValidUsername();
    }

    @Override
    public String getValidPassword()
    {
        return _delegate.getValidPassword();
    }

    @Override
    public String getKind()
    {
        return _delegate.getKind();
    }

    @Override
    public String getType()
    {
        return _delegate.getType();
    }

    private void setClassQualifiedTestName(final String name)
    {
        final LoggerContext loggerContext = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();
        loggerContext.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, name);
    }
}
