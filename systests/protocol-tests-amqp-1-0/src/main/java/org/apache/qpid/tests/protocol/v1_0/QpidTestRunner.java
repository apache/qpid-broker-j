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
 */

package org.apache.qpid.tests.protocol.v1_0;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidTestRunner extends BlockJUnit4ClassRunner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidTestRunner.class);

    private final BrokerAdmin _brokerAdmin;
    private final Class _testClass;

    public QpidTestRunner(final Class<?> klass) throws InitializationError
    {
        super(klass);
        _testClass = klass;
        _brokerAdmin = (new BrokerAdminFactory()).createInstance("EMBEDDED_BROKER_PER_CLASS");

        LOGGER.debug("Runner ctor " + klass.getSimpleName());
    }

    @Override
    protected Object createTest() throws Exception
    {
        Object test = super.createTest();
        ProtocolTestBase qpidTest = ((ProtocolTestBase) test);
        qpidTest.init(_brokerAdmin);
        return test;
    }

    @Override
    public void run(final RunNotifier notifier)
    {
        _brokerAdmin.beforeTestClass(_testClass);
        try
        {
            super.run(notifier);
        }
        finally
        {
            _brokerAdmin.afterTestClass(_testClass);
        }
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier)
    {
        _brokerAdmin.beforeTestMethod(_testClass, method.getMethod());
        try
        {
            super.runChild(method, notifier);
        }
        finally
        {
            _brokerAdmin.afterTestMethod(_testClass, method.getMethod());
        }
    }
}
