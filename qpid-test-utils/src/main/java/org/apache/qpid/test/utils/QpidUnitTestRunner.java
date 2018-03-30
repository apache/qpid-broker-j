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

package org.apache.qpid.test.utils;

import ch.qos.logback.classic.LoggerContext;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidUnitTestRunner extends BlockJUnit4ClassRunner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestRunner.class);

    private final Class<?> _testClass;

    public QpidUnitTestRunner(final Class<?> klass) throws InitializationError
    {
        super(klass);
        _testClass = klass;
    }

    @Override
    public void run(final RunNotifier notifier)
    {
        setClassQualifiedTestName(_testClass.getName());
        try
        {
            super.run(notifier);
        }
        finally
        {
            setClassQualifiedTestName(null);
        }
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier)
    {
        LOGGER.info("========================= executing test : {}", _testClass.getSimpleName() + "#" + method.getName());
        setClassQualifiedTestName(_testClass.getName() + "." + method.getName());
        LOGGER.info("========================= start executing test : {}", _testClass.getSimpleName() + "#" + method.getName());

        try
        {
            super.runChild(method, notifier);
        }
        finally
        {
            LOGGER.info("========================= stop executing test : {} ", _testClass.getSimpleName() + "#" + method.getName());
            setClassQualifiedTestName(_testClass.getName());
            LOGGER.info("========================= cleaning up test environment for test : {}", _testClass.getSimpleName() + "#" + method.getName());
        }
    }

    private void setClassQualifiedTestName(final String name)
    {
        final LoggerContext loggerContext = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();
        loggerContext.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, name);
    }



}
