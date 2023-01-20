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

import java.lang.reflect.Method;

import ch.qos.logback.classic.LoggerContext;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit's extension. Logs test method execution start and end
 */
public class QpidUnitTestExtension implements AfterAllCallback, AfterEachCallback, BeforeAllCallback, BeforeEachCallback
{
    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestExtension.class);

    /** Logger context */
    private static final LoggerContext LOGGER_CONTEXT = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();

    /** Test class */
    private Class<?> _testClass;

    /** Test method */
    private Method _testMethod;

    /**
     * Callback executed before all testing methods
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void beforeAll(final ExtensionContext extensionContext)
    {
        _testClass = TestUtils.getTestClass(extensionContext);
        setClassQualifiedTestName(_testClass.getName());
    }

    /**
     * Callback executed before after all testing methods
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterAll(final ExtensionContext extensionContext)
    {
        _testClass = null;
        setClassQualifiedTestName(null);
    }

    /**
     * Callback executed before single testing method
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void beforeEach(final ExtensionContext extensionContext)
    {
        _testMethod = TestUtils.getTestMethod(extensionContext);
        LOGGER.info("========================= executing test : {}", _testClass.getSimpleName() + "#" + _testMethod.getName());
        setClassQualifiedTestName(_testClass.getName() + "." + _testMethod.getName());
        LOGGER.info("========================= start executing test : {}", _testClass.getSimpleName() + "#" + _testMethod.getName());
    }

    /**
     * Callback executed after single testing method
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterEach(final ExtensionContext extensionContext)
    {
        LOGGER.info("========================= stop executing test : {} ", _testClass.getSimpleName() + "#" + _testMethod.getName());
        setClassQualifiedTestName(_testClass.getName());
        LOGGER.info("========================= cleaning up test environment for test : {}", _testClass.getSimpleName() + "#" + _testMethod.getName());
        _testMethod = null;
    }

    /**
     * Sets test name into the logger context
     *
     * @param name Test name
     */
    private void setClassQualifiedTestName(final String name)
    {
        LOGGER_CONTEXT.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, name);
    }
}
