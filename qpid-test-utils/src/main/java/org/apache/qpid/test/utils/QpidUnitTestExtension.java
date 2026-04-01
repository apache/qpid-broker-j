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
 * JUnit extension that logs test method execution start and end.
 * <p>
 * Sets the {@code classQualifiedTestName} property on the Logback {@link LoggerContext} so that
 * the {@link ch.qos.logback.classic.sift.SiftingAppender SiftingAppender} configured with
 * {@link LogbackPropertyValueDiscriminator} can route log output into per-test files.
 * <p>
 * During {@code @BeforeAll}/{@code @AfterAll} the property is set to the fully-qualified class
 * name. During individual test execution it is narrowed to {@code className.methodName}, producing
 * a separate log file per test method.
 */
public class QpidUnitTestExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestExtension.class);

    private static final LoggerContext LOGGER_CONTEXT = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();

    private static final String BANNER = "=========================";

    /**
     * Sets the class-qualified test name before any test method runs.
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void beforeAll(final ExtensionContext extensionContext)
    {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        setClassQualifiedTestName(testClass.getName());
    }

    /**
     * Logs the test start and narrows the log routing to the method-level file.
     * <p>
     * The first log message is written to the class-level log file (before
     * {@code setClassQualifiedTestName} switches routing). The second message goes to the
     * method-level file.
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void beforeEach(final ExtensionContext extensionContext)
    {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final Method testMethod = extensionContext.getRequiredTestMethod();
        final String testDisplayName = testClass.getSimpleName() + "#" + testMethod.getName();

        LOGGER.info("{} executing test : {}", BANNER, testDisplayName);
        setClassQualifiedTestName(testClass.getName() + "." + testMethod.getName());
        LOGGER.info("{} start executing test : {}", BANNER, testDisplayName);
    }

    /**
     * Logs the test end and restores the log routing to the class-level file.
     * <p>
     * The first log message is written to the method-level log file. The second message goes to
     * the class-level file (after {@code setClassQualifiedTestName} restores class-level routing).
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterEach(final ExtensionContext extensionContext)
    {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final Method testMethod = extensionContext.getRequiredTestMethod();
        final String testDisplayName = testClass.getSimpleName() + "#" + testMethod.getName();

        LOGGER.info("{} stop executing test : {} ", BANNER, testDisplayName);
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("{} cleaning up test environment for test : {}", BANNER, testDisplayName);
    }

    /**
     * Clears the class-qualified test name after all test methods complete.
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterAll(final ExtensionContext extensionContext)
    {
        setClassQualifiedTestName(null);
    }

    /**
     * Sets the test name property on the Logback logger context.
     * {@link LogbackPropertyValueDiscriminator} reads this property to route log output.
     *
     * @param name fully-qualified test name, or {@code null} to clear
     */
    private void setClassQualifiedTestName(final String name)
    {
        LOGGER_CONTEXT.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, name);
    }
}
