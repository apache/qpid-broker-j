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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for unit tests.
 */
@ExtendWith(QpidUnitTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UnitTestBase
{
    /** Temporary folder location */
    public static final String TMP_FOLDER = System.getProperty("java.io.tmpdir");

    /** Type of the virtual host node */
    private static final String PROFILE_VIRTUALHOSTNODE_TYPE = "virtualhostnode.type";

    /** Port helper */
    private static final PortHelper PORT_HELPER = new PortHelper();

    /** Helper class for setting system properties during test execution */
    public final SystemPropertySetter _systemPropertySetter = new SystemPropertySetter();

    /** Set of callbacks executed after all tests */
    private final Set<Runnable> _afterAllTearDownRegistry = new LinkedHashSet<>();

    /** Test class name */
    private String _testClassName;

    /** Test name */
    private String _testName;

    /**
     * Resolves test class name from TestInfo
     *
     * @param testInfo TestInfo
     */
    @BeforeAll
    public void beforeAll(final TestInfo testInfo)
    {
        _testClassName = testInfo.getTestClass()
                .orElseThrow(() -> new RuntimeException("Failed to resolve test method"))
                .getSimpleName();
    }

    /**
     * Resolves test method name from TestInfo
     *
     * @param testInfo TestInfo
     */
    @BeforeEach
    public void beforeEach(final TestInfo testInfo)
    {
        _testName = testInfo.getTestMethod()
                .orElseThrow(() -> new RuntimeException("Failed to resolve test method"))
                .getName();
    }

    /** Executes callbacks and cleans system variables */
    @AfterEach
    public void cleanupAfterEach()
    {
        _systemPropertySetter.afterEach();
    }

    /** Executes callbacks and cleans system variables */
    @AfterAll
    public void cleanupAfterAll()
    {
        _afterAllTearDownRegistry.forEach(Runnable::run);
        _afterAllTearDownRegistry.clear();
    }

    /**
     * Retrieves test class name
     *
     * @return Test class name
     */
    public String getTestClassName()
    {
        return _testClassName;
    }

    /**
     * Retrieves test name (name of the test method)
     *
     * @return Test name
     */
    public String getTestName()
    {
        return _testName;
    }

    /**
     * Sets system property
     *
     * @param property Property name
     * @param value Property value
     */
    public void setTestSystemProperty(final String property, final String value)
    {
        _systemPropertySetter.setSystemProperty(property, value);
    }

    /**
     * Retrieves free port number
     *
     * @return Free port number
     */
    public int findFreePort()
    {
        return PORT_HELPER.getNextAvailable();
    }

    /**
     * Retrieves free port number greater than one supplied
     *
     * @param fromPort Port number to start from
     *
     * @return Free port number
     */
    public int getNextAvailable(int fromPort)
    {
        return PORT_HELPER.getNextAvailable(fromPort);
    }

    /**
     * Returns random UUID
     *
     * @return UUID
     */
    public static UUID randomUUID()
    {
        return UUID.randomUUID();
    }

    /**
     * Adds callback to execute after all tests
     *
     * @param runnable Runnable instance
     */
    public void registerAfterAllTearDown(Runnable runnable)
    {
        _afterAllTearDownRegistry.add(runnable);
    }

    /**
     * Retrieves JVM vendor
     *
     * @return JvmVendor
     */
    public static JvmVendor getJvmVendor()
    {
        return JvmVendor.getJvmVendor();
    }

    /**
     * Retrieves virtual host node store type
     *
     * @return VirtualHostNodeStoreType
     */
    public VirtualHostNodeStoreType getVirtualHostNodeStoreType()
    {
        final String type = System.getProperty(PROFILE_VIRTUALHOSTNODE_TYPE, VirtualHostNodeStoreType.MEMORY.name()).toUpperCase();
        return VirtualHostNodeStoreType.valueOf(type);
    }
}
