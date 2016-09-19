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
package org.apache.qpid.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import ch.qos.logback.classic.ClassicConstants;
import junit.framework.TestCase;
import junit.framework.TestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class QpidTestCase extends TestCase
{
    private static final String TEST_EXCLUDES = "test.excludes";
    private static final String TEST_EXCLUDELIST = "test.excludelist";
    private static final String TEST_EXCLUDEFILES = "test.excludefiles";
    private static final String VIRTUAL_HOST_NODE_TYPE = "virtualhostnode.type";
    private static final String VIRTUAL_HOST_NODE_CONTEXT_BLUEPRINT = "virtualhostnode.context.blueprint";
    private static final String TEST_OVERRIDDEN_PROPERTIES = "test.overridden.properties";

    public static final String QPID_HOME = System.getProperty("QPID_HOME");
    public static final String TEST_PROFILES_DIR = QPID_HOME + File.separator + ".." + File.separator + "test-profiles" + File.separator;
    public static final String TEST_RESOURCES_DIR = TEST_PROFILES_DIR + "test_resources/";
    public static final String TMP_FOLDER = System.getProperty("java.io.tmpdir");
    public static final String CLASS_QUALIFIED_TEST_NAME = "classQualifiedTestName";

    private static final Logger _logger = LoggerFactory.getLogger(QpidTestCase.class);
    private static QpidTestCase _currentInstance;

    private final Map<String, String> _propertiesSetForTest = new HashMap<>();

    private final Set<Runnable> _tearDownRegistry = new HashSet<>();

    /**
     * Some tests are excluded when the property test.excludes is set to true.
     * An exclusion list is either a file (prop test.excludesfile) which contains one test name
     * to be excluded per line or a String (prop test.excludeslist) where tests to be excluded are
     * separated by " ". Excluded tests are specified following the format:
     * className#testName where className is the class of the test to be
     * excluded and testName is the name of the test to be excluded.
     * className#* excludes all the tests of the specified class.
     */
    static
    {
        if (Boolean.getBoolean("test.exclude"))
        {
            _logger.info("Some tests should be excluded, building the exclude list");
            String exclusionListURIs = System.getProperty(TEST_EXCLUDEFILES, "");
            String exclusionListString = System.getProperty(TEST_EXCLUDELIST, "");
            String testExcludes = System.getProperty(TEST_EXCLUDES);

            //For the maven build, process the test.excludes property
            if(testExcludes != null && "".equals(exclusionListURIs))
            {
                for (String exclude : testExcludes.split("\\s+"))
                {
                    exclusionListURIs += TEST_PROFILES_DIR + File.separator + exclude + ";";
                }
            }


            List<String> exclusionList = new ArrayList<>();
            for (String uri : exclusionListURIs.split(";\\s*"))
            {
                File file = new File(uri);
                if (file.exists())
                {
                    _logger.info("Using exclude file: " + uri);
                    try(FileReader fileReader = new FileReader(file))
                    {
                        try(BufferedReader in = new BufferedReader(fileReader))
                        {
                            String excludedTest = in.readLine();
                            do
                            {
                                exclusionList.add(excludedTest);
                                excludedTest = in.readLine();
                            }
                            while (excludedTest != null);
                        }
                    }
                    catch (IOException e)
                    {
                        _logger.warn("Exception when reading exclusion list", e);
                    }
                }
                else
                {
                    _logger.info("Specified exclude file does not exist: " + uri);
                }
            }

            if (!exclusionListString.equals(""))
            {
                _logger.info("Using excludeslist: " + exclusionListString);
                for (String test : exclusionListString.split("\\s+"))
                {
                    exclusionList.add(test);
                }
            }

            _exclusionList = Collections.unmodifiableList(exclusionList);
        }
        else
        {
            _exclusionList = Collections.emptyList();
        }
    }

    private static final List<String> _exclusionList;
    private static final Properties OVERRIDDEN_PROPERTIES = loadOverriddenTestSystemProperties();

    public void run(TestResult testResult)
    {
        try
        {
            _currentInstance = this;
            MDC.put(CLASS_QUALIFIED_TEST_NAME, getClassQualifiedTestName());

            if (_exclusionList.contains(getClass().getPackage().getName() + ".*") ||
                _exclusionList.contains(getClass().getName() + "#*") ||
                _exclusionList.contains(getClass().getName() + "#" + getName()))
            {
                _logger.info("Test: " + getName() + " is excluded");
                testResult.endTest(this);
            }
            else
            {
                overrideTestSystemProperties();
                super.run(testResult);
            }
        }
        finally
        {
            _logger.info(ClassicConstants.FINALIZE_SESSION_MARKER, "Shutting down sub-appender");
            _currentInstance = null;
            MDC.remove(CLASS_QUALIFIED_TEST_NAME);
            revertTestSystemProperties();
        }
    }

    @Override
    protected void runTest() throws Throwable
    {
        _logger.info("========== run " + getTestName() + " ==========");
        super.runTest();
    }

    public String getTestProfileVirtualHostNodeType()
    {
        final String storeType = System.getProperty(VIRTUAL_HOST_NODE_TYPE);

        if (_logger.isDebugEnabled())
        {
            _logger.debug(VIRTUAL_HOST_NODE_TYPE + "=" + storeType);
        }

        return storeType != null ? storeType : "TestMemory";
    }

    protected String getClassQualifiedTestName()
    {
        return getClass().getCanonicalName() + "." + getName();
    }

    protected String getTestName()
    {
        return getClass().getSimpleName() + "." + getName();
    }

    public String getTestProfileVirtualHostNodeBlueprint()
    {
        return System.getProperty(VIRTUAL_HOST_NODE_CONTEXT_BLUEPRINT);
    }

    public void registerTearDown(Runnable runnable)
    {
        _tearDownRegistry.add(runnable);
    }

    public static QpidTestCase getCurrentInstance()
    {
        return _currentInstance;
    }

    /**
     * Gets the next available port starting at a port.
     *
     * @param fromPort the port to scan for availability
     * @throws NoSuchElementException if there are no ports available
     */
    public int getNextAvailable(int fromPort)
    {
        return new PortHelper().getNextAvailable(fromPort);
    }

    public int findFreePort()
    {
        return new PortHelper().getNextAvailable();
    }

    /**
     * Set a System property for duration of this test only. The tearDown will
     * guarantee to reset the property to its previous value after the test
     * completes.
     *
     * @param property The property to set
     * @param value the value to set it to, if null, the property will be cleared
     */
    protected void setTestSystemProperty(final String property, final String value)
    {
        if (!_propertiesSetForTest.containsKey(property))
        {
            // Record the current value so we can revert it later.
            _propertiesSetForTest.put(property, System.getProperty(property));
        }

        if (value == null)
        {
            System.clearProperty(property);
        }
        else
        {
            System.setProperty(property, value);
        }

        _logger.info("Set system property \"" + property + "\" to: \"" + value + "\"");
    }

    /**
     * Restore the System property values that were set by this test run.
     */
    protected void revertTestSystemProperties()
    {
        if(!_propertiesSetForTest.isEmpty())
        {
            _logger.debug("reverting " + _propertiesSetForTest.size() + " test properties");
            for (String key : _propertiesSetForTest.keySet())
            {
                String value = _propertiesSetForTest.get(key);
                if (value != null)
                {
                    System.setProperty(key, value);
                }
                else
                {
                    System.clearProperty(key);
                }
            }

            _propertiesSetForTest.clear();
        }
    }

    protected void setUp() throws Exception
    {
        _logger.info("========== start " + getTestName() + " ==========");
        super.setUp();
    }

    protected void tearDown() throws Exception
    {
        _logger.info("========== tearDown " + getTestName() + " ==========");
        for (Runnable runnable : _tearDownRegistry)
        {
            runnable.run();
        }
        _tearDownRegistry.clear();
    }

    protected void overrideTestSystemProperties()
    {
        setTestOverriddenProperties(OVERRIDDEN_PROPERTIES);
    }

    protected void setTestOverriddenProperties(Properties properties)
    {
        for (String propertyName : properties.stringPropertyNames())
        {
            setTestSystemProperty(propertyName, properties.getProperty(propertyName));
        }
    }

    private static Properties loadOverriddenTestSystemProperties()
    {
        Properties properties = new Properties();
        String pathToFileWithOverriddenClientAndBrokerProperties = System.getProperty(TEST_OVERRIDDEN_PROPERTIES);
        if (pathToFileWithOverriddenClientAndBrokerProperties != null)
        {
            File file = new File(pathToFileWithOverriddenClientAndBrokerProperties);
            if (file.exists())
            {
                _logger.info("Loading overridden system properties from {}", file.getAbsolutePath());
                try (InputStream propertiesStream = new FileInputStream(file))
                {

                    properties.load(propertiesStream);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(String.format(
                            "Cannot load overridden properties from '%s'. Verify value provided with system property '%s'",
                            file.getAbsolutePath(),
                            TEST_OVERRIDDEN_PROPERTIES), e);
                }
            }
            else
            {
                throw new RuntimeException(String.format(
                        "File with overridden properties at '%s' does not exists. Verify value provided with system property '%s'",
                        file.getAbsolutePath(),
                        TEST_OVERRIDDEN_PROPERTIES));
            }
        }
        return properties;
    }
}
