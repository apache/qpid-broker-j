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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.StandardSystemProperty;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

@RunWith(QpidUnitTestRunner.class)
public class UnitTestBase
{
    public static final String TMP_FOLDER = System.getProperty("java.io.tmpdir");
    private static final String PROFILE_VIRTUALHOSTNODE_TYPE = "virtualhostnode.type";

    @Rule
    public final TestName _testName = new TestName();

    private final Map<String, String> _propertiesSetForTest = new HashMap<>();
    private final Set<Runnable> _tearDownRegistry = new LinkedHashSet<>();

    @After
    public void cleanupPostTest()
    {
        revertSysProps();

        _tearDownRegistry.forEach(Runnable::run);
    }

    private void revertSysProps()
    {
        _propertiesSetForTest.forEach((key, value) -> {
            if (value != null)
            {
                System.setProperty(key, value);
            }
            else
            {
                System.clearProperty(key);
            }
        });
    }

    public String getTestName()
    {
        return _testName.getMethodName();
    }

    public void setTestSystemProperty(final String property, final String value)
    {
        if (!_propertiesSetForTest.containsKey(property))
        {
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
    }

    public int findFreePort()
    {
        return new PortHelper().getNextAvailable();
    }

    public int getNextAvailable(int fromPort)
    {
        return new PortHelper().getNextAvailable(fromPort);
    }


    public void registerTearDown(Runnable runnable)
    {
        _tearDownRegistry.add(runnable);
    }

    public JvmVendor getJvmVendor()
    {
        final String property = String.valueOf(System.getProperty(StandardSystemProperty.JAVA_VENDOR.key())).toUpperCase();
        if (property.contains("IBM"))
        {
            return JvmVendor.IBM;
        }
        else if (property.contains("ORACLE"))
        {
            return JvmVendor.ORACLE;
        }
        else if (property.contains("OPENJDK"))
        {
            return JvmVendor.OPENJDK;
        }
        else
        {
            return JvmVendor.UNKNOWN;
        }
    }

    public VirtualHostNodeStoreType getVirtualHostNodeStoreType()
    {
        final String type = System.getProperty(PROFILE_VIRTUALHOSTNODE_TYPE, VirtualHostNodeStoreType.MEMORY.name()).toUpperCase();
        return VirtualHostNodeStoreType.valueOf(type);
    }
}
