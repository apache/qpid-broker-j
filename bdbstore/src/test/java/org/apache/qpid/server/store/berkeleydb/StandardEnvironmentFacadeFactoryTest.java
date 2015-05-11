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
package org.apache.qpid.server.store.berkeleydb;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.util.HashMap;


import com.sleepycat.je.Environment;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.util.FileUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StandardEnvironmentFacadeFactoryTest extends QpidTestCase
{
    private HashMap<String, String> _jeProperties;
    private File _path;
    private ConfiguredObject<?> _parent;

    @Override
    public void setUp()throws Exception
    {
        super.setUp();
        _jeProperties=  new HashMap<>();
        _jeProperties.put("je.log.memOnly", "true");
        _jeProperties.put("je.maxMemoryPercent", "5");
        _path = TestFileUtils.createTestDirectory(".je.test", true);

        // make mock object implementing FileBasedSettings
        _parent = mock(ConfiguredObject.class, withSettings().extraInterfaces(FileBasedSettings.class).defaultAnswer(new Answer()
        {
            public Object answer(InvocationOnMock invocation)
            {
                if (invocation.getMethod().getName().equals("getStorePath"))
                {
                    return _path.getAbsolutePath();
                }
                return null;
            }
        }));

    }
    @Override
    public void tearDown()throws Exception
    {
        try
        {
            EnvHomeRegistry.getInstance().deregisterHome(_path);
            FileUtils.delete(_path, true);
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testCreateEnvironmentFacade()
    {
        when(_parent.getName()).thenReturn(getTestName());
        when(_parent.getContextKeys(any(boolean.class))).thenReturn(_jeProperties.keySet());
        for (String key : _jeProperties.keySet())
        {
            when(_parent.getContextValue(String.class, key)).thenReturn(_jeProperties.get(key));
        }

        StandardEnvironmentFacadeFactory factory = new StandardEnvironmentFacadeFactory();
        EnvironmentFacade facade = factory.createEnvironmentFacade(_parent);
        try
        {
            assertNotNull("Facade should not be null", facade);
            Environment environment = facade.getEnvironment();
            for (String key : _jeProperties.keySet())
            {
                when(_parent.getContextValue(String.class, key)).thenReturn(_jeProperties.get(key));
                assertEquals("Unexpected environment setting", _jeProperties.get(key), environment.getConfig().getConfigParam(key));
            }
        }
        finally
        {
            facade.getEnvironment().close();
        }
    }


}
