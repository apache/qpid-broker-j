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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.util.Collections;

import com.sleepycat.je.EnvironmentConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class StandardEnvironmentFacadeFactoryTest extends UnitTestBase
{
    private File _path;
    private ConfiguredObject<?> _parent;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));

        _path = TestFileUtils.createTestDirectory(".je.test", true);

        // make mock object implementing FileBasedSettings
        _parent = mock(ConfiguredObject.class, withSettings().extraInterfaces(FileBasedSettings.class).defaultAnswer(new Answer()
        {
            @Override
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
    public void tearDown()throws Exception
    {
        try
        {
            EnvHomeRegistry.getInstance().deregisterHome(_path);
        }
        finally
        {
            FileUtils.delete(_path, true);
        }
    }

    @Test
    public void testCreateEnvironmentFacade()
    {
        when(_parent.getName()).thenReturn(getTestName());
        when(_parent.getContextKeys(any(boolean.class))).thenReturn(Collections.singleton(EnvironmentConfig.ENV_IS_TRANSACTIONAL));
        when(_parent.getContextValue(String.class, EnvironmentConfig.ENV_IS_TRANSACTIONAL)).thenReturn("false");

        StandardEnvironmentFacadeFactory factory = new StandardEnvironmentFacadeFactory();
        EnvironmentFacade facade = factory.createEnvironmentFacade(_parent);
        try
        {
            facade.beginTransaction(null);
            fail("Context variables were not picked up on environment creation");
        }
        catch(UnsupportedOperationException e)
        {
            //pass
        }
    }


}
