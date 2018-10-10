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

import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.JUL_LOGGER_LEVEL_OVERRIDE;
import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade
        .LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class StandardEnvironmentFacadeTest extends UnitTestBase
{
    protected File _storePath;
    protected EnvironmentFacade _environmentFacade;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));

        _storePath = new File(TMP_FOLDER + File.separator + "bdb" + File.separator + getTestName());
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_environmentFacade != null)
            {
                _environmentFacade.close();
            }
        }
        finally
        {
            if (_storePath != null)
            {
                FileUtils.delete(_storePath, true);
            }
        }
    }

    @Test
    public void testSecondEnvironmentFacadeUsingSamePathRejected() throws Exception
    {
        EnvironmentFacade ef = createEnvironmentFacade();
        assertNotNull("Environment should not be null", ef);
        try
        {
            createEnvironmentFacade();
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException iae)
        {
            // PASS
        }

        ef.close();

        EnvironmentFacade ef2 = createEnvironmentFacade();
        assertNotNull("Environment should not be null", ef2);
    }

    @Test
    public void testClose() throws Exception
    {
        EnvironmentFacade ef = createEnvironmentFacade();
        ef.close();
    }

    @Test
    public void testOverrideJeParameter() throws Exception
    {
        // verify that transactions can be created by default
        EnvironmentFacade ef = createEnvironmentFacade();
        Transaction t = ef.beginTransaction(null);
        t.commit();
        ef.close();

        // customize the environment to be non-transactional
        ef = createEnvironmentFacade(Collections.singletonMap(EnvironmentConfig.ENV_IS_TRANSACTIONAL, "false"));
        try
        {
            ef.beginTransaction(null);
            fail("Overridden settings were not picked up on environment creation");
        }
        catch(UnsupportedOperationException e)
        {
            // pass
        }
        ef.close();
    }


    @Test
    public void testOpenDatabaseReusesCachedHandle() throws Exception
    {
        DatabaseConfig createIfAbsentDbConfig = DatabaseConfig.DEFAULT.setAllowCreate(true);

        EnvironmentFacade ef = createEnvironmentFacade();
        Database handle1 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotNull(handle1);

        Database handle2 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertSame("Database handle should be cached", handle1, handle2);

        ef.closeDatabase("myDatabase");

        Database handle3 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotSame("Expecting a new handle after database closure", handle1, handle3);
    }

    EnvironmentFacade createEnvironmentFacade()
    {
        _environmentFacade = createEnvironmentFacade(Collections.<String, String>emptyMap());
        return _environmentFacade;

    }

    EnvironmentFacade createEnvironmentFacade(Map<String, String> map)
    {
        StandardEnvironmentConfiguration sec = mock(StandardEnvironmentConfiguration.class);
        when(sec.getName()).thenReturn(getTestName());
        when(sec.getParameters()).thenReturn(map);
        when(sec.getStorePath()).thenReturn(_storePath.getAbsolutePath());
        when(sec.getFacadeParameter(eq(Integer.class),
                                    eq(LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME),
                                    anyInt())).thenReturn(0);
        when(sec.getFacadeParameter(eq(Map.class),
                                    any(),
                                    eq(JUL_LOGGER_LEVEL_OVERRIDE),
                                    any())).thenReturn(Collections.emptyMap());


        return new StandardEnvironmentFacade(sec);
    }

}
