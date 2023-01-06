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
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class StandardEnvironmentFacadeTest extends UnitTestBase
{
    protected File _storePath;
    protected EnvironmentFacade _environmentFacade;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _storePath = new File(TMP_FOLDER + File.separator + "bdb" + File.separator + getTestName());
    }

    @AfterEach
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
    public void testSecondEnvironmentFacadeUsingSamePathRejected()
    {
        EnvironmentFacade ef = createEnvironmentFacade();
        assertNotNull(ef, "Environment should not be null");
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
        assertNotNull(ef2, "Environment should not be null");
    }

    @Test
    public void testClose() throws Exception
    {
        EnvironmentFacade ef = createEnvironmentFacade();
        ef.close();
    }

    @Test
    public void testOverrideJeParameter()
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
    public void testOpenDatabaseReusesCachedHandle()
    {
        DatabaseConfig createIfAbsentDbConfig = DatabaseConfig.DEFAULT.setAllowCreate(true);

        EnvironmentFacade ef = createEnvironmentFacade();
        Database handle1 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotNull(handle1);

        Database handle2 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertSame(handle1, handle2, "Database handle should be cached");

        ef.closeDatabase("myDatabase");

        Database handle3 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotSame(handle1, handle3, "Expecting a new handle after database closure");
    }

    EnvironmentFacade createEnvironmentFacade()
    {
        _environmentFacade = createEnvironmentFacade(Collections.emptyMap());
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
        when(sec.getFacadeParameter(eq(Integer.class),
                                    eq(QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD),
                                    anyInt())).thenReturn(DEFAULT_QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD);
        when(sec.getFacadeParameter(eq(Long.class),
                                    eq(QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT),
                                    anyLong())).thenReturn(DEFAULT_QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT);

        return new StandardEnvironmentFacade(sec);
    }

}
