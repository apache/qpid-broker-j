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

package org.apache.qpid.server.protocol.v1_0.store.bdb;


import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.io.Files;
import com.sleepycat.je.CacheMode;
import org.junit.Before;

import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreTestCase;
import org.apache.qpid.server.store.berkeleydb.BDBEnvironmentContainer;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.StandardEnvironmentConfiguration;
import org.apache.qpid.server.store.berkeleydb.StandardEnvironmentFacade;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBLinkStoreTest extends LinkStoreTestCase
{
    private StandardEnvironmentFacade _facade;
    private File _storeFolder;

    @Override
    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));
        super.setUp();
    }

    @Override
    protected LinkStore createLinkStore()
    {
        _storeFolder = Files.createTempDir();
        StandardEnvironmentConfiguration configuration = mock(StandardEnvironmentConfiguration.class);
        when(configuration.getName()).thenReturn("test");
        when(configuration.getStorePath()).thenReturn(_storeFolder.getAbsolutePath());
        when(configuration.getCacheMode()).thenReturn(CacheMode.DEFAULT);
        when(configuration.getParameters()).thenReturn(Collections.emptyMap());
        when(configuration.getFacadeParameter(eq(Integer.class),
                                              eq(LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME),
                                              anyInt())).thenReturn(0);
        when(configuration.getFacadeParameter(eq(Map.class),
                                              any(),
                                              eq(JUL_LOGGER_LEVEL_OVERRIDE),
                                              any())).thenReturn(Collections.emptyMap());
       _facade = new StandardEnvironmentFacade(configuration);

        BDBEnvironmentContainer environmentContainer = mock(BDBEnvironmentContainer.class);
        when(environmentContainer.getEnvironmentFacade()).thenReturn(_facade);
        return new BDBLinkStore(environmentContainer);
    }

    @Override
    protected void deleteLinkStore()
    {
        if (_facade != null)
        {
            _facade.close();
        }
        if (_storeFolder != null)
        {
            FileUtils.delete(_storeFolder, true);
        }
    }
}
