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
package org.apache.qpid.server.virtualhost.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBVirtualHostImplTest extends UnitTestBase
{
    private File _storePath;
    private VirtualHostNode<?> _node;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _storePath = TestFileUtils.createTestDirectory();

        _node = BrokerTestHelper.createVirtualHostNodeMock("testNode",
                                                           true,
                                                           BrokerTestHelper.createAccessControlMock(),
                                                           BrokerTestHelper.createBrokerMock());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_storePath != null)
        {
            FileUtils.delete(_storePath, true);
        }
    }

    @Test
    public void testValidateOnCreateForInvalidStorePath() throws Exception
    {
        String hostName = getTestName();
        File file = new File(_storePath + File.separator + hostName);
        assertTrue(file.createNewFile(), "Empty file is not created");
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BDBVirtualHost.ID, UUID.randomUUID());
        attributes.put(BDBVirtualHost.TYPE, BDBVirtualHostImpl.VIRTUAL_HOST_TYPE);
        attributes.put(BDBVirtualHost.NAME, hostName);
        attributes.put(BDBVirtualHost.STORE_PATH, file.getAbsoluteFile());

        BDBVirtualHostImpl host = new BDBVirtualHostImpl(attributes, _node);
        try
        {
            host.create();
            fail("Cannot create DBD virtual host from existing empty file");
        }
        catch (IllegalConfigurationException e)
        {
            assertTrue(e.getMessage().startsWith("Cannot open virtual host message store"),
                       "Unexpected exception " + e.getMessage());
        }
    }

}
