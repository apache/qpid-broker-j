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
package org.apache.qpid.server.virtualhostnode.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBVirtualHostNodeTest extends UnitTestBase
{
    private Broker<?> _broker;
    private File _storePath;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _broker = BrokerTestHelper.createBrokerMock();
        TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);

        _storePath = TestFileUtils.createTestDirectory();
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
        String nodeName = getTestName();
        File file = new File(_storePath + File.separator + nodeName);
        assertTrue(file.createNewFile(), "Empty file is not created");
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BDBVirtualHostNode.ID, UUID.randomUUID());
        attributes.put(BDBVirtualHostNode.TYPE, BDBVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put(BDBVirtualHostNode.NAME, nodeName);
        attributes.put(BDBVirtualHostNode.STORE_PATH, file.getAbsolutePath());

        BDBVirtualHostNodeImpl node = new BDBVirtualHostNodeImpl(attributes, _broker);
        try
        {
            node.create();
            fail("Cannot create DBD node from existing empty file");
        }
        catch (IllegalConfigurationException e)
        {
            assertTrue(e.getMessage().startsWith("Cannot open node configuration store"),
                    "Unexpected exception " + e.getMessage());
        }
    }

}
