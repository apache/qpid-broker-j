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
package org.apache.qpid.server.virtualhostnode.derby;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerImpl;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class DerbyVirtualHostNodeTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private File _workDir;
    private Broker<BrokerImpl> _broker;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.DERBY)));

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();
        _workDir = TestFileUtils.createTestDirectory("qpid.work_dir", true);
        setTestSystemProperty("qpid.work_dir", _workDir.getAbsolutePath());
        _broker = createBroker();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_broker != null)
            {
                _broker.close();
            }
        }
        finally
        {
            if (_taskExecutor != null)
            {
                _taskExecutor.stop();
            }
            if (_workDir != null)
            {
                TestFileUtils.delete(_workDir, true);
            }
        }
    }

    @Test
    public void testCreateAndCloseVirtualHostNode() throws Exception
    {
        String nodeName = getTestName();
        Map<String, Object> nodeData = new HashMap<>();
        nodeData.put(VirtualHostNode.NAME, nodeName);
        nodeData.put(VirtualHostNode.TYPE, DerbyVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);

        VirtualHostNode<?> virtualHostNode = (VirtualHostNode<?>)_broker.createChild(VirtualHostNode.class, nodeData);
        virtualHostNode.start();
        virtualHostNode.close();
    }


    @Test
    public void testCreateDuplicateVirtualHostNodeAndClose() throws Exception
    {

        String nodeName = getTestName();
        Map<String, Object> nodeData = new HashMap<>();
        nodeData.put(VirtualHostNode.NAME, nodeName);
        nodeData.put(VirtualHostNode.TYPE, DerbyVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);

        VirtualHostNode<?> virtualHostNode = (VirtualHostNode<?>)_broker.createChild(VirtualHostNode.class, nodeData);
        virtualHostNode.start();

        try
        {
            _broker.createChild(VirtualHostNode.class, nodeData);
        }
        catch(Exception e)
        {
            assertEquals("Unexpected message",
                                "Child of type " + virtualHostNode.getClass().getSimpleName() + " already exists with name of " + getTestName(),
                                e.getMessage());

        }
        virtualHostNode.close();
    }

    @Test
    public void testOnCreateValidationForFileStorePath() throws Exception
    {
        File file = new File(_workDir, getTestName());
        file.createNewFile();

        String nodeName = getTestName();
        Map<String, Object> nodeData = new HashMap<>();
        nodeData.put(VirtualHostNode.NAME, nodeName);
        nodeData.put(VirtualHostNode.TYPE, DerbyVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        nodeData.put(DerbyVirtualHostNodeImpl.STORE_PATH, file.getAbsolutePath());
        try
        {
            _broker.createChild(VirtualHostNode.class, nodeData);
            fail("Cannot create store for the file store path");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }

    }


    @Test
    public void testOnCreateValidationForNonWritableStorePath() throws Exception
    {
        if (Files.getFileAttributeView(_workDir.toPath(), PosixFileAttributeView.class) != null)
        {
            File file = new File(_workDir, getTestName());
            file.mkdirs();
            if (file.setWritable(false, false))
            {
                String nodeName = getTestName();
                Map<String, Object> nodeData = new HashMap<>();
                nodeData.put(VirtualHostNode.NAME, nodeName);
                nodeData.put(VirtualHostNode.TYPE, DerbyVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
                nodeData.put(DerbyVirtualHostNodeImpl.STORE_PATH, file.getAbsolutePath());
                try
                {
                    _broker.createChild(VirtualHostNode.class, nodeData);
                    fail("Cannot create store for the non writable store path");
                }
                catch (IllegalConfigurationException e)
                {
                    // pass
                }
            }
        }
    }

    private BrokerImpl createBroker()
    {
        Map<String, Object> brokerAttributes = Collections.<String, Object>singletonMap(Broker.NAME, "Broker");
        SystemConfig parent = BrokerTestHelper.mockWithSystemPrincipal(SystemConfig.class, mock(Principal.class));
        when(parent.getEventLogger()).thenReturn(new EventLogger());
        when(parent.getCategoryClass()).thenReturn(SystemConfig.class);
        when(parent.getTaskExecutor()).thenReturn(_taskExecutor);
        when(parent.getChildExecutor()).thenReturn(_taskExecutor);
        when(parent.getModel()).thenReturn(BrokerModel.getInstance());
        when(parent.getObjectFactory()).thenReturn(new ConfiguredObjectFactoryImpl(BrokerModel.getInstance()));
        BrokerImpl broker = new BrokerImpl(brokerAttributes, parent);
        broker.start();
        return broker;
    }
}
