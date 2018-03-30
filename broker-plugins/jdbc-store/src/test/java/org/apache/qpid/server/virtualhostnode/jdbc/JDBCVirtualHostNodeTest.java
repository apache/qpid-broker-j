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
 */

package org.apache.qpid.server.virtualhostnode.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.TestJdbcUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class JDBCVirtualHostNodeTest extends UnitTestBase
{
    private CurrentThreadTaskExecutor _taskExecutor;
    private String _connectionURL;

    @Before
    public void setUp() throws Exception
    {
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        if (_connectionURL != null)
        {
            TestJdbcUtils.shutdownDerby(_connectionURL);
        }
    }

    @After
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    @Test
    public void testInvalidTableNamePrefix() throws Exception
    {
        SystemConfig systemConfig = mock(SystemConfig.class);
        Broker broker = mock(Broker.class);
        final ConfiguredObjectFactoryImpl factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(broker.getObjectFactory()).thenReturn(factory);
        when(broker.getModel()).thenReturn(factory.getModel());
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(broker.getParent()).thenReturn(systemConfig);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, JDBCVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put("connectionUrl", "jdbc://example.com");
        JDBCVirtualHostNode<?> jdbcVirtualHostNode = new JDBCVirtualHostNodeImpl(attributes, broker);

        // This list is not exhaustive
        List<String> knownInvalidPrefixes = Arrays.asList("with\"dblquote",
                                                          "with'quote",
                                                          "with-dash",
                                                          "with;semicolon",
                                                          "with space",
                                                          "with%percent",
                                                          "with|pipe",
                                                          "with(paren",
                                                          "with)paren",
                                                          "with[bracket",
                                                          "with]bracket",
                                                          "with{brace",
                                                          "with}brace");
        for (String invalidPrefix : knownInvalidPrefixes)
        {
            try
            {
                jdbcVirtualHostNode.setAttributes(Collections.<String, Object>singletonMap("tableNamePrefix",
                                                                                           invalidPrefix));
                fail(String.format("Should not be able to set prefix to '%s'", invalidPrefix));
            }
            catch (IllegalConfigurationException e)
            {
                // pass
            }
        }
    }

    @Test
    public void testDeleteAction()
    {
        _connectionURL = "jdbc:derby:memory:/" + getTestName();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, JDBCVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put("connectionUrl", _connectionURL + ";create=true");

        Broker<?> broker = BrokerTestHelper.createBrokerMock();
        final VirtualHostNode virtualHostNode =
                broker.getObjectFactory().create(VirtualHostNode.class, attributes, broker);
        virtualHostNode.start();

        AtomicBoolean deleted = new AtomicBoolean();
        ((JDBCContainer) virtualHostNode).addDeleteAction(object -> deleted.set(true));

        virtualHostNode.delete();
        assertEquals("Delete action was not invoked", true, deleted.get());
    }
}
