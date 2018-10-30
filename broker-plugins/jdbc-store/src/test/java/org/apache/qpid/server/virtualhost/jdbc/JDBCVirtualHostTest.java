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

package org.apache.qpid.server.virtualhost.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.TestJdbcUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class JDBCVirtualHostTest extends UnitTestBase
{
    private CurrentThreadTaskExecutor _taskExecutor;
    private String _connectionURL;

    @Before
    public void setUp() throws Exception
    {
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
    }

    @After
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
        if (_connectionURL != null)
        {
            TestJdbcUtils.shutdownDerby(_connectionURL);
        }
    }

    @Test
    public void testInvalidTableNamePrefix() throws Exception
    {
        final VirtualHostNode vhn = BrokerTestHelper.createVirtualHostNodeMock("testNode",
                                                                               true,
                                                                               BrokerTestHelper.createAccessControlMock(),
                                                                               BrokerTestHelper.createBrokerMock());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, JDBCVirtualHostImpl.VIRTUAL_HOST_TYPE);
        attributes.put("connectionUrl", "jdbc://example.com");
        JDBCVirtualHost<?> jdbcVirtualHost = new JDBCVirtualHostImpl(attributes, vhn);

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
                jdbcVirtualHost.setAttributes(Collections.<String, Object>singletonMap("tableNamePrefix",
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
        attributes.put(ConfiguredObject.TYPE, JDBCVirtualHostImpl.VIRTUAL_HOST_TYPE);
        attributes.put("connectionUrl", _connectionURL + ";create=true");

        final VirtualHost vh = BrokerTestHelper.createVirtualHost(attributes, this);

        AtomicBoolean deleted = new AtomicBoolean();
        ((JDBCContainer)vh).addDeleteAction(object -> deleted.set(true));

        vh.delete();
        assertEquals("Delete action was not invoked", true, deleted.get());
    }
}
