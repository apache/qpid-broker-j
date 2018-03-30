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
package org.apache.qpid.server.management.plugin;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.State;
import org.apache.qpid.test.utils.UnitTestBase;

public class HttpManagementTest extends UnitTestBase
{
    private UUID _id;
    private Broker _broker;
    private HttpManagement _management;

    @Before
    public void setUp() throws Exception
    {
        _id = UUID.randomUUID();
        _broker = mock(Broker.class);
        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        when(_broker.getObjectFactory()).thenReturn(objectFactory);
        when(_broker.getModel()).thenReturn(objectFactory.getModel());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        TaskExecutor taskExecutor = new TaskExecutorImpl();
        taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);


        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED, true);
        attributes.put(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED, true);
        attributes.put(HttpManagement.NAME, getTestName());
        attributes.put(HttpManagement.TIME_OUT, 10000l);
        attributes.put(ConfiguredObject.ID, _id);
        attributes.put(HttpManagement.DESIRED_STATE, State.QUIESCED);
        _management = new HttpManagement(attributes, _broker);
        _management.open();
    }

    @Test
    public void testGetSessionTimeout()
    {
        assertEquals("Unexpected session timeout", 10000l, (long) _management.getSessionTimeout());
    }

    @Test
    public void testGetName()
    {
        assertEquals("Unexpected name", getTestName(), _management.getName());
    }

    @Test
    public void testIsHttpsSaslAuthenticationEnabled()
    {
        assertEquals("Unexpected value for the https sasl enabled attribute",
                            true,
                            _management.isHttpsSaslAuthenticationEnabled());

    }

    @Test
    public void testIsHttpSaslAuthenticationEnabled()
    {
        assertEquals("Unexpected value for the http sasl enabled attribute",
                            false,
                            _management.isHttpSaslAuthenticationEnabled());
    }

    @Test
    public void testIsHttpsBasicAuthenticationEnabled()
    {
        assertEquals("Unexpected value for the https basic authentication enabled attribute",
                            true,
                            _management.isHttpsBasicAuthenticationEnabled());
    }

    @Test
    public void testIsHttpBasicAuthenticationEnabled()
    {
        assertEquals("Unexpected value for the http basic authentication enabled attribute",
                            false,
                            _management.isHttpBasicAuthenticationEnabled());
    }


}
