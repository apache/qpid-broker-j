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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    private HttpManagement _management;

    @BeforeEach
    public void setUp() throws Exception
    {
        UUID id = UUID.randomUUID();
        Broker broker = mock(Broker.class);
        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getEventLogger()).thenReturn(mock(EventLogger.class));
        TaskExecutor taskExecutor = new TaskExecutorImpl();
        taskExecutor.start();
        when(broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(broker.getChildExecutor()).thenReturn(taskExecutor);


        Map<String, Object> attributes = new HashMap<>();
        attributes.put(HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED, true);
        attributes.put(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED, true);
        attributes.put(HttpManagement.NAME, getTestName());
        attributes.put(HttpManagement.TIME_OUT, 10000L);
        attributes.put(ConfiguredObject.ID, id);
        attributes.put(HttpManagement.DESIRED_STATE, State.QUIESCED);
        _management = new HttpManagement(attributes, broker);
        _management.open();
    }

    @Test
    public void testGetSessionTimeout()
    {
        assertEquals(10000L, _management.getSessionTimeout(), "Unexpected session timeout");
    }

    @Test
    public void testGetName()
    {
        assertEquals(getTestName(), _management.getName(), "Unexpected name");
    }

    @Test
    public void testIsHttpsSaslAuthenticationEnabled()
    {
        assertTrue(_management.isHttpsSaslAuthenticationEnabled(),
                "Unexpected value for the https sasl enabled attribute");

    }

    @Test
    public void testIsHttpSaslAuthenticationEnabled()
    {
        assertFalse(_management.isHttpSaslAuthenticationEnabled(),
                "Unexpected value for the http sasl enabled attribute");
    }

    @Test
    public void testIsHttpsBasicAuthenticationEnabled()
    {
        assertTrue(_management.isHttpsBasicAuthenticationEnabled(),
                "Unexpected value for the https basic authentication enabled attribute");
    }

    @Test
    public void testIsHttpBasicAuthenticationEnabled()
    {
        assertFalse(_management.isHttpBasicAuthenticationEnabled(),
                "Unexpected value for the http basic authentication enabled attribute");
    }
}
