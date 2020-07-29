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

package org.apache.qpid.server.management.plugin.controller.v7_0.category;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.ConfiguredObject;


public class LegacyCategoryController_v8_0Test
{
    private static final String TEST_CATEGORY = "Port";
    private static final String PARENT_CATEGORY = "Broker";
    private static final String DEFAULT_TYPE = "";
    private static final String PORT_NAME = "testPort";
    private static final String PROTOCOL_ALLOW_LIST = "Tls.*";
    private static final String PROTOCOL_DENY_LIST = "Ssl.*";
    private static final String NEW_CONTEXT_TLS_PROTOCOL_ALLOW_LIST = "qpid.security.tls.protocolAllowList";
    public static final String NEW_CONTEXT_TLS_PROTOCOL_DENY_LIST = "qpid.security.tls.protocolDenyList";
    public static final String OLD_CONTEXT_TLS_PROTOCOL_WHITE_LIST = "qpid.security.tls.protocolWhiteList";
    public static final String OLD_CONTEXT_TLS_PROTOCOL_BLACK_LIST = "qpid.security.tls.protocolBlackList";
    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_CONTEXT = "context";

    private  LegacyCategoryController_v8_0 _controller;
    private ConfiguredObject _root;
    private ManagementController _nextVersionManagementController;

    @Before
    public void setUp()
    {
        _nextVersionManagementController = mock(ManagementController.class);
        LegacyManagementController managementController = mock(LegacyManagementController.class);
        when(managementController.getNextVersionManagementController()).thenReturn(_nextVersionManagementController);
        _controller = new LegacyCategoryController_v8_0(managementController,
                                                        TEST_CATEGORY,
                                                        PARENT_CATEGORY,
                                                        DEFAULT_TYPE,
                                                        Collections.emptySet());

        _root = mock(ConfiguredObject.class);
    }

    @Test
    public void getExistingPortWithSetAllowDenyTlsProtocolSettings()
    {
        final List<String> path = Arrays.asList("port", PORT_NAME);
        final Map<String, List<String>> parameters = Collections.emptyMap();
        final LegacyConfiguredObject nextVersionPort = createNewVersionPortMock();

        when(_nextVersionManagementController.get(_root,
                                                  TEST_CATEGORY,
                                                  path,
                                                  parameters)).thenReturn(nextVersionPort);

        final Object port = _controller.get(_root, path, parameters);
        assertThat(port, instanceOf(LegacyConfiguredObject.class));
        final LegacyConfiguredObject newPort = (LegacyConfiguredObject)port;
        assertPortTLSSettings(newPort);
    }


    @Test
    public void testCreatePortWithSetAllowDenyTlsProtocolSettings()
    {
        final List<String> path = Arrays.asList("port", PORT_NAME);

        final Map<String, String> oldContext = new HashMap<>();
        oldContext.put(OLD_CONTEXT_TLS_PROTOCOL_WHITE_LIST,PROTOCOL_ALLOW_LIST);
        oldContext.put(OLD_CONTEXT_TLS_PROTOCOL_BLACK_LIST,PROTOCOL_DENY_LIST);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTRIBUTE_NAME, PORT_NAME);
        attributes.put(ATTRIBUTE_CONTEXT, oldContext);
        attributes.put("type", "AMQP");


        final Map<String,String> newVersionContext = new HashMap<>();
        newVersionContext.put(NEW_CONTEXT_TLS_PROTOCOL_ALLOW_LIST, PROTOCOL_ALLOW_LIST);
        newVersionContext.put(NEW_CONTEXT_TLS_PROTOCOL_DENY_LIST, PROTOCOL_DENY_LIST);
        Map<String, Object> newAttributes = new HashMap<>();
        newAttributes.put(ATTRIBUTE_NAME, PORT_NAME);
        newAttributes.put(ATTRIBUTE_CONTEXT, newVersionContext);
        newAttributes.put("type", "AMQP");

        LegacyConfiguredObject newVersionPort = createNewVersionPortMock();
        when(_nextVersionManagementController.createOrUpdate(eq(_root), eq(TEST_CATEGORY), eq(path), eq(newAttributes), eq(false) )).thenReturn(newVersionPort);
        ManagementException error = ManagementException.createUnprocessableManagementException("unexpected");
       // when(_nextVersionManagementController.createOrUpdate(any(ConfiguredObject.class), anyString(), anyList(), anyMap(), anyBoolean())).thenThrow(error);
        LegacyConfiguredObject port = _controller.createOrUpdate(_root, path, attributes, false) ;
        assertThat(port, is(notNullValue()));
        assertPortTLSSettings(port);
    }

    private void assertPortTLSSettings(final LegacyConfiguredObject port)
    {
        assertThat(port.getAttribute(ATTRIBUTE_NAME), equalTo(PORT_NAME));
        assertThat(port.getContextValue(OLD_CONTEXT_TLS_PROTOCOL_WHITE_LIST), equalTo(PROTOCOL_ALLOW_LIST));
        assertThat(port.getContextValue(OLD_CONTEXT_TLS_PROTOCOL_BLACK_LIST), equalTo(PROTOCOL_DENY_LIST));
        final Object context = port.getAttribute("context");
        assertThat(context, instanceOf(Map.class));
        final Map contextMap = (Map) context;
        assertThat(contextMap.get(OLD_CONTEXT_TLS_PROTOCOL_WHITE_LIST), equalTo(PROTOCOL_ALLOW_LIST));
        assertThat(contextMap.get(OLD_CONTEXT_TLS_PROTOCOL_BLACK_LIST), equalTo(PROTOCOL_DENY_LIST));
    }
    private LegacyConfiguredObject createNewVersionPortMock()
    {
        final LegacyConfiguredObject nextVersionPort = mock(LegacyConfiguredObject.class);
        final Map<String,String> newVersionContext = new HashMap<>();
        newVersionContext.put(NEW_CONTEXT_TLS_PROTOCOL_ALLOW_LIST, PROTOCOL_ALLOW_LIST);
        newVersionContext.put(NEW_CONTEXT_TLS_PROTOCOL_DENY_LIST, PROTOCOL_DENY_LIST);
        when(nextVersionPort.getAttribute(ATTRIBUTE_NAME)).thenReturn(PORT_NAME);
        when(nextVersionPort.getAttribute(ATTRIBUTE_CONTEXT)).thenReturn(newVersionContext);
        when(nextVersionPort.getContextValue(NEW_CONTEXT_TLS_PROTOCOL_ALLOW_LIST)).thenReturn(PROTOCOL_ALLOW_LIST);
        when(nextVersionPort.getContextValue(NEW_CONTEXT_TLS_PROTOCOL_DENY_LIST)).thenReturn(PROTOCOL_DENY_LIST);
        return nextVersionPort;
    }

}
