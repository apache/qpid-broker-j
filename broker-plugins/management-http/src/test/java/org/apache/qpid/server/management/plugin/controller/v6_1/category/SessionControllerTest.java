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
package org.apache.qpid.server.management.plugin.controller.v6_1.category;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.ControllerManagementResponse;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.test.utils.UnitTestBase;

public class SessionControllerTest extends UnitTestBase
{
    private LegacyManagementController _legacyManagementController;
    private SessionController _sessionController;

    @Before
    public void setUp()
    {
        _legacyManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(_legacyManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
        _sessionController = new SessionController(_legacyManagementController, Collections.emptySet());
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final UUID sessionID = UUID.randomUUID();

        final LegacyConfiguredObject nextVersionSession = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject nextVersionConsumer = mock(LegacyConfiguredObject.class);

        when(nextVersionSession.getCategory()).thenReturn(SessionController.TYPE);
        when(nextVersionSession.getAttribute(LegacyConfiguredObject.ID)).thenReturn(sessionID);

        final ManagementResponse operationResult = new ControllerManagementResponse(ResponseType.MODEL_OBJECT,
                                                                                    Collections.singletonList(
                                                                                            nextVersionConsumer));
        when(nextVersionSession.invoke(eq("getConsumers"), eq(Collections.emptyMap()), eq(true))).thenReturn(
                operationResult);

        final LegacyConfiguredObject convertedConsumer = mock(LegacyConfiguredObject.class);
        when(_legacyManagementController.convertFromNextVersion(nextVersionConsumer)).thenReturn(convertedConsumer);

        final LegacyConfiguredObject convertedSession =
                _sessionController.convertNextVersionLegacyConfiguredObject(nextVersionSession);

        assertThat(convertedSession.getAttribute(LegacyConfiguredObject.ID), is(equalTo(sessionID)));

        final Collection<LegacyConfiguredObject> consumers = convertedSession.getChildren(ConsumerController.TYPE);
        assertThat(consumers, is(notNullValue()));
        assertThat(consumers.size(), is(equalTo(1)));
        assertThat(consumers.iterator().next(), is(equalTo(convertedConsumer)));
    }
}