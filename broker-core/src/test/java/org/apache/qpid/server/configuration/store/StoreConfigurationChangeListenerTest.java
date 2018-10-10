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
package org.apache.qpid.server.configuration.store;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class StoreConfigurationChangeListenerTest extends UnitTestBase
{
    private DurableConfigurationStore _store;
    private StoreConfigurationChangeListener _listener;

    @Before
    public void setUp() throws Exception
    {
        _store = mock(DurableConfigurationStore.class);
        _listener = new StoreConfigurationChangeListener(_store);
    }

    @Test
    public void testStateChanged()
    {
        notifyBrokerStarted();
        UUID id = UUID.randomUUID();
        ConfiguredObject object = mock(VirtualHost.class);
        when(object.isDurable()).thenReturn(true);
        when(object.getId()).thenReturn(id);
        ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(object.asObjectRecord()).thenReturn(record);
        _listener.stateChanged(object, State.ACTIVE, State.DELETED);
        verify(_store).remove(record);
    }

    @Test
    public void testChildAdded()
    {
        notifyBrokerStarted();
        Broker broker = mock(Broker.class);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.isDurable()).thenReturn(true);
        VirtualHost child = mock(VirtualHost.class);
        when(child.getCategoryClass()).thenReturn(VirtualHost.class);
        Model model = mock(Model.class);
        when(model.getChildTypes(any(Class.class))).thenReturn(Collections.<Class<? extends ConfiguredObject>>emptyList());
        when(model.getParentType(eq(VirtualHost.class))).thenReturn((Class)Broker.class);
        when(child.getModel()).thenReturn(model);
        when(child.isDurable()).thenReturn(true);
        final ConfiguredObjectRecord childRecord = mock(ConfiguredObjectRecord.class);
        when(child.asObjectRecord()).thenReturn(childRecord);
        _listener.childAdded(broker, child);
        verify(_store).update(eq(true), eq(childRecord));
    }

    @Test
    public void testAttributeSet()
    {
        notifyBrokerStarted();
        Broker broker = mock(Broker.class);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.isDurable()).thenReturn(true);
        final ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(broker.asObjectRecord()).thenReturn(record);
        _listener.attributeSet(broker, Broker.DESCRIPTION, null, "test description");
        verify(_store).update(eq(false), eq(record));
    }

    @Test
    public void testChildAddedWhereParentManagesChildStorage()
    {
        notifyBrokerStarted();

        VirtualHostNode<?> object = mock(VirtualHostNode.class);
        when(object.managesChildStorage()).thenReturn(true);
        VirtualHost<?> virtualHost = mock(VirtualHost.class);
        _listener.childAdded(object, virtualHost);
        verifyNoMoreInteractions(_store);
    }

    private void notifyBrokerStarted()
    {
        Broker broker = mock(Broker.class);
        _listener.stateChanged(broker, State.UNINITIALIZED, State.ACTIVE);
    }
}
