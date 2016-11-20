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
package org.apache.qpid.server.model.testmodels.hierarchy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.store.preferences.NoopPreferenceStoreFactoryService;
import org.apache.qpid.server.store.preferences.PreferenceStore;

@ManagedObject( category = false,
                type = TestStandardCarImpl.TEST_STANDARD_CAR_TYPE,
                validChildTypes = "org.apache.qpid.server.model.testmodels.hierarchy.TestStandardCarImpl#getSupportedChildTypes()")
public class TestStandardCarImpl extends AbstractConfiguredObject<TestStandardCarImpl>
        implements TestStandardCar<TestStandardCarImpl>
{
    public static final String TEST_STANDARD_CAR_TYPE = "testpertrolcar";
    private final PreferenceStore _preferenceStore;

    @ManagedAttributeField
    private Colour _bodyColour;

    @ManagedAttributeField
    private Colour _interiorColour;

    @ManagedObjectFactoryConstructor
    public TestStandardCarImpl(final Map<String, Object> attributes)
    {
        this(attributes, TestModel.getInstance());
    }

    public TestStandardCarImpl(final Map<String, Object> attributes, Model model)
    {
        super(parentsMap(), attributes, newTaskExecutor(), model);
        _preferenceStore = new NoopPreferenceStoreFactoryService().createInstance(this, null);
    }


    private static CurrentThreadTaskExecutor newTaskExecutor()
    {
        CurrentThreadTaskExecutor currentThreadTaskExecutor = new CurrentThreadTaskExecutor();
        currentThreadTaskExecutor.start();
        return currentThreadTaskExecutor;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                             final Map<String, Object> attributes,
                                                                             final ConfiguredObject... otherParents)
    {
        return getObjectFactory().createAsync(childClass, attributes, this);
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    @SuppressWarnings("unused")
    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        Collection<String> types = Arrays.asList(TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE, TestHybridEngineImpl.TEST_HYBRID_ENGINE_TYPE);
        return Collections.singletonMap(TestEngine.class.getSimpleName(), types);
    }

    @Override
    public Door openDoor(final Door door)
    {
        return door;
    }

    @Override
    public Colour getBodyColour()
    {
        return _bodyColour;
    }

    @Override
    public Colour getInteriorColour()
    {
        return _interiorColour;
    }

    @Override
    public UserPreferences createUserPreferences(final ConfiguredObject<?> object)
    {
        return new UserPreferencesImpl(getTaskExecutor(), object, _preferenceStore, Collections.<Preference>emptySet());
    }
}
