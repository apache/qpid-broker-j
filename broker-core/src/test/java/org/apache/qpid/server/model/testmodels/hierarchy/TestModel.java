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
import java.util.Set;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;

public class TestModel extends Model
{
    private static final Model INSTANCE = new TestModel();
    private Class<? extends ConfiguredObject>[] _supportedCategories =
            new Class[] {
                    TestCar.class,
                    TestEngine.class,
                    TestSensor.class
            };

    private final ConfiguredObjectFactory _objectFactory;
    private ConfiguredObjectTypeRegistry _registry;

    private TestModel()
    {
        this(null);
    }

    public TestModel(final ConfiguredObjectFactory objectFactory)
    {
        this(objectFactory, Collections.<ConfiguredObjectAttributeInjector>emptySet());
    }

    public TestModel(final ConfiguredObjectFactory objectFactory, ConfiguredObjectAttributeInjector injector)
    {
        this(objectFactory, Collections.singleton(injector));
    }

    public TestModel(final ConfiguredObjectFactory objectFactory, Set<ConfiguredObjectAttributeInjector> attributeInjectors)
    {
        _objectFactory = objectFactory == null ? new ConfiguredObjectFactoryImpl(this) : objectFactory;

        ConfiguredObjectRegistration configuredObjectRegistration = new ConfiguredObjectRegistrationImpl();

        _registry = new ConfiguredObjectTypeRegistry(Collections.singletonList(configuredObjectRegistration),
                                                     attributeInjectors,
                                                     Collections.EMPTY_LIST, _objectFactory);
    }


    @Override
    public Collection<Class<? extends ConfiguredObject>> getSupportedCategories()
    {
        return Arrays.asList(_supportedCategories);
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getChildTypes(final Class<? extends ConfiguredObject> parent)
    {
        if (TestCar.class.isAssignableFrom(parent))
        {
            return  Arrays.asList(TestEngine.class, TestInstrumentPanel.class);
        }
        else if (TestInstrumentPanel.class.isAssignableFrom(parent))
        {
            return Arrays.asList(TestGauge.class, TestSensor.class);
        }
        else
        {
            return Collections.emptySet();
        }
    }

    @Override
    public Class<? extends ConfiguredObject> getRootCategory()
    {
        return TestCar.class;
    }

    @Override
    public Class<? extends ConfiguredObject> getParentType(final Class<? extends ConfiguredObject> child)
    {
        if (TestEngine.class.isAssignableFrom(child) || TestInstrumentPanel.class.isAssignableFrom(child))
        {
            return TestCar.class;
        }
        else if (TestGauge.class.isAssignableFrom(child) || TestSensor.class.isAssignableFrom(child))
        {
            return TestInstrumentPanel.class;
        }
        else
        {
            return null;
        }
    }

    @Override
    public int getMajorVersion()
    {
        return 99;
    }

    @Override
    public int getMinorVersion()
    {
        return 99;
    }

    @Override
    public ConfiguredObjectFactory getObjectFactory()
    {
        return _objectFactory;
    }

    @Override
    public ConfiguredObjectTypeRegistry getTypeRegistry()
    {
        return _registry;
    }

    public static Model getInstance()
    {
        return INSTANCE;
    }
}
