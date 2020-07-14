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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;

import java.util.Collection;
import java.util.Collections;

public class TestModel extends Model
{
    public static final TestModel MODEL = new TestModel().init();

    private static final ConfiguredObjectRegistration REGISTRATION = new ConfiguredObjectRegistration()
    {
        @Override
        public Collection<Class<? extends ConfiguredObject>> getConfiguredObjectClasses()
        {
            return Collections.singleton(TestConfiguredObject.class);
        }

        @Override
        public String getType()
        {
            return null;
        }
    };

    private TestConfiguredObjectFactory _factory;

    private ConfiguredObjectTypeRegistry _registry;

    private TestModel()
    {
        super();
    }

    private TestModel init()
    {
        _factory = new TestConfiguredObjectFactory(this);
        _registry = new ConfiguredObjectTypeRegistry(
                Collections.singleton(REGISTRATION),
                Collections.emptySet(),
                Collections.singleton(TestConfiguredObject.class),
                _factory);
        return this;
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getSupportedCategories()
    {
        return Collections.singletonList(TestConfiguredObject.class);
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getChildTypes(Class<? extends ConfiguredObject> parent)
    {
        return Collections.emptyList();
    }

    @Override
    public Class<? extends ConfiguredObject> getRootCategory()
    {
        return TestConfiguredObject.class;
    }

    @Override
    public Class<? extends ConfiguredObject> getParentType(Class<? extends ConfiguredObject> child)
    {
        return null;
    }

    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    @Override
    public int getMinorVersion()
    {
        return 1;
    }

    @Override
    public ConfiguredObjectFactory getObjectFactory()
    {
        return _factory;
    }

    @Override
    public ConfiguredObjectTypeRegistry getTypeRegistry()
    {
        return _registry;
    }
}
