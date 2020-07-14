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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.plugin.ConfiguredObjectTypeFactory;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.UnresolvedConfiguredObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class TestConfiguredObjectFactory implements ConfiguredObjectFactory
{
    private static final TestConfiguredObjectTypeFactory _FACTORY = new TestConfiguredObjectTypeFactory();

    private final Model _model;

    public TestConfiguredObjectFactory(Model model)
    {
        _model = model;
    }

    @Override
    public <X extends ConfiguredObject<X>> UnresolvedConfiguredObject<X> recover(ConfiguredObjectRecord record, ConfiguredObject<?> parent)
    {
        if (!TestConfiguredObject.TYPE.equals(record.getType()))
        {
            return null;
        }
        return (UnresolvedConfiguredObject<X>) _FACTORY.recover(this, record, parent);
    }

    @Override
    public <X extends ConfiguredObject<X>> X create(Class<X> clazz, Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        if (TestConfiguredObject.class.equals(clazz))
        {
            return (X) _FACTORY.create(this, attributes, parent);
        }
        return null;
    }

    @Override
    public <X extends ConfiguredObject<X>> ListenableFuture<X> createAsync(Class<X> clazz, Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        final SettableFuture<X> returnVal = SettableFuture.create();
        returnVal.set(create(clazz, attributes, parent));
        return returnVal;
    }

    @Override
    public <X extends ConfiguredObject<X>> ConfiguredObjectTypeFactory<X> getConfiguredObjectTypeFactory(String category, String type)
    {
        if (TestConfiguredObject.class.getSimpleName().equals(category) && TestConfiguredObject.TYPE.equals(type))
        {
            return (ConfiguredObjectTypeFactory<X>) new TestConfiguredObjectTypeFactory();
        }
        return null;
    }

    @Override
    public Collection<String> getSupportedTypes(Class<? extends ConfiguredObject> category)
    {
        if (TestConfiguredObject.class.equals(category))
        {
            return Collections.singleton(TestConfiguredObject.TYPE);
        }
        return Collections.emptyList();
    }

    @Override
    public Model getModel()
    {
        return _model;
    }
}
