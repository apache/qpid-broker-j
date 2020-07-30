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

package org.apache.qpid.server.logging.logback.validator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.plugin.ConfiguredObjectTypeFactory;
import org.apache.qpid.server.store.ConfiguredObjectDependency;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.UnresolvedConfiguredObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class TestConfiguredObjectTypeFactory implements ConfiguredObjectTypeFactory<TestConfiguredObject>
{
    @Override
    public Class<? super TestConfiguredObject> getCategoryClass()
    {
        return TestConfiguredObject.class;
    }

    @Override
    public TestConfiguredObject create(ConfiguredObjectFactory factory, Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        return new TestConfiguredObject(parent, attributes);
    }

    @Override
    public ListenableFuture<TestConfiguredObject> createAsync(ConfiguredObjectFactory factory, Map<String, Object> attributes, ConfiguredObject<?> parent)
    {
        final SettableFuture<TestConfiguredObject> returnVal = SettableFuture.create();
        returnVal.set(create(factory, attributes, parent));
        return returnVal;
    }

    @Override
    public UnresolvedConfiguredObject<TestConfiguredObject> recover(final ConfiguredObjectFactory factory, final ConfiguredObjectRecord record, final ConfiguredObject<?> parent)
    {
        return new UnresolvedConfiguredObject<TestConfiguredObject>()
        {
            @Override
            public ConfiguredObject<?> getParent()
            {
                return parent;
            }

            @Override
            public Collection<ConfiguredObjectDependency<?>> getUnresolvedDependencies()
            {
                return Collections.emptyList();
            }

            @Override
            public TestConfiguredObject resolve()
            {
                return create(factory, record.getAttributes(), parent).withId(record.getId());
            }
        };
    }

    @Override
    public String getType()
    {
        return null;
    }
}
