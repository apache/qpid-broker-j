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
package org.apache.qpid.server.model;

import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public final class SystemConfigBootstrapModel extends Model
{

    public static final int MODEL_MAJOR_VERSION = 1;
    public static final int MODEL_MINOR_VERSION = 0;
    public static final String MODEL_VERSION = MODEL_MAJOR_VERSION + "." + MODEL_MINOR_VERSION;
    private static final Model MODEL_INSTANCE = new SystemConfigBootstrapModel();

    private final ConfiguredObjectTypeRegistry _typeRegistry;

    private Class<? extends ConfiguredObject> _rootCategory;
    private final ConfiguredObjectFactory _objectFactory;

    private SystemConfigBootstrapModel()
    {
        setRootCategory(SystemConfig.class);

        _objectFactory = new ConfiguredObjectFactoryImpl(this);
        _typeRegistry = new ConfiguredObjectTypeRegistry((new QpidServiceLoader()).instancesOf(ConfiguredObjectRegistration.class),
                                                         (new QpidServiceLoader()).instancesOf(ConfiguredObjectAttributeInjector.class),
                                                         getSupportedCategories(),
                                                         _objectFactory);
    }

    @Override
    public final ConfiguredObjectTypeRegistry getTypeRegistry()
    {
        return _typeRegistry;
    }


    public static Model getInstance()
    {
        return MODEL_INSTANCE;
    }

    @Override
    public Class<? extends ConfiguredObject> getRootCategory()
    {
        return _rootCategory;
    }

    @Override
    public Class<? extends ConfiguredObject> getParentType(final Class<? extends ConfiguredObject> child)
    {
        return null;
    }

    @Override
    public int getMajorVersion()
    {
        return MODEL_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion()
    {
        return MODEL_MINOR_VERSION;
    }

    @Override
    public ConfiguredObjectFactory getObjectFactory()
    {
        return _objectFactory;
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getChildTypes(Class<? extends ConfiguredObject> parent)
    {
        return Collections.<Class<? extends ConfiguredObject>>emptyList();
    }

    @Override
    public Collection<Class<? extends ConfiguredObject>> getSupportedCategories()
    {
        return Collections.unmodifiableSet(Collections.<Class<? extends ConfiguredObject>>singleton(SystemConfig.class));
    }

    private void setRootCategory(final Class<? extends ConfiguredObject> rootCategory)
    {
        _rootCategory = rootCategory;
    }


}
