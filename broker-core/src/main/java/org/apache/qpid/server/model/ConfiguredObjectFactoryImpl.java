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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.plugin.ConfiguredObjectTypeFactory;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.UnresolvedConfiguredObject;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectFactoryImpl implements ConfiguredObjectFactory
{
    private final Map<String, String> _defaultTypes = new HashMap<>();
    private final Map<String, Map<String, ConfiguredObjectTypeFactory>> _allFactories = new HashMap<>();
    private final Map<String, Collection<String>> _supportedTypes = new HashMap<>();

    private final Model _model;

    public ConfiguredObjectFactoryImpl(Model model)
    {
        _model = model;
        QpidServiceLoader serviceLoader =
                new QpidServiceLoader();
        Iterable<ConfiguredObjectTypeFactory> allFactories =
                serviceLoader.instancesOf(ConfiguredObjectTypeFactory.class);
        for (ConfiguredObjectTypeFactory factory : allFactories)
        {
            final Class<? extends ConfiguredObject> categoryClass = factory.getCategoryClass();
            final String categoryName = categoryClass.getSimpleName();

            Map<String, ConfiguredObjectTypeFactory> categoryFactories = _allFactories.get(categoryName);
            if (categoryFactories == null)
            {
                categoryFactories = new HashMap<>();
                _allFactories.put(categoryName, categoryFactories);
                _supportedTypes.put(categoryName, new ArrayList<>());
                ManagedObject annotation = categoryClass.getAnnotation(ManagedObject.class);
                if (annotation != null && !"".equals(annotation.defaultType()))
                {
                    _defaultTypes.put(categoryName, annotation.defaultType());
                }
                else
                {
                    _defaultTypes.put(categoryName, categoryName);
                }

            }
            if (categoryFactories.put(factory.getType(), factory) != null)
            {
                throw new ServerScopedRuntimeException(
                        "Misconfiguration - there is more than one factory defined for class " + categoryName
                        + " with type " + factory.getType());
            }
            if (factory.getType() != null)
            {
                _supportedTypes.get(categoryName).add(factory.getType());
            }
        }
    }

    @Override
    public <X extends ConfiguredObject<X>> UnresolvedConfiguredObject<X> recover(ConfiguredObjectRecord record,
                                                                                 ConfiguredObject<?> parent)
    {
        String category = record.getType();


        String type = (String) record.getAttributes().get(ConfiguredObject.TYPE);

        if(type == null || "".equals(type))
        {
            type = getOnlyValidChildTypeIfKnown(parent, category);
        }

        ConfiguredObjectTypeFactory<X> factory = getConfiguredObjectTypeFactory(category, type);

        if(factory == null)
        {
            throw new NoFactoryForTypeException(category, type);
        }

        return factory.recover(this, record, parent);
    }

    private String getOnlyValidChildTypeIfKnown(final ConfiguredObject<?> parent, final String category)
    {
        if(parent != null)
        {
            final Collection<String> validChildTypes =
                    _model.getTypeRegistry().getValidChildTypes(parent.getTypeClass(), category);
            if (validChildTypes != null && validChildTypes.size() == 1)
            {
                return validChildTypes.iterator().next();
            }
        }
        return null;

    }

    @Override
    public <X extends ConfiguredObject<X>> X create(Class<X> clazz,
                                                    final Map<String, Object> attributes,
                                                    final ConfiguredObject<?> parent)
    {
        ConfiguredObjectTypeFactory<X> factory = getConfiguredObjectTypeFactory(clazz, attributes, parent);

        return factory.create(this, attributes, parent);
    }


    @Override
    public <X extends ConfiguredObject<X>> ListenableFuture<X> createAsync(Class<X> clazz,
                                                    final Map<String, Object> attributes,
                                                    final ConfiguredObject<?> parent)
    {
        ConfiguredObjectTypeFactory<X> factory = getConfiguredObjectTypeFactory(clazz, attributes, parent);

        return factory.createAsync(this, attributes, parent);
    }

    private <X extends ConfiguredObject<X>> ConfiguredObjectTypeFactory<X> getConfiguredObjectTypeFactory(final Class<X> categoryClass,
                                                                                                          Map<String, Object> attributes,
                                                                                                          ConfiguredObject<?> parent)
    {
        final String category = categoryClass.getSimpleName();
        Map<String, ConfiguredObjectTypeFactory> categoryFactories = _allFactories.get(category);
        if(categoryFactories == null)
        {
            throw new NoFactoryForCategoryException(category);
        }
        String type = (String) attributes.get(ConfiguredObject.TYPE);

        ConfiguredObjectTypeFactory<X> factory;

        if(type != null)
        {
            factory = getConfiguredObjectTypeFactory(category, type);
            if(factory == null)
            {
                throw new NoFactoryForTypeException(category, type);
            }
        }
        else
        {
            factory = getConfiguredObjectTypeFactory(category, getOnlyValidChildTypeIfKnown(parent, category));
        }
        return factory;
    }

    @Override
    public <X extends ConfiguredObject<X>> ConfiguredObjectTypeFactory<X> getConfiguredObjectTypeFactory(final String category,
                                                                                                         final String type)
    {
        Map<String, ConfiguredObjectTypeFactory> categoryFactories = _allFactories.get(category);
        if(categoryFactories == null)
        {
            throw new NoFactoryForCategoryException(category);
        }
        ConfiguredObjectTypeFactory factory = categoryFactories.get(type);
        if(factory == null)
        {
            if(type == null || "".equals(type.trim()))
            {
                factory = categoryFactories.get(_defaultTypes.get(category));
            }
            if(factory == null)
            {
                throw new NoFactoryForTypeException(category, type);
            }
        }
        return factory;
    }

    @Override
    public Collection<String> getSupportedTypes(Class<? extends ConfiguredObject> category)
    {
        return Collections.unmodifiableCollection(_supportedTypes.get(category.getSimpleName()));
    }


    @Override
    public Model getModel()
    {
        return _model;
    }

}
