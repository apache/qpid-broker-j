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
package org.apache.qpid.server.management.plugin.controller.latest;

import static org.apache.qpid.server.management.plugin.ManagementException.createNotFoundManagementException;
import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.getCollectionMemberType;
import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.returnsCollectionOfConfiguredObjects;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.ControllerManagementResponse;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.Model;

public class LatestManagementControllerAdapter implements ManagementController
{
    private ManagementController _latestManagementController;

    public LatestManagementControllerAdapter(final ManagementController latestManagementController)
    {
        _latestManagementController = latestManagementController;
    }

    @Override
    public String getVersion()
    {
        return _latestManagementController.getVersion();
    }

    @Override
    public Collection<String> getCategories()
    {
        return _latestManagementController.getCategories();
    }

    @Override
    public String getCategoryMapping(final String category)
    {
        return _latestManagementController.getCategoryMapping(category);
    }

    @Override
    public String getCategory(final ConfiguredObject<?> managedObject)
    {
        return _latestManagementController.getCategory(managedObject);
    }

    @Override
    public Collection<String> getCategoryHierarchy(final ConfiguredObject<?> root, final String category)
    {
        return _latestManagementController.getCategoryHierarchy(root, category);
    }

    @Override
    public ManagementController getNextVersionManagementController()
    {
        return _latestManagementController;
    }

    @Override
    public ManagementResponse handleGet(final ManagementRequest request) throws ManagementException
    {
        ManagementResponse response = _latestManagementController.handleGet(request);
        return new ControllerManagementResponse(response.getType(),
                                                convertResponseObject(response.getBody()),
                                                response.getResponseCode(),
                                                response.getHeaders());
    }

    @Override
    public ManagementResponse handlePut(final ManagementRequest request) throws ManagementException
    {
        ManagementResponse response = _latestManagementController.handlePut(request);
        return new ControllerManagementResponse(response.getType(),
                                                convertResponseObject(response.getBody()),
                                                response.getResponseCode(),
                                                response.getHeaders());
    }

    @Override
    public ManagementResponse handlePost(final ManagementRequest request) throws ManagementException
    {
        ManagementResponse response = _latestManagementController.handlePost(request);
        return new ControllerManagementResponse(response.getType(),
                                                convertResponseObject(response.getBody()),
                                                response.getResponseCode(),
                                                response.getHeaders());
    }

    @Override
    public ManagementResponse handleDelete(final ManagementRequest request) throws ManagementException
    {
        ManagementResponse response = _latestManagementController.handleDelete(request);
        return new ControllerManagementResponse(response.getType(),
                                                convertResponseObject(response.getBody()),
                                                response.getResponseCode(),
                                                response.getHeaders());
    }


    @Override
    public Object get(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        Object result = _latestManagementController.get(root, category, path, parameters);
        return convertResponseObject(result);
    }


    @Override
    public Object createOrUpdate(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, Object> attributes,
                                 final boolean isPost) throws ManagementException
    {
        Object result = _latestManagementController.createOrUpdate(root, category, path, attributes, isPost);
        if (result instanceof ConfiguredObject)
        {
            return new LegacyConfiguredObjectObject((ConfiguredObject) result);
        }
        return null;
    }

    @Override
    public int delete(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        return _latestManagementController.delete(root, category, path, parameters);
    }

    @Override
    public ManagementResponse invoke(final ConfiguredObject<?> root,
                                     final String category,
                                     final List<String> path,
                                     final String operationName,
                                     final Map<String, Object> parameters,
                                     final boolean isPost,
                                     final boolean isSecureOrAllowedOnInsecureChannel) throws ManagementException
    {
        ManagementResponse response =
                _latestManagementController.invoke(root, category, path, operationName, parameters, isPost,
                                                   isSecureOrAllowedOnInsecureChannel);
        if (response.getType() == ResponseType.MODEL_OBJECT)
        {
            Object result = response.getBody();

            if (result instanceof ConfiguredObject)
            {
                result = new LegacyConfiguredObjectObject((ConfiguredObject<?>) result);
            }
            else if (result instanceof Collection)
            {
                result = ((Collection<?>) result).stream()
                                                 .map(o -> new LegacyConfiguredObjectObject((ConfiguredObject<?>) o))
                                                 .collect(Collectors.toSet());
            }
            return new ControllerManagementResponse(ResponseType.MODEL_OBJECT, result);
        }
        return response;
    }

    @Override
    public Object getPreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        return _latestManagementController.getPreferences(root, category, path, parameters);
    }

    @Override
    public void setPreferences(final ConfiguredObject<?> root,
                               final String category,
                               final List<String> path,
                               final Object preferences,
                               final Map<String, List<String>> parameters,
                               final boolean isPost) throws ManagementException
    {
        _latestManagementController.setPreferences(root, category, path, preferences, parameters, isPost);
    }

    @Override
    public int deletePreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        return _latestManagementController.delete(root, category, path, parameters);
    }

    @Override
    public Object formatConfiguredObject(final Object data,
                                         final Map<String, List<String>> parameters,
                                         final boolean isSecureOrAllowedOnInsecureChannel)
    {
        Object content = data;
        if (content instanceof LegacyConfiguredObjectObject)
        {

            content = ((LegacyConfiguredObjectObject) data).getConfiguredObject();
        }
        else if (data instanceof Collection)
        {
            content = ((Collection<?>) data).stream()
                                            .filter(o -> o instanceof LegacyConfiguredObjectObject)
                                            .map(LegacyConfiguredObjectObject.class::cast)
                                            .map(LegacyConfiguredObjectObject::getConfiguredObject)
                                            .collect(Collectors.toSet());
        }

        return _latestManagementController.formatConfiguredObject(content,
                                                                  parameters,
                                                                  isSecureOrAllowedOnInsecureChannel);
    }

    private Class<? extends ConfiguredObject> getRequestCategoryClass(final String categoryName,
                                                                      final Model model)
    {
        for (Class<? extends ConfiguredObject> category : model.getSupportedCategories())
        {
            if (category.getSimpleName().toLowerCase().equals(categoryName))
            {
                return category;
            }
        }
        throw createNotFoundManagementException(String.format("Category is not found for '%s'", categoryName));
    }

    private Object convertResponseObject(final Object result)
    {
        if (result instanceof ConfiguredObject)
        {
            return new LegacyConfiguredObjectObject((ConfiguredObject) result);
        }
        else if (result instanceof Collection)
        {
            return ((Collection<?>) result).stream().filter(o -> o instanceof ConfiguredObject)
                                           .map(ConfiguredObject.class::cast)
                                           .map(o -> new LegacyConfiguredObjectObject((ConfiguredObject<?>) o))
                                           .collect(Collectors.toSet());
        }
        return result;
    }

    private class LegacyConfiguredObjectObject implements LegacyConfiguredObject
    {
        private final Map<String, Object> _actualAttributes;
        private ConfiguredObject<?> _configuredObject;

        LegacyConfiguredObjectObject(final ConfiguredObject<?> configuredObject)
        {
            _configuredObject = configuredObject;
            _actualAttributes = configuredObject.getActualAttributes();
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return _configuredObject.getAttributeNames();
        }

        @Override
        public Object getAttribute(final String name)
        {
            return convertIntoLegacyIfRequired(name, _configuredObject.getAttribute(name));
        }

        @Override
        public Object getActualAttribute(final String name)
        {
            return _actualAttributes.get(name);
        }

        @Override
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            Class<? extends ConfiguredObject> categoryClass =
                    getRequestCategoryClass(category.toLowerCase(), _configuredObject.getModel());
            return _configuredObject.getChildren(categoryClass)
                                    .stream()
                                    .map(LegacyConfiguredObjectObject::new)
                                    .collect(Collectors.toSet());
        }

        @Override
        public String getCategory()
        {
            return _configuredObject.getCategoryClass().getSimpleName();
        }

        @Override
        @SuppressWarnings("unchecked")
        public ManagementResponse invoke(final String operationName,
                                         final Map<String, Object> parameters,
                                         final boolean isSecure)
        {
            try
            {
                final Model model = _configuredObject.getModel();
                final Map<String, ConfiguredObjectOperation<?>> availableOperations =
                        model.getTypeRegistry().getOperations(_configuredObject.getClass());
                final ConfiguredObjectOperation operation = availableOperations.get(operationName);
                if (operation == null)
                {
                    throw createNotFoundManagementException(String.format("No such operation as '%s' in '%s'",
                                                                          operationName,
                                                                          getCategory()));
                }

                Object returnValue = operation.perform(_configuredObject, parameters);

                final ResponseType responseType;
                if (ConfiguredObject.class.isAssignableFrom(operation.getReturnType()))
                {
                    returnValue = new LegacyConfiguredObjectObject((ConfiguredObject<?>) returnValue);
                    responseType = ResponseType.MODEL_OBJECT;
                }
                else if (returnsCollectionOfConfiguredObjects(operation))
                {
                    returnValue = ((Collection) returnValue).stream()
                                                            .map(o -> new LegacyConfiguredObjectObject((ConfiguredObject<?>) o))
                                                            .collect(Collectors.toSet());
                    responseType = ResponseType.MODEL_OBJECT;
                }
                else
                {
                    responseType = ResponseType.DATA;
                }
                return new ControllerManagementResponse(responseType, returnValue);
            }
            catch (RuntimeException e)
            {
                throw ManagementException.toManagementException(e,
                                                                getCategoryMapping(getCategory()),
                                                                Collections.emptyList());
            }
            catch (Error e)
            {
                throw ManagementException.handleError(e);
            }
        }

        @Override
        public LegacyConfiguredObject getNextVersionConfiguredObject()
        {
            return null;
        }

        @Override
        public LegacyConfiguredObject getParent(final String category)
        {
            ConfiguredObject<?> parent = _configuredObject.getParent();
            if (category != null && !parent.getCategoryClass().getSimpleName().equalsIgnoreCase(category))
            {
                throw new IllegalArgumentException(String.format(
                        "ConfiguredObject of category '%s' has no parent of category %s",
                        getCategory(),
                        category));
            }
            return new LegacyConfiguredObjectObject(parent);
        }

        @Override
        public boolean isSecureAttribute(final String name)
        {
            ConfiguredObjectAttribute<?, ?> objectAttribute = getConfiguredObjectAttribute(name);
            return objectAttribute != null && objectAttribute.isSecure();
        }

        @Override
        public boolean isOversizedAttribute(final String name)
        {
            ConfiguredObjectAttribute<?, ?> objectAttribute = getConfiguredObjectAttribute(name);
            return objectAttribute != null && objectAttribute.isOversized();
        }

        @Override
        public String getContextValue(final String contextKey)
        {
            return _configuredObject.getContextValue(String.class, contextKey);
        }

        @Override
        public Map<String, Object> getStatistics()
        {
            return _configuredObject.getStatistics();
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final LegacyConfiguredObjectObject object = (LegacyConfiguredObjectObject) o;
            return Objects.equals(_configuredObject, object._configuredObject);
        }

        @Override
        public int hashCode()
        {
            return _configuredObject.hashCode();
        }

        ConfiguredObject<?> getConfiguredObject()
        {
            return _configuredObject;
        }

        private Object convertIntoLegacyIfRequired(final String name, final Object value)
        {
            if (value != null)
            {
                ConfiguredObjectAttribute<?, ?> attribute = getConfiguredObjectAttribute(name);

                final Class<?> type = attribute.getType();
                final Type genericType = attribute.getGenericType();
                if (ConfiguredObject.class.isAssignableFrom(type))
                {
                    return new LegacyConfiguredObjectObject((ConfiguredObject<?>) value);
                }
                else if (Collection.class.isAssignableFrom(type)
                         && genericType instanceof ParameterizedType
                         && ConfiguredObject.class.isAssignableFrom(getCollectionMemberType((ParameterizedType) genericType)))
                {
                    Collection<?> collection = (Collection<?>) value;
                    return collection.stream()
                                     .filter(o -> o instanceof LegacyConfiguredObjectObject)
                                     .map(LegacyConfiguredObjectObject.class::cast)
                                     .map(co -> new LegacyConfiguredObjectObject((ConfiguredObject<?>) co))
                                     .collect(Collectors.toSet());
                }
            }
            return value;
        }


        private ConfiguredObjectAttribute<?, ?> getConfiguredObjectAttribute(final String name)
        {
            return _configuredObject.getModel()
                                    .getTypeRegistry()
                                    .getAttributeTypes(_configuredObject.getClass())
                                    .get(name);
        }
    }
}
