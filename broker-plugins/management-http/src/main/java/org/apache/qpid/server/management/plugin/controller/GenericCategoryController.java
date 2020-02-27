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
package org.apache.qpid.server.management.plugin.controller;

import static org.apache.qpid.server.management.plugin.ManagementException.createBadRequestManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createInternalServerErrorManagementException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.latest.LatestManagementControllerAdapter;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;

public abstract class GenericCategoryController implements CategoryController
{
    private final String _name;
    private final LegacyManagementController _managementController;
    private final ManagementController _nextVersionManagementController;
    private final String _defaultType;
    private final Map<String, TypeController> _typeControllers;
    private final Map<String, TypeController> _nextVersionTypeControllers;

    protected GenericCategoryController(final LegacyManagementController managementController,
                                        final ManagementController nextVersionManagementController,
                                        final String name,
                                        final String defaultType,
                                        final Set<TypeController> typeControllers)
    {
        _name = name;
        _managementController = managementController;
        boolean isNextLatest = nextVersionManagementController != null &&
                               BrokerModel.MODEL_VERSION.equalsIgnoreCase(nextVersionManagementController.getVersion());
        _nextVersionManagementController = isNextLatest ?
                new LatestManagementControllerAdapter(nextVersionManagementController)
                : nextVersionManagementController;
        _defaultType = defaultType;
        _typeControllers = typeControllers.stream().collect(Collectors.toMap(TypeController::getTypeName, c -> c));
        _nextVersionTypeControllers =
                typeControllers.stream().collect(Collectors.toMap(TypeController::getNextVersionTypeName, c -> c));
    }

    @Override
    public String getCategory()
    {
        return _name;
    }

    @Override
    public String getNextVersionCategory()
    {
        return _name;
    }

    @Override
    public String getDefaultType()
    {
        return _defaultType;
    }

    @Override
    public LegacyManagementController getManagementController()
    {
        return _managementController;
    }

    @Override
    public Object get(final ConfiguredObject<?> root,
                      final List<String> path,
                      final Map<String, List<String>> parameters)
            throws ManagementException
    {
        final Object content =
                _nextVersionManagementController.get(root,
                                                     getNextVersionCategory(),
                                                     path,
                                                     convertQueryParametersToNextVersion(parameters));
        return convert(content);
    }

    @Override
    public LegacyConfiguredObject createOrUpdate(ConfiguredObject<?> root,
                                                 List<String> path,
                                                 Map<String, Object> attributes,
                                                 boolean isPost) throws ManagementException
    {
        final Map<String, Object> body = convertAttributesToNextVersion(root, path, attributes);
        final Object configuredObject =
                _nextVersionManagementController.createOrUpdate(root, getNextVersionCategory(), path, body, isPost);
        if (configuredObject instanceof LegacyConfiguredObject)
        {
            LegacyConfiguredObject object = (LegacyConfiguredObject) configuredObject;
            return convertFromNextVersion(object);
        }
        return null;
    }

    @Override
    public LegacyConfiguredObject convertFromNextVersion(final LegacyConfiguredObject object)
    {
        TypeController controller = getTypeController(object);
        if (controller != null)
        {
            return controller.convertFromNextVersion(object);
        }
        return convertNextVersionLegacyConfiguredObject(object);
    }

    @Override
    public int delete(final ConfiguredObject<?> root,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        return _nextVersionManagementController.delete(root,
                                                       getNextVersionCategory(),
                                                       path,
                                                       convertQueryParametersToNextVersion(parameters));
    }

    @Override
    public ManagementResponse invoke(final ConfiguredObject<?> root,
                                     final List<String> path,
                                     final String operation,
                                     final Map<String, Object> parameters,
                                     final boolean isPost,
                                     final boolean isSecure) throws ManagementException
    {

        Object result = get(root, path, Collections.emptyMap());
        if (result instanceof LegacyConfiguredObject)
        {
            final LegacyConfiguredObject legacyConfiguredObject = (LegacyConfiguredObject) result;
            return legacyConfiguredObject.invoke(operation, parameters, isSecure);
        }
        else
        {
            throw createBadRequestManagementException(String.format("Configured object %s/%s is not found",
                                                                    getManagementController().getCategoryMapping(
                                                                            getCategory()),
                                                                    String.join("/", path)));
        }
    }

    @Override
    public Object getPreferences(final ConfiguredObject<?> root,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        return _nextVersionManagementController.getPreferences(root,
                                                               getNextVersionCategory(),
                                                               path,
                                                               parameters);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setPreferences(final ConfiguredObject<?> root,
                               final List<String> path,
                               final Object preferences,
                               final Map<String, List<String>> parameters,
                               final boolean isPost) throws ManagementException
    {
        _nextVersionManagementController.setPreferences(root,
                                                        getNextVersionCategory(),
                                                        path,
                                                        preferences,
                                                        parameters,
                                                        isPost);
    }

    @Override
    public int deletePreferences(final ConfiguredObject<?> root,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        return _nextVersionManagementController.deletePreferences(root,
                                                                  getNextVersionCategory(),
                                                                  path,
                                                                  parameters);
    }

    protected abstract LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object);

    protected Map<String, List<String>> convertQueryParametersToNextVersion(final Map<String, List<String>> parameters)
    {
        return parameters;
    }

    protected ManagementController getNextVersionManagementController()
    {
        return _nextVersionManagementController;
    }

    protected Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                                 final List<String> path,
                                                                 final Map<String, Object> attributes)
    {
        TypeController typeController = getTypeController(attributes);
        if (typeController != null)
        {
            return typeController.convertAttributesToNextVersion(root, path, attributes);
        }
        return attributes;
    }


    private Object convert(final Object content)
    {
        if (content instanceof LegacyConfiguredObject)
        {
            return convertFromNextVersion((LegacyConfiguredObject) content);
        }
        else if (content instanceof Collection)
        {
            final Collection<?> items = (Collection<?>) content;
            return items.stream()
                        .filter(LegacyConfiguredObject.class::isInstance)
                        .map(LegacyConfiguredObject.class::cast)
                        .map(this::convertFromNextVersion)
                        .collect(Collectors.toList());
        }
        else
        {
            throw createInternalServerErrorManagementException("Unexpected data format from next version");
        }
    }

    private TypeController getTypeController(final Map<String, Object> attributes)
    {
        String type = (String) attributes.get(LegacyConfiguredObject.TYPE);
        if (type == null)
        {
            type = getDefaultType();
        }
        if (type != null)
        {
            return _typeControllers.get(type);
        }
        return null;
    }

    protected TypeController getTypeController(final LegacyConfiguredObject object)
    {
        String type = (String) object.getAttribute(LegacyConfiguredObject.TYPE);
        return _nextVersionTypeControllers.get(type);
    }
}
