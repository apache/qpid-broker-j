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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;

public class GenericLegacyConfiguredObject implements LegacyConfiguredObject
{
    private final LegacyManagementController _managementController;
    private final LegacyConfiguredObject _nextVersionLegacyConfiguredObject;
    private final String _category;

    public GenericLegacyConfiguredObject(final LegacyManagementController managementController,
                                         final LegacyConfiguredObject nextVersionLegacyConfiguredObject,
                                         final String category)
    {
        _nextVersionLegacyConfiguredObject = nextVersionLegacyConfiguredObject;
        _managementController = managementController;
        _category = category;
    }

    @Override
    public Collection<String> getAttributeNames()
    {
        return _nextVersionLegacyConfiguredObject.getAttributeNames();
    }

    @Override
    public Object getAttribute(final String name)
    {
        return convertLegacyConfiguredObjectIfRequired(_nextVersionLegacyConfiguredObject.getAttribute(name));
    }

    @Override
    public Object getActualAttribute(final String name)
    {
        return _nextVersionLegacyConfiguredObject.getActualAttribute(name);
    }

    @Override
    public Collection<LegacyConfiguredObject> getChildren(final String category)
    {
        final Collection<LegacyConfiguredObject> children =
                _nextVersionLegacyConfiguredObject.getChildren(category);
        if (children != null)
        {
            return children.stream().map(_managementController::convertFromNextVersion).collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    @Override
    public String getCategory()
    {
        return _category;
    }

    @Override
    public ManagementResponse invoke(final String operation,
                                     final Map<String, Object> parameters,
                                     final boolean isSecure)
    {
        ManagementResponse result = _nextVersionLegacyConfiguredObject.invoke(operation, parameters, isSecure);

        return convertLegacyConfiguredObjectIfRequired(result);
    }

    @Override
    public LegacyConfiguredObject getNextVersionConfiguredObject()
    {
        return _nextVersionLegacyConfiguredObject;
    }

    @Override
    public LegacyConfiguredObject getParent(final String category)
    {
        LegacyConfiguredObject parent = _nextVersionLegacyConfiguredObject.getParent(category);
        return _managementController.convertFromNextVersion(parent);
    }

    @Override
    public boolean isSecureAttribute(final String name)
    {
        return _nextVersionLegacyConfiguredObject.isSecureAttribute(name);
    }

    @Override
    public boolean isOversizedAttribute(final String name)
    {
        return _nextVersionLegacyConfiguredObject.isOversizedAttribute(name);
    }

    @Override
    public String getContextValue(final String contextKey)
    {
        return _nextVersionLegacyConfiguredObject.getContextValue(contextKey);
    }

    @Override
    public Map<String, Object> getStatistics()
    {
        return _nextVersionLegacyConfiguredObject.getStatistics();
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
        final GenericLegacyConfiguredObject
                that = (GenericLegacyConfiguredObject) o;
        return Objects.equals(_nextVersionLegacyConfiguredObject, that._nextVersionLegacyConfiguredObject);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_nextVersionLegacyConfiguredObject);
    }

    public LegacyManagementController getManagementController()
    {
        return _managementController;
    }

    protected LegacyConfiguredObject getNextVersionLegacyConfiguredObject()
    {
        return _nextVersionLegacyConfiguredObject;
    }

    private Object convertLegacyConfiguredObjectIfRequired(final Object value)
    {
        if (value instanceof LegacyConfiguredObject)
        {
            return _managementController.convertFromNextVersion((LegacyConfiguredObject) value);
        }
        else if (value instanceof Collection)
        {
            Collection<?> collection = (Collection<?>) value;
            if (collection.size() > 0 && collection.iterator().next() instanceof LegacyConfiguredObject)
            {
                return collection.stream()
                                 .filter(o -> o instanceof LegacyConfiguredObject)
                                 .map(LegacyConfiguredObject.class::cast)
                                 .map(_managementController::convertFromNextVersion)
                                 .collect(Collectors.toSet());
            }
        }
        return value;
    }

    private ManagementResponse convertLegacyConfiguredObjectIfRequired(final ManagementResponse response)
    {
        if (response.getType() == ResponseType.MODEL_OBJECT)
        {
            Object body = convertLegacyConfiguredObjectIfRequired(response.getBody());
            return new ControllerManagementResponse(response.getType(),
                                                    body,
                                                    response.getResponseCode(),
                                                    response.getHeaders());
        }
        return response;
    }
}
