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

package org.apache.qpid.server.management.plugin.controller.v7_0.category;

import java.util.Collection;
import java.util.Map;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;

public class ContainerDecorator implements LegacyConfiguredObject
{
    private static final String MODEL_VERSION = "modelVersion";
    private final GenericLegacyConfiguredObject _original;

    public ContainerDecorator(final GenericLegacyConfiguredObject original)
    {
        _original = original;
    }

    @Override
    public Collection<String> getAttributeNames()
    {
        return _original.getAttributeNames();
    }

    @Override
    public Object getAttribute(final String name)
    {
        if (MODEL_VERSION.equals(name))
        {
            return _original.getManagementController().getVersion();
        }
        return _original.getAttribute(name);
    }

    @Override
    public Map<String, Object> getStatistics()
    {
        return _original.getStatistics();
    }

    @Override
    public Object getActualAttribute(final String name)
    {
        if (MODEL_VERSION.equals(name))
        {
            return _original.getManagementController().getVersion();
        }
        return _original.getActualAttribute(name);
    }

    @Override
    public boolean isSecureAttribute(final String name)
    {
        return _original.isSecureAttribute(name);
    }

    @Override
    public boolean isOversizedAttribute(final String name)
    {
        return _original.isOversizedAttribute(name);
    }

    @Override
    public String getCategory()
    {
        return _original.getCategory();
    }

    @Override
    public Collection<LegacyConfiguredObject> getChildren(final String category)
    {
        return _original.getChildren(category);
    }

    @Override
    public LegacyConfiguredObject getParent(final String category)
    {
        if (LegacyCategoryControllerFactory.CATEGORY_BROKER.equals(getCategory())
            && LegacyCategoryControllerFactory.CATEGORY_SYSTEM_CONFIG.equals(category))
        {
            LegacyConfiguredObject nextVersionParent = _original.getNextVersionConfiguredObject().getParent(category);
            return new GenericLegacyConfiguredObject(_original.getManagementController(),
                                                     nextVersionParent,
                                                     category);
        }
        return _original.getParent(category);
    }

    @Override
    public String getContextValue(final String contextKey)
    {
        return _original.getContextValue(contextKey);
    }

    @Override
    public ManagementResponse invoke(final String operation,
                                     final Map<String, Object> parameters,
                                     final boolean isSecure)
    {
        return _original.invoke(operation, parameters, isSecure);
    }

    @Override
    public LegacyConfiguredObject getNextVersionConfiguredObject()
    {
        return _original.getNextVersionConfiguredObject();
    }
}
