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
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;

public class VirtualHostQueryServlet extends QueryServlet<VirtualHost<?>>
{
    private static final long serialVersionUID = 1L;

    @Override
    protected VirtualHost<?> getParent(final HttpServletRequest request, final ConfiguredObject<?> managedObject)
    {
        final List<String>
                path = HttpManagementUtil.getPathInfoElements(request.getServletPath(), request.getPathInfo());
        if(managedObject instanceof Broker)
        {
            final Broker<?> broker = HttpManagementUtil.getBroker(request.getServletContext());
            if (path.size() == 3)
            {
                VirtualHostNode<?> vhn = broker.getChildByName(VirtualHostNode.class, path.get(0));
                if (vhn != null)
                {
                    return vhn.getChildByName(VirtualHost.class, path.get(1));
                }
            }
        }
        else if(managedObject instanceof VirtualHost)
        {
            if(path.size() == 1)
            {
                return (VirtualHost<?>)managedObject;
            }
        }
        return null;
    }

    @Override
    protected Class<? extends ConfiguredObject> getSupportedCategory(final String categoryName,
                                                                     final Model brokerModel)
    {

        Class<? extends ConfiguredObject> category = null;
        for(Class<? extends ConfiguredObject> supportedCategory : brokerModel.getSupportedCategories())
        {
            if(categoryName.equalsIgnoreCase(supportedCategory.getSimpleName()))
            {
                category = supportedCategory;
                break;
            }
        }
        final Collection<Class<? extends ConfiguredObject>> ancestors = brokerModel.getAncestorCategories(category);
        if(category == VirtualHost.class
                || category == Connection.class
                || ancestors.contains(VirtualHost.class)
                || ancestors.contains(Connection.class))
        {
            return category;
        }
        else
        {
            return null;
        }
    }

    @Override
    protected String getRequestedCategory(final HttpServletRequest request, final ConfiguredObject<?> managedObject)
    {
        List<String> pathInfoElements =
                HttpManagementUtil.getPathInfoElements(request.getServletPath(), request.getPathInfo());
        if (managedObject instanceof Broker && pathInfoElements.size() == 3)
        {
            return pathInfoElements.get(2);
        }
        else if(managedObject instanceof VirtualHost && pathInfoElements.size() == 1)
        {
            return pathInfoElements.get(0);
        }
        return null;
    }

    @Override
    protected List<ConfiguredObject<?>> getAllObjects(final VirtualHost<?> virtualHost,
                                                      final Class<? extends ConfiguredObject> category,
                                                      final HttpServletRequest request)
    {
        final Model model = virtualHost.getModel();
        if (category == VirtualHost.class)
        {
            return List.of(virtualHost);
        }
        else if (model.getAncestorCategories(category).contains(VirtualHost.class))
        {

            List<Class<? extends ConfiguredObject>> hierarchy = new ArrayList<>();

            Class<? extends ConfiguredObject> element = category;
            while (element != null && element != VirtualHost.class)
            {
                hierarchy.add(element);
                final Class<? extends ConfiguredObject> parentType =
                        model.getParentType(element);
                if(parentType == null)
                {
                    break;
                }
                else
                {
                    element = parentType;
                }
            }
            Collections.reverse(hierarchy);
            Collection<ConfiguredObject<?>> parents = List.of(virtualHost);
            return getObjects(hierarchy, parents);
        }
        else
        {
            List<ConfiguredObject<?>> parents = new ArrayList<>();
            parents.addAll(virtualHost.getConnections());

            if(category == Connection.class)
            {
                return parents;
            }
            else
            {
                List<Class<? extends ConfiguredObject>> hierarchy = new ArrayList<>();

                Class<? extends ConfiguredObject> element = category;
                while (element != null && element != Connection.class)
                {
                    hierarchy.add(element);
                    final Class<? extends ConfiguredObject> parentType =
                            model.getParentType(element);
                    if (parentType == null)
                    {
                        break;
                    }
                    else
                    {
                        element = parentType;
                    }
                }
                Collections.reverse(hierarchy);

                return getObjects(hierarchy, parents);
            }
        }
    }

    private List<ConfiguredObject<?>> getObjects(final List<Class<? extends ConfiguredObject>> hierarchy,
                                                 Collection<ConfiguredObject<?>> parents)
    {
        Collection<ConfiguredObject<?>> children = Collections.emptyList();
        for(Class<? extends ConfiguredObject> childClass : hierarchy)
        {
            children = new HashSet<>();
            for(ConfiguredObject<?> parent : parents)
            {
                children.addAll((Collection<? extends ConfiguredObject<?>>) parent.getChildren(childClass)) ;
            }
            parents = children;
        }

        return new ArrayList<>(children);
    }
}
