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

import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;

public class BrokerQueryServlet extends QueryServlet<Broker<?>>
{
    private static final long serialVersionUID = 1L;

    @Override
    protected Broker<?> getParent(final HttpServletRequest request, final ConfiguredObject<?> managedObject)
    {
        return managedObject instanceof Broker ? HttpManagementUtil.getBroker(request.getServletContext()) : null;
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
        return category;
    }

    @Override
    protected String getRequestedCategory(final HttpServletRequest request, final ConfiguredObject<?> managedObject)
    {
        List<String> pathInfoElements =
                HttpManagementUtil.getPathInfoElements(request.getServletPath(), request.getPathInfo());
        if (pathInfoElements.size() == 1)
        {
            return pathInfoElements.get(0);
        }
        return null;
    }

    @Override
    protected List<ConfiguredObject<?>> getAllObjects(final Broker<?> broker,
                                                      final Class<? extends ConfiguredObject> category,
                                                      final HttpServletRequest request)
    {
        if(category == Broker.class)
        {
            return Collections.<ConfiguredObject<?>>singletonList(broker);
        }
        else
        {
            final Model brokerModel = broker.getModel();

            List<Class<? extends ConfiguredObject>> hierarchy = new ArrayList<>();

            Class<? extends ConfiguredObject> element = category;
            while (element != null && element != Broker.class)
            {
                hierarchy.add(element);
                Class<? extends ConfiguredObject> parentType =
                        brokerModel.getParentType(element);
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
            Collection<ConfiguredObject<?>> parents = Collections.<ConfiguredObject<?>>singletonList(broker);
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
}
