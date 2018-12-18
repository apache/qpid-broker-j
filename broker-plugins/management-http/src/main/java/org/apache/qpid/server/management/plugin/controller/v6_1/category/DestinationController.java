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
package org.apache.qpid.server.management.plugin.controller.v6_1.category;

import static org.apache.qpid.server.management.plugin.ManagementException.createBadRequestManagementException;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.ConfiguredObject;

public class DestinationController extends LegacyCategoryController
{

    private static final String ALTERNATE_BINDING = "alternateBinding";

    DestinationController(final LegacyManagementController legacyManagementController,
                          final String name,
                          final String[] parentCategories,
                          final String defaultType,
                          final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              name,
              parentCategories,
              defaultType,
              typeControllers);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                              final List<String> path,
                                                              final Map<String, Object> attributes)
    {
        if (attributes.containsKey(LegacyDestination.ALTERNATE_EXCHANGE))
        {
            final Map<String, Object> converted = new LinkedHashMap<>(attributes);
            final String alternateExchange = (String) converted.remove(LegacyDestination.ALTERNATE_EXCHANGE);
            final LegacyConfiguredObject exchange = findExchange(root,
                                                                 path,
                                                                 object -> alternateExchange.equals(object.getAttribute(LegacyConfiguredObject.ID))
                                                                           || alternateExchange.equals(object.getAttribute(LegacyConfiguredObject.NAME)));
            if (exchange != null)
            {
                converted.put(ALTERNATE_BINDING,
                              Collections.singletonMap("destination",
                                                       exchange.getAttribute(LegacyConfiguredObject.NAME)));
            }
            else
            {
                throw createBadRequestManagementException(String.format("Cannot find alternate exchange '%s'",
                                                                        alternateExchange));
            }
            return converted;
        }
        return attributes;
    }

    private LegacyConfiguredObject findExchange(final ConfiguredObject<?> root,
                                                final List<String> path,
                                                final Predicate<LegacyConfiguredObject> predicate)
    {
        final Collection<String> hierarchy =
                getNextVersionManagementController().getCategoryHierarchy(root, ExchangeController.TYPE);

        final List<String> exchangePath = path.size() > hierarchy.size() - 1 ? path.subList(0, hierarchy.size() - 1) : path;
        final Object result = getNextVersionManagementController().get(root,
                                                                       ExchangeController.TYPE,
                                                                       exchangePath,
                                                                       Collections.emptyMap());
        if (result instanceof Collection)
        {
            final Collection<?> exchanges = (Collection<?>) result;
            return exchanges.stream()
                            .filter(LegacyConfiguredObject.class::isInstance)
                            .map(LegacyConfiguredObject.class::cast)
                            .filter(predicate)
                            .findFirst()
                            .orElse(null);
        }
        else
        {
            throw createBadRequestManagementException("Cannot find alternate exchange");
        }
    }

    public static class LegacyDestination extends GenericLegacyConfiguredObject
    {
        static final String ALTERNATE_EXCHANGE = "alternateExchange";

        LegacyDestination(final LegacyManagementController managementController,
                          final LegacyConfiguredObject nextVersionDestination,
                          final String category)
        {
            super(managementController, nextVersionDestination, category);
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return Stream.concat(super.getAttributeNames().stream(),
                                 Stream.of(ALTERNATE_EXCHANGE)).collect(Collectors.toSet());
        }

        @Override
        public Object getAttribute(final String name)
        {
            return getAttributeInternal(name, false);
        }


        @Override
        public Object getActualAttribute(final String name)
        {
            return getAttributeInternal(name, true);
        }

        protected Object getAttributeInternal(final String name, boolean isActual)
        {
            if (ALTERNATE_EXCHANGE.equals(name))
            {
                final Object altBinding = getAttribute(ALTERNATE_BINDING, isActual);
                if (altBinding instanceof AlternateBinding)
                {
                    final AlternateBinding alternateBinding = (AlternateBinding) altBinding;

                    final Collection<LegacyConfiguredObject> exchanges =
                            getNextVersionLegacyConfiguredObject().getParent(VirtualHostController.TYPE)
                                                                  .getChildren(ExchangeController.TYPE);

                    final LegacyConfiguredObject altExchange = exchanges.stream()
                                                                  .filter(e -> alternateBinding.getDestination()
                                                                                               .equals(e.getAttribute(NAME)))
                                                                  .findFirst()
                                                                  .orElse(null);
                    if (altExchange != null)
                    {
                        return getManagementController().convertFromNextVersion(altExchange);
                    }
                }
                return null;
            }
            return getAttribute(name, isActual);
        }

        Object getAttribute(String name, boolean isActual)
        {
            if (isActual)
            {
                return super.getActualAttribute(name);
            }
            return super.getAttribute(name);
        }

        @Override
        public boolean isSecureAttribute(final String name)
        {
            if (ALTERNATE_EXCHANGE.equals(name))
            {
                return super.isSecureAttribute(ALTERNATE_BINDING);
            }
            return super.isSecureAttribute(name);
        }

        @Override
        public boolean isOversizedAttribute(final String name)
        {
            if (ALTERNATE_EXCHANGE.equals(name))
            {
                return super.isOversizedAttribute(ALTERNATE_BINDING);
            }
            return super.isOversizedAttribute(name);
        }
    }
}
