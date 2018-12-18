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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.Binding;

public class ExchangeController extends DestinationController
{

    public static final String TYPE = "Exchange";

    ExchangeController(final LegacyManagementController legacyManagementController,
                       final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{VirtualHostController.TYPE},
              null,
              typeControllers);
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacyExchange(getManagementController(), object);
    }

    public static class LegacyExchange extends LegacyDestination
    {
        LegacyExchange(final LegacyManagementController managementController,
                       final LegacyConfiguredObject nextVersionQueue)
        {
            super(managementController, nextVersionQueue, ExchangeController.TYPE);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            if (BindingController.TYPE.equals(category))
            {
                Collection<Binding> bindings = (Collection<Binding>) getAttribute("bindings");
                if (bindings != null)
                {
                    Map<String, LegacyConfiguredObject> queues =
                            getNextVersionLegacyConfiguredObject().getParent(VirtualHostController.TYPE)
                                                                  .getChildren(QueueController.TYPE)
                                                                  .stream()
                                                                  .collect(Collectors.toMap(q -> (String) q.getAttribute(
                                                                          LegacyConfiguredObject.NAME),
                                                                                            q -> q));
                    return bindings.stream()
                                   .map(b -> new BindingController.LegacyBinding(getManagementController(),
                                                                             getNextVersionLegacyConfiguredObject(),
                                                                             queues.get(b.getName()),
                                                                             b.getBindingKey(),
                                                                             b.getArguments()))
                                   .collect(Collectors.toList());
                }
                return Collections.emptyList();
            }
            return super.getChildren(category);
        }
    }
}
