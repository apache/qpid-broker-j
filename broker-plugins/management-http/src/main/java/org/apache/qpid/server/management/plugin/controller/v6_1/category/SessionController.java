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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;

public class SessionController extends LegacyCategoryController
{
    public static final String TYPE = "Session";

    SessionController(final LegacyManagementController legacyManagementController,
                      final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{LegacyCategoryControllerFactory.CATEGORY_CONNECTION},
              null,
              typeControllers);
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacySession(getManagementController(), object);
    }

    public static class LegacySession extends GenericLegacyConfiguredObject
    {
        LegacySession(final LegacyManagementController managementController,
                      final LegacyConfiguredObject nextVersionLegacyConfiguredObject)
        {
            super(managementController, nextVersionLegacyConfiguredObject, SessionController.TYPE);
        }

        @Override
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            if (ConsumerController.TYPE.equalsIgnoreCase(category))
            {
                final LegacyConfiguredObject nextVersionSession = getNextVersionLegacyConfiguredObject();
                final ManagementResponse result =
                        nextVersionSession.invoke("getConsumers", Collections.emptyMap(), true);
                if (result != null && result.getResponseCode() == 200  && result.getType() == ResponseType.MODEL_OBJECT)
                {
                    final Object objects = result.getBody();
                    if (objects instanceof Collection)
                    {
                        return ((Collection<?>) objects).stream().filter(o -> o instanceof LegacyConfiguredObject)
                                                        .map(o -> (LegacyConfiguredObject)o)
                                                        .map(o -> getManagementController().convertFromNextVersion(o))
                                                        .collect(Collectors.toList());
                    }
                }
                throw ManagementException.createInternalServerErrorManagementException(
                        "Unexpected result of performing operation Session#getConsumers()");
            }
            return super.getChildren(category);
        }
    }
}
