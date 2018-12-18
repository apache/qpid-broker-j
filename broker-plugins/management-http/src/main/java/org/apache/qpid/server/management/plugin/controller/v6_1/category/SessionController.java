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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
        @SuppressWarnings("unchecked")
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            if (ConsumerController.TYPE.equalsIgnoreCase(category))
            {
                final LegacyConfiguredObject nextVersionSession = getNextVersionLegacyConfiguredObject();
                final LegacyConfiguredObject connection =
                        nextVersionSession.getParent(LegacyCategoryControllerFactory.CATEGORY_CONNECTION);
                final LegacyConfiguredObject vh = connection.getParent(VirtualHostController.TYPE);
                final UUID sessionID = (UUID) getAttribute(ID);
                final UUID connectionID = (UUID) connection.getAttribute(ID);
                final List<LegacyConfiguredObject> consumers = new ArrayList<>();
                final Collection<LegacyConfiguredObject> queues = vh.getChildren(QueueController.TYPE);
                if (queues != null)
                {
                    queues.forEach(q -> {
                        final Collection<LegacyConfiguredObject> queueConsumers =
                                q.getChildren(ConsumerController.TYPE);
                        if (queueConsumers != null)
                        {
                            queueConsumers.stream()
                                          .filter(c -> sameSession(c, sessionID, connectionID))
                                          .map(c -> getManagementController().convertFromNextVersion(c))
                                          .forEach(consumers::add);
                        }
                    });
                }
                return consumers;
            }
            return super.getChildren(category);
        }

        private boolean sameSession(final LegacyConfiguredObject consumer,
                                    final UUID sessionID,
                                    final UUID connectionID)
        {
            LegacyConfiguredObject session = (LegacyConfiguredObject) consumer.getAttribute("session");
            if (session != null)
            {
                if (sessionID.equals(session.getAttribute(ID)))
                {
                    LegacyConfiguredObject con = session.getParent(LegacyCategoryControllerFactory.CATEGORY_CONNECTION);
                    return con != null && connectionID.equals(con.getAttribute(ID));
                }
            }
            return false;
        }
    }
}