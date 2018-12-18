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
import static org.apache.qpid.server.management.plugin.ManagementException.createInternalServerErrorManagementException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.ConfiguredObject;

public class ConsumerController implements CategoryController
{
    public static final String TYPE = "Consumer";
    private final LegacyManagementController _managementController;

    ConsumerController(final LegacyManagementController managementController)
    {
        _managementController = managementController;
    }

    @Override
    public String getCategory()
    {
        return TYPE;
    }

    @Override
    public String getNextVersionCategory()
    {
        return TYPE;
    }

    @Override
    public String getDefaultType()
    {
        return null;
    }

    @Override
    public String[] getParentCategories()
    {
        return new String[]{SessionController.TYPE, QueueController.TYPE};
    }

    @Override
    public LegacyManagementController getManagementController()
    {
        return _managementController;
    }

    private ManagementController getNextVersionManagementController()
    {
        return _managementController.getNextVersionManagementController();
    }

    @Override
    public Object get(final ConfiguredObject<?> root,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        final Collection<String> hierarchy = _managementController.getCategoryHierarchy(root, TYPE);
        final String consumerName = path.size() == hierarchy.size() ? path.get(hierarchy.size() - 1) : null;
        final String queueName = path.size() >= hierarchy.size() - 1 ? path.get(hierarchy.size() - 2) : null;
        final String sessionName = path.size() >= hierarchy.size() - 2 ? path.get(hierarchy.size() - 3) : null;

        List<String> virtualHostPath = path;
        if (virtualHostPath.size() > hierarchy.size() - 3)
        {
            virtualHostPath = virtualHostPath.subList(0, hierarchy.size() - 3);
        }

        final Object queues = getNextVersionManagementController().get(root,
                                                                       "Queue",
                                                                       virtualHostPath,
                                                                       Collections.emptyMap());

        Collection<LegacyConfiguredObject> consumers;
        if (queues instanceof LegacyConfiguredObject)
        {
            consumers = getQueueConsumers(sessionName, queueName, consumerName, (LegacyConfiguredObject) queues);
        }
        else if (queues instanceof Collection)
        {
            consumers = ((Collection<?>) queues).stream()
                                           .map(LegacyConfiguredObject.class::cast)
                                           .map(q -> getQueueConsumers(sessionName, queueName, consumerName, q))
                                           .flatMap(Collection::stream)
                                           .collect(Collectors.toList());
        }
        else
        {
            throw createInternalServerErrorManagementException("Unexpected consumer format from next version");
        }
        return consumers;
    }


    @Override
    public int delete(ConfiguredObject<?> root,
                      List<String> path,
                      Map<String, List<String>> parameters) throws ManagementException
    {
        throw createBadRequestManagementException("Consumer cannot be deleted via management interfaces");
    }

    @Override
    public LegacyConfiguredObject createOrUpdate(ConfiguredObject<?> root,
                                                 List<String> path,
                                                 Map<String, Object> attributes,
                                                 boolean isPost) throws ManagementException
    {
        throw createBadRequestManagementException("Consumer cannot be created or updated via management interfaces");
    }

    @Override
    public ManagementResponse invoke(ConfiguredObject<?> root,
                                     List<String> path,
                                     String operation,
                                     Map<String, Object> parameters,
                                     boolean isPost, final boolean isSecure) throws ManagementException
    {
        Object result = get( root, path, Collections.emptyMap());
        if (result instanceof Collection && ((Collection)result).size() == 1)
        {
            LegacyConfiguredObject  object = (LegacyConfiguredObject) ((Collection<?>)result).iterator().next();
            return object.invoke(operation, parameters, isSecure);
        }

        throw createBadRequestManagementException(String.format("Cannot find consumer for path %s",
                                                                String.join("/" + path)));
    }

    @Override
    public Object getPreferences(ConfiguredObject<?> root,
                                 List<String> path,
                                 Map<String, List<String>> parameters) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Consumer");
    }

    @Override
    public void setPreferences(ConfiguredObject<?> root,
                               List<String> path,
                               Object preferences,
                               Map<String, List<String>> parameters,
                               boolean isPost) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Consumer");
    }

    @Override
    public int deletePreferences(ConfiguredObject<?> root,
                                 List<String> path,
                                 Map<String, List<String>> parameters) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Consumer");
    }

    @Override
    public LegacyConfiguredObject convertFromNextVersion(final LegacyConfiguredObject nextVersionObject)
    {
        return new LegacyConsumer(getManagementController(), nextVersionObject);
    }


    private Collection<LegacyConfiguredObject> getQueueConsumers(final String sessionName,
                                                                 final String queueName,
                                                                 final String consumerName,
                                                                 final LegacyConfiguredObject nextVersionQueue)
    {
        if (queueName == null
            || queueName.equals("*")
            || queueName.equals(nextVersionQueue.getAttribute(LegacyConfiguredObject.NAME)))
        {
            Collection<LegacyConfiguredObject> queueConsumers = nextVersionQueue.getChildren(ConsumerController.TYPE);
            if (queueConsumers != null)
            {
                return queueConsumers.stream()
                                     .filter(c -> consumerName == null
                                                  || "*".equals(consumerName)
                                                  || consumerName.equals(c.getAttribute("name")))
                                     .filter(c -> {
                                         if (sessionName == null || "*".equals(sessionName))
                                         {
                                             return true;
                                         }
                                         else
                                         {
                                             Object obj = c.getAttribute("session");
                                             if (obj instanceof LegacyConfiguredObject)
                                             {
                                                 return sessionName.equals(((LegacyConfiguredObject) obj).getAttribute(
                                                         LegacyConfiguredObject.NAME));
                                             }
                                             return false;
                                         }
                                     })
                                     .map(this::convertFromNextVersion)
                                     .collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }

    static class LegacyConsumer extends GenericLegacyConfiguredObject
    {
        LegacyConsumer(final LegacyManagementController managementController,
                       final LegacyConfiguredObject nextVersionLegacyConfiguredObject)
        {
            super(managementController, nextVersionLegacyConfiguredObject, ConsumerController.TYPE);
        }

        @Override
        public LegacyConfiguredObject getParent(final String category)
        {
            if (SessionController.TYPE.equals(category))
            {
                final LegacyConfiguredObject nextVersionConsumer = getNextVersionLegacyConfiguredObject();
                final LegacyConfiguredObject nextVersionSession =
                        (LegacyConfiguredObject) nextVersionConsumer.getAttribute(SessionController.TYPE.toLowerCase());
                return getManagementController().convertFromNextVersion(nextVersionSession);
            }

            return getManagementController().convertFromNextVersion(getNextVersionLegacyConfiguredObject().getParent(category));
        }
    }
}
