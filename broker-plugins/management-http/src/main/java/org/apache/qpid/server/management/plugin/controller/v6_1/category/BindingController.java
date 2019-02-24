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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.ControllerManagementResponse;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;

public class BindingController implements CategoryController
{
    public static final String TYPE = "Binding";
    private final LegacyManagementController _managementController;
    private final ManagementController _nextVersionManagementController;

    BindingController(final LegacyManagementController managementController)
    {
        _managementController = managementController;
        _nextVersionManagementController = _managementController.getNextVersionManagementController();
    }

    @Override
    public String getCategory()
    {
        return TYPE;
    }

    @Override
    public String getNextVersionCategory()
    {
        return null;
    }

    @Override
    public String getDefaultType()
    {
        return null;
    }

    @Override
    public String[] getParentCategories()
    {
        return new String[]{ExchangeController.TYPE, QueueController.TYPE};
    }

    @Override
    public LegacyManagementController getManagementController()
    {
        return _managementController;
    }

    @Override
    public Object get(final ConfiguredObject<?> root,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        final Collection<String> hierarchy = _managementController.getCategoryHierarchy(root, getCategory());

        final String bindingName = path.size() == hierarchy.size() ? path.get(hierarchy.size() - 1) : null;
        final String queueName = path.size() >= hierarchy.size() - 1 ? path.get(hierarchy.size() - 2) : null;

        final List<String> exchangePath =
                path.size() >= hierarchy.size() - 2 ? path.subList(0, hierarchy.size() - 2) : path;

        return getExchangeBindings(root, exchangePath, queueName, bindingName);

    }


    @Override
    public LegacyConfiguredObject createOrUpdate(final ConfiguredObject<?> root,
                                                 final List<String> path,
                                                 final Map<String, Object> attributes,
                                                 final boolean isPost) throws ManagementException
    {
        if (path.contains("*"))
        {
            throw createBadRequestManagementException("Wildcards in path are not supported for post and put requests");
        }

        final Collection<String> hierarchy = _managementController.getCategoryHierarchy(root, getCategory());
        if (path.size() < hierarchy.size() - 2)
        {
            throw createBadRequestManagementException(String.format("Cannot create binding for path %s",
                                                                    String.join("/" + path)));
        }

        String queueName = null;

        if (path.size() > hierarchy.size() - 2)
        {
            queueName = path.get(hierarchy.size() - 2);
        }
        if (queueName == null)
        {
            queueName = (String) attributes.get("queue");
        }
        if (queueName == null)
        {
            throw createBadRequestManagementException(
                    "Queue is required for binding creation. Please specify queue either in path or in binding attributes");
        }

        final List<String> exchangePath = path.subList(0, hierarchy.size() - 2);
        final LegacyConfiguredObject
                nextVersionExchange = getNextVersionObject(root, exchangePath, ExchangeController.TYPE);
        final List<String> queuePath = new ArrayList<>(path.subList(0, hierarchy.size() - 3));
        queuePath.add(queueName);
        final LegacyConfiguredObject nextVersionQueue =
                getNextVersionObject(root, queuePath, QueueController.TYPE);

        String bindingKey = (String) attributes.get(GenericLegacyConfiguredObject.NAME);
        if (bindingKey == null)
        {
            bindingKey = path.size() == hierarchy.size() ? path.get(hierarchy.size() - 1) : null;
        }
        if (bindingKey == null)
        {
            bindingKey = "";
        }

        final Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("bindingKey", bindingKey);
        parameters.put("destination", queueName);

        Map<String, Object> arguments = null;
        if (attributes.containsKey("arguments"))
        {
            Object args = attributes.get("arguments");
            if (args instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, Object> argumentsMap = (Map<String, Object>) args;
                arguments = new HashMap<>(argumentsMap);
                if (!arguments.isEmpty())
                {
                    parameters.put("arguments", arguments);
                }
            }
            else
            {
                throw createBadRequestManagementException(String.format("Unexpected attributes specified : %s", args));
            }
        }
        parameters.put("replaceExistingArguments", !isPost);

        ManagementResponse response = nextVersionExchange.invoke("bind", parameters, true);
        final boolean newBindings = Boolean.TRUE.equals(response.getBody());
        if (!newBindings)
        {
            return null;
        }

        return new LegacyBinding(_managementController,
                                 nextVersionExchange,
                                 nextVersionQueue,
                                 bindingKey,
                                 arguments);
    }

    @Override
    public int delete(final ConfiguredObject<?> root,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        if (path.contains("*"))
        {
            throw createBadRequestManagementException("Wildcards in path are not supported for delete requests");
        }

        final Collection<String> hierarchy = _managementController.getCategoryHierarchy(root, getCategory());
        if (path.size() < hierarchy.size() - 2)
        {
            throw createBadRequestManagementException(String.format("Cannot delete binding for path %s",
                                                                    String.join("/", path)));
        }

        final String bindingName = path.size() == hierarchy.size() ? path.get(hierarchy.size() - 1) : null;
        final String queueName = path.get(hierarchy.size() - 2);
        final List<String> ids = parameters.get(GenericLegacyConfiguredObject.ID);
        final List<String> exchangePath = path.subList(0, hierarchy.size() - 2);

        final LegacyConfiguredObject exchange = getNextVersionObject(root, exchangePath, ExchangeController.TYPE);

        final AtomicInteger counter = new AtomicInteger();

        if (ids != null && !ids.isEmpty())
        {
            @SuppressWarnings("unchecked")
            Collection<Binding> bindings = (Collection<Binding>) exchange.getAttribute("bindings");
            if (bindings != null)
            {
                bindings.stream()
                        .filter(b -> ids.contains(String.valueOf(generateBindingId(exchange,
                                                                                   b.getDestination(),
                                                                                   b.getBindingKey()))))
                        .forEach(b -> {

                            Map<String, Object> params = new LinkedHashMap<>();
                            params.put("bindingKey", b.getBindingKey());
                            params.put("destination", b.getDestination());
                            ManagementResponse r = exchange.invoke("unbind", params, true);
                            if (Boolean.TRUE.equals(r.getBody()))
                            {
                                counter.incrementAndGet();
                            }
                        });
            }
        }
        else if (bindingName != null)
        {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("bindingKey", bindingName);
            params.put("destination", queueName);
            ManagementResponse response = exchange.invoke("unbind", params, true);
            if (Boolean.TRUE.equals(response.getBody()))
            {
                counter.incrementAndGet();
            }
        }
        else
        {
            throw createBadRequestManagementException("Only deletion by binding full path or ids is supported");
        }

        return counter.get();
    }

    private LegacyConfiguredObject getNextVersionObject(final ConfiguredObject<?> root,
                                                        final List<String> path,
                                                        final String type)
    {
        return (LegacyConfiguredObject) _nextVersionManagementController.get(root,
                                                                             type.toLowerCase(),
                                                                             path,
                                                                             Collections.emptyMap());
    }

    @Override
    public ManagementResponse invoke(ConfiguredObject<?> root,
                                     List<String> path,
                                     String operation,
                                     Map<String, Object> parameters,
                                     boolean isPost, final boolean isSecure) throws ManagementException
    {
        Object result = get(root, path, Collections.emptyMap());
        if (result instanceof Collection && ((Collection)result).size() == 1)
        {
            LegacyConfiguredObject  object = (LegacyConfiguredObject) ((Collection<?>)result).iterator().next();
            return object.invoke(operation, parameters, isSecure);
        }
        throw createBadRequestManagementException(String.format("Operation '%s' cannot be invoked for Binding path '%s'",
                                                                operation,
                                                                String.join("/", path)));
    }


    @Override
    public Object getPreferences(ConfiguredObject<?> root,
                                 List<String> path,
                                 Map<String, List<String>> parameters) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Binding");
    }

    @Override
    public void setPreferences(ConfiguredObject<?> root,
                               List<String> path,
                               Object preferences,
                               Map<String, List<String>> parameters,
                               boolean isPost) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Binding");
    }

    @Override
    public int deletePreferences(ConfiguredObject<?> root,
                                 List<String> path,
                                 Map<String, List<String>> parameters) throws ManagementException
    {
        throw createBadRequestManagementException("Preferences not supported for Binding");
    }

    @Override
    public LegacyConfiguredObject convertFromNextVersion(final LegacyConfiguredObject nextVersionObject)
    {
        return null;
    }

    private Collection<LegacyConfiguredObject> getExchangeBindings(final ConfiguredObject<?> root,
                                                                   final List<String> exchangePath,
                                                                   final String queueName,
                                                                   final String bindingName)
    {
        final Object result = getNextVersionExchanges(root, exchangePath);
        if (result instanceof LegacyConfiguredObject)
        {
            return getExchangeBindings((LegacyConfiguredObject) result, queueName, bindingName);
        }
        else if (result instanceof Collection)
        {
            return ((Collection<?>) result).stream()
                                           .map(LegacyConfiguredObject.class::cast)
                                           .map(e -> getExchangeBindings(e, queueName, bindingName))
                                           .flatMap(Collection::stream)
                                           .collect(Collectors.toList());
        }
        else
        {
            throw createInternalServerErrorManagementException("Unexpected format of content from next version");
        }

    }

    private Object getNextVersionExchanges(final ConfiguredObject<?> root, final List<String> exchangePath)
    {
        try
        {
            return _nextVersionManagementController.get(root,
                                                        ExchangeController.TYPE.toLowerCase(),
                                                        exchangePath,
                                                        Collections.emptyMap());
        }
        catch (ManagementException e)
        {
            if (e.getStatusCode() != HttpServletResponse.SC_NOT_FOUND)
            {
                throw e;
            }
            return Collections.emptyList();
        }
    }


    private Collection<LegacyConfiguredObject> getExchangeBindings(final LegacyConfiguredObject nextVersionExchange,
                                                                   final String queueName,
                                                                   final String bindingName)
    {

        @SuppressWarnings("unchecked")
        Object items = nextVersionExchange.getAttribute("bindings");
        if (items instanceof Collection)
        {
            return ((Collection<?>) items).stream()
                                          .map(Binding.class::cast)
                                          .filter(b -> (queueName == null
                                                        || "*".equals(queueName)
                                                        || queueName.equals(b.getDestination()))
                                                       && (bindingName == null || "*".equals(bindingName) || bindingName
                                                  .equals(b.getName())))
                                          .map(b -> createManageableBinding(b, nextVersionExchange))
                                          .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private LegacyConfiguredObject createManageableBinding(final Binding binding,
                                                           final LegacyConfiguredObject nextVersionExchange)
    {
        final LegacyConfiguredObject nextVersionVirtualHost = nextVersionExchange.getParent(VirtualHostController.TYPE);
        final LegacyConfiguredObject queue = findNextVersionQueue(binding.getDestination(), nextVersionVirtualHost);

        return new LegacyBinding(_managementController,
                                 nextVersionExchange,
                                 queue,
                                 binding.getBindingKey(),
                                 binding.getArguments());
    }


    private static LegacyConfiguredObject findNextVersionQueue(final String queueName,
                                                               final LegacyConfiguredObject nextVersionVirtualHost)
    {
        Collection<LegacyConfiguredObject> queues = nextVersionVirtualHost.getChildren(QueueController.TYPE);
        return queues.stream()
                     .filter(q -> queueName.equals(q.getAttribute(GenericLegacyConfiguredObject.NAME)))
                     .findFirst()
                     .orElse(null);
    }


    private static UUID generateBindingId(final LegacyConfiguredObject exchange,
                                          final String queueName,
                                          final String bindingKey)
    {
        String pseudoID = exchange.getAttribute(GenericLegacyConfiguredObject.ID) + "/" + queueName + "/" + bindingKey;
        return UUID.nameUUIDFromBytes(pseudoID.getBytes(StandardCharsets.UTF_8));
    }

    static class LegacyBinding implements LegacyConfiguredObject
    {
        private static final String ARGUMENTS = "arguments";
        private static final String QUEUE = "queue";
        private static final String EXCHANGE = "exchange";
        private static final Collection<String> ATTRIBUTE_NAMES =
                Collections.unmodifiableSet(Stream.concat(GenericLegacyConfiguredObject.AVAILABLE_ATTRIBUTES.stream(),
                                                          Stream.of(ARGUMENTS, QUEUE, EXCHANGE))
                                                  .collect(Collectors.toSet()));

        private final String _bindingKey;
        private final Map<String, Object> _arguments;
        private final LegacyConfiguredObject _exchange;
        private final UUID _id;
        private final LegacyConfiguredObject _queue;
        private final LegacyManagementController _controller;

        LegacyBinding(final LegacyManagementController controller,
                      final LegacyConfiguredObject nextVersionExchange,
                      final LegacyConfiguredObject nextVersionQueue,
                      final String bindingKey,
                      final Map<String, Object> arguments)
        {
            _controller = controller;
            _exchange = _controller.convertFromNextVersion(nextVersionExchange);
            _queue = _controller.convertFromNextVersion(nextVersionQueue);
            _bindingKey = bindingKey;
            _arguments = arguments != null && !arguments.isEmpty() ? arguments : null;
            String queueName = (String) nextVersionQueue.getAttribute(NAME);
            _id = generateBindingId(nextVersionExchange, queueName, bindingKey);
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return ATTRIBUTE_NAMES;
        }

        @Override
        public Object getAttribute(final String name)
        {
            if (ID.equalsIgnoreCase(name))
            {
                return _id;
            }
            else if (NAME.equalsIgnoreCase(name))
            {
                return _bindingKey;
            }
            else if (STATE.equalsIgnoreCase(name) || DESIRED_STATE.equalsIgnoreCase(name))
            {
                return State.ACTIVE;
            }
            else if (TYPE.equalsIgnoreCase(name))
            {
                return TYPE;
            }
            else if (CONTEXT.equalsIgnoreCase(name))
            {
                return _exchange.getAttribute(CONTEXT);
            }
            else if (QUEUE.equalsIgnoreCase(name))
            {
                return _queue;
            }
            else if (EXCHANGE.equalsIgnoreCase(name))
            {
                return _exchange;
            }
            else if (DURABLE.equalsIgnoreCase(name))
            {
                return Boolean.TRUE.equals(_queue.getAttribute(DURABLE))
                       && Boolean.TRUE.equals(_exchange.getAttribute(DURABLE));
            }
            else if (LIFETIME_POLICY.equalsIgnoreCase(name))
            {
                return _queue.getAttribute(LIFETIME_POLICY);
            }
            else if (ARGUMENTS.equalsIgnoreCase(name))
            {
                return _arguments;
            }

            return null;
        }

        @Override
        public Object getActualAttribute(final String name)
        {
            if (QUEUE.equalsIgnoreCase(name))
            {
                return _queue.getAttribute(LegacyConfiguredObject.NAME);
            }
            else if (EXCHANGE.equalsIgnoreCase(name))
            {
                return _exchange.getAttribute(LegacyConfiguredObject.NAME);
            }
            return getAttribute(name);
        }

        @Override
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            return Collections.emptyList();
        }

        @Override
        public String getCategory()
        {
            return BindingController.TYPE;
        }

        @Override
        public ManagementResponse invoke(final String operation,
                                         final Map<String, Object> parameters,
                                         final boolean isSecure)
        {
            if ("getStatistics".equals(operation))
            {
                return new ControllerManagementResponse(ResponseType.DATA, Collections.emptyMap());
            }
            throw createBadRequestManagementException("No operation is available for Binding");
        }

        @Override
        public LegacyConfiguredObject getNextVersionConfiguredObject()
        {
            return null;
        }

        public void delete()
        {
            Map<String, Object> parameters = new LinkedHashMap<>();
            parameters.put("bindingKey", getAttribute(NAME));
            parameters.put("destination", _queue.getAttribute(NAME));
            _exchange.getNextVersionConfiguredObject().invoke("unbind", parameters, true);
        }

        @Override
        public LegacyConfiguredObject getParent(final String category)
        {
            if (QueueController.TYPE.equalsIgnoreCase(category))
            {
                return _queue;
            }
            else if (ExchangeController.TYPE.equalsIgnoreCase(category))
            {
                return _exchange;
            }
            throw createInternalServerErrorManagementException(String.format("Category %s is not parent of Binding",
                                                                             category));
        }

        @Override
        public boolean isSecureAttribute(final String name)
        {
            return false;
        }

        @Override
        public boolean isOversizedAttribute(final String name)
        {
            return false;
        }

        @Override
        public String getContextValue(final String contextKey)
        {
            return _exchange.getContextValue(contextKey);
        }

        @Override
        public Map<String, Object> getStatistics()
        {
            return Collections.emptyMap();
        }
    }
}
