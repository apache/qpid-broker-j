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
package org.apache.qpid.server.security.access.config;

import static org.apache.qpid.server.security.access.config.LegacyOperation.BIND;
import static org.apache.qpid.server.security.access.config.LegacyOperation.UNBIND;
import static org.apache.qpid.server.security.access.config.ObjectType.EXCHANGE;
import static org.apache.qpid.server.security.access.config.ObjectType.METHOD;
import static org.apache.qpid.server.security.access.config.ObjectType.QUEUE;
import static org.apache.qpid.server.security.access.config.ObjectType.USER;
import static org.apache.qpid.server.security.access.config.LegacyOperation.ACCESS_LOGS;
import static org.apache.qpid.server.security.access.config.LegacyOperation.PUBLISH;
import static org.apache.qpid.server.security.access.config.LegacyOperation.PURGE;
import static org.apache.qpid.server.security.access.config.LegacyOperation.UPDATE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

class LegacyAccessControlAdapter
{
    private static final Set<String> LOG_ACCESS_METHOD_NAMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("getFile",
                                                                    "getFiles",
                                                                    "getAllFiles",
                                                                    "getLogEntries")));
    private static final Set<String> QUEUE_UPDATE_METHODS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("moveMessages",
                                                                    "copyMessages",
                                                                    "deleteMessages")));

    private static final Set<String> LEGACY_PREFERENCES_METHOD_NAMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("getPreferences",
                                                                    "setPreferences",
                                                                    "deletePreferences")));

    private static final Set<String> BDB_VIRTUAL_HOST_NODE_OPERATIONS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("updateMutableConfig",
                                                                    "cleanLog",
                                                                    "checkpoint")));

    private static final Set<String> BROKER_CONFIGURE_OPERATIONS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("setJVMOptions",
                                                                    "dumpHeap",
                                                                    "performGC",
                                                                    "getThreadStackTraces",
                                                                    "findThreadStackTraces",
                                                                    "extractConfig",
                                                                    "restart")));

    private static final Set<String> VIRTUALHOST_UPDATE_OPERATIONS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("importMessageStore",
                                                                    "extractMessageStore")));

    private final LegacyAccessControl _accessControl;
    private final Model _model;

    LegacyAccessControlAdapter(final LegacyAccessControl accessControl,
                               final Model model)
    {
        _accessControl = accessControl;
        _model = model;
    }

    private Model getModel()
    {
        return _model;
    }

    Result authorise(final LegacyOperation operation, final PermissionedObject configuredObject)
    {
        if (isAllowedOperation(operation, configuredObject))
        {
            return Result.ALLOWED;
        }

        Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
        ObjectType objectType = getACLObjectTypeManagingConfiguredObjectOfCategory(categoryClass);
        if (objectType == null)
        {
            throw new IllegalArgumentException("Cannot identify object type for category " + categoryClass );
        }

        ObjectProperties properties = getACLObjectProperties(configuredObject, operation);
        LegacyOperation authoriseOperation = validateAuthoriseOperation(operation, categoryClass);
        return _accessControl.authorise(authoriseOperation, objectType, properties);

    }

    private boolean isAllowedOperation(LegacyOperation operation, PermissionedObject configuredObject)
    {
        if (configuredObject instanceof Session && (operation == LegacyOperation.CREATE || operation == LegacyOperation.UPDATE
                                                    || operation == LegacyOperation.DELETE))
        {
            return true;

        }

        if (configuredObject instanceof Consumer && (operation == LegacyOperation.UPDATE || operation == LegacyOperation.DELETE))
        {
            return true;
        }

        if (configuredObject instanceof Connection && (operation == LegacyOperation.UPDATE || operation == LegacyOperation.DELETE))
        {
            return true;
        }

        return false;
    }

    private ObjectType getACLObjectTypeManagingConfiguredObjectOfCategory(Class<? extends ConfiguredObject> category)
    {
        if (Binding.class.isAssignableFrom(category))
        {
            return ObjectType.EXCHANGE;
        }
        else if (VirtualHostNode.class.isAssignableFrom(category))
        {
            return ObjectType.VIRTUALHOSTNODE;
        }
        else if (isBrokerType(category))
        {
            return ObjectType.BROKER;
        }
        else if (isVirtualHostType(category))
        {
            return ObjectType.VIRTUALHOST;
        }
        else if (Group.class.isAssignableFrom(category))
        {
            return ObjectType.GROUP;
        }
        else if (GroupMember.class.isAssignableFrom(category))
        {
            // UPDATE GROUP
            return ObjectType.GROUP;
        }
        else if (User.class.isAssignableFrom(category))
        {
            return ObjectType.USER;
        }
        else if (Queue.class.isAssignableFrom(category))
        {
            return ObjectType.QUEUE;
        }
        else if (Exchange.class.isAssignableFrom(category))
        {
            return ObjectType.EXCHANGE;
        }
        else if (Session.class.isAssignableFrom(category))
        {
            // PUBLISH EXCHANGE
            return ObjectType.EXCHANGE;
        }
        else if (Consumer.class.isAssignableFrom(category))
        {
            // CONSUME QUEUE
            return ObjectType.QUEUE;
        }
        else if (RemoteReplicationNode.class.isAssignableFrom(category))
        {
            // VHN permissions apply to remote nodes
            return ObjectType.VIRTUALHOSTNODE;
        }
        return null;
    }

    private boolean isVirtualHostType(Class<? extends ConfiguredObject> category)
    {
        return VirtualHost.class.isAssignableFrom(category) ||
               VirtualHostLogger.class.isAssignableFrom(category) ||
               VirtualHostLogInclusionRule.class.isAssignableFrom(category) ||
               VirtualHostAccessControlProvider.class.isAssignableFrom(category) ||
               Connection.class.isAssignableFrom(category);
    }

    private boolean isBrokerType(Class<? extends ConfiguredObject> category)
    {
        return Broker.class.isAssignableFrom(category) ||
               BrokerLogInclusionRule.class.isAssignableFrom(category) ||
               VirtualHostAlias.class.isAssignableFrom(category) ||
               ( !VirtualHostNode.class.isAssignableFrom(category) && getModel().getChildTypes(Broker.class).contains(category));
    }


    private ObjectProperties getACLObjectProperties(PermissionedObject configuredObject, LegacyOperation configuredObjectOperation)
    {
        String objectName = configuredObject.getName();
        Class<? extends ConfiguredObject> configuredObjectType = configuredObject.getCategoryClass();
        ObjectProperties properties = new ObjectProperties(objectName);

        if (configuredObject instanceof Queue)
        {
            setQueueProperties((Queue)configuredObject, properties);
        }
        else if (configuredObject instanceof Exchange)
        {
            Exchange<?> exchange = (Exchange<?>)configuredObject;
            Object lifeTimePolicy = exchange.getAttribute(ConfiguredObject.LIFETIME_POLICY);
            properties.put(ObjectProperties.Property.AUTO_DELETE, lifeTimePolicy != LifetimePolicy.PERMANENT);
            properties.put(ObjectProperties.Property.TEMPORARY, lifeTimePolicy != LifetimePolicy.PERMANENT);
            properties.put(ObjectProperties.Property.DURABLE, (Boolean) exchange.getAttribute(ConfiguredObject.DURABLE));
            properties.put(ObjectProperties.Property.TYPE, (String) exchange.getAttribute(Exchange.TYPE));
            VirtualHost virtualHost = (VirtualHost) exchange.getParent();
            properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, (String)virtualHost.getAttribute(virtualHost.NAME));
        }
        else if (configuredObject instanceof QueueConsumer)
        {
            Queue<?> queue = (Queue<?>)((QueueConsumer<?,?>)configuredObject).getParent();
            setQueueProperties(queue, properties);
        }
        else if (isBrokerType(configuredObjectType))
        {
            String description = String.format("%s %s '%s'",
                                               configuredObjectOperation == null? null : configuredObjectOperation.name().toLowerCase(),
                                               configuredObjectType == null ? null : configuredObjectType.getSimpleName().toLowerCase(),
                                               objectName);
            properties = new OperationLoggingDetails(description);
        }
        else if (isVirtualHostType(configuredObjectType))
        {
            ConfiguredObject<?> virtualHost = getModel().getAncestor(VirtualHost.class, (ConfiguredObject<?>)configuredObject);
            properties = new ObjectProperties((String)virtualHost.getAttribute(ConfiguredObject.NAME));
        }
        return properties;
    }

    private void setQueueProperties(ConfiguredObject<?>  queue, ObjectProperties properties)
    {
        properties.setName((String)queue.getAttribute(Exchange.NAME));
        Object lifeTimePolicy = queue.getAttribute(ConfiguredObject.LIFETIME_POLICY);
        properties.put(ObjectProperties.Property.AUTO_DELETE, lifeTimePolicy != LifetimePolicy.PERMANENT);
        properties.put(ObjectProperties.Property.TEMPORARY, lifeTimePolicy != LifetimePolicy.PERMANENT);
        properties.put(ObjectProperties.Property.DURABLE, (Boolean)queue.getAttribute(ConfiguredObject.DURABLE));
        properties.put(ObjectProperties.Property.EXCLUSIVE, queue.getAttribute(Queue.EXCLUSIVE) != ExclusivityPolicy.NONE);
        Object alternateExchange = queue.getAttribute(Queue.ALTERNATE_EXCHANGE);
        if (alternateExchange != null)
        {
            String name = alternateExchange instanceof ConfiguredObject ?
                    (String)((ConfiguredObject)alternateExchange).getAttribute(ConfiguredObject.NAME) :
                    String.valueOf(alternateExchange);
            properties.put(ObjectProperties.Property.ALTERNATE, name);
        }
        String owner = (String)queue.getAttribute(Queue.OWNER);
        if (owner != null)
        {
            properties.put(ObjectProperties.Property.OWNER, owner);
        }
        VirtualHost virtualHost = (VirtualHost) queue.getParent();
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, (String)virtualHost.getAttribute(virtualHost.NAME));
    }


    private LegacyOperation validateAuthoriseOperation(LegacyOperation operation, Class<? extends ConfiguredObject> category)
    {
        if (operation == LegacyOperation.CREATE || operation == LegacyOperation.UPDATE)
        {
            if (Consumer.class.isAssignableFrom(category))
            {
                // CREATE CONSUMER is transformed into CONSUME QUEUE rule
                return LegacyOperation.CONSUME;
            }
            else if (GroupMember.class.isAssignableFrom(category))
            {
                // CREATE GROUP MEMBER is transformed into UPDATE GROUP rule
                return LegacyOperation.UPDATE;
            }
            else if (isBrokerType(category))
            {
                // CREATE/UPDATE broker child is transformed into CONFIGURE BROKER rule
                return LegacyOperation.CONFIGURE;
            }
        }
        else if (operation == LegacyOperation.DELETE)
        {
            if (isBrokerType(category))
            {
                // DELETE broker child is transformed into CONFIGURE BROKER rule
                return LegacyOperation.CONFIGURE;

            }
            else if (GroupMember.class.isAssignableFrom(category))
            {
                // DELETE GROUP MEMBER is transformed into UPDATE GROUP rule
                return LegacyOperation.UPDATE;
            }
        }
        return operation;
    }

    Result authoriseAction(final PermissionedObject configuredObject,
                           String actionName,
                           final Map<String, Object> arguments)
    {
        Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
        if(categoryClass == Exchange.class)
        {
            MessageDestination exchange = (MessageDestination) configuredObject;
            if("publish".equals(actionName))
            {

                final ObjectProperties _props =
                        new ObjectProperties(exchange.getAddressSpace().getName(), exchange.getName(), (String)arguments.get("routingKey"), (Boolean)arguments.get("immediate"));
                return _accessControl.authorise(PUBLISH, EXCHANGE, _props);
            }
        }
        else if(categoryClass == VirtualHost.class)
        {
            if("connect".equals(actionName))
            {
                String virtualHostName = configuredObject.getName();
                ObjectProperties properties = new ObjectProperties(virtualHostName);
                properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, virtualHostName);
                return _accessControl.authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);
            }
        }
        else if(categoryClass == Broker.class)
        {
            if("manage".equals(actionName))
            {
                return _accessControl.authorise(LegacyOperation.ACCESS, ObjectType.MANAGEMENT, ObjectProperties.EMPTY);
            }
            else if("CONFIGURE".equals(actionName) || "SHUTDOWN".equals(actionName))
            {
                return _accessControl.authorise(LegacyOperation.valueOf(actionName), ObjectType.BROKER, ObjectProperties.EMPTY);
            }
        }
        else if(categoryClass == Queue.class)
        {
            Queue queue = (Queue) configuredObject;
            if("publish".equals(actionName))
            {

                final ObjectProperties _props =
                        new ObjectProperties(queue.getParent().getName(), "", queue.getName(), (Boolean)arguments.get("immediate"));
                return _accessControl.authorise(PUBLISH, EXCHANGE, _props);
            }
        }

        return Result.DEFER;

    }

    Result authoriseMethod(final PermissionedObject configuredObject,
                           final String methodName,
                           final Map<String, Object> arguments)
    {
        Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
        if(categoryClass == Queue.class)
        {
            Queue queue = (Queue) configuredObject;
            final ObjectProperties properties = new ObjectProperties();
            if("clearQueue".equals(methodName))
            {
                setQueueProperties(queue, properties);
                return _accessControl.authorise(PURGE, QUEUE, properties);
            }
            else if(QUEUE_UPDATE_METHODS.contains(methodName))
            {
                VirtualHost virtualHost = queue.getVirtualHost();
                final String virtualHostName = virtualHost.getName();
                properties.setName(methodName);
                properties.put(ObjectProperties.Property.COMPONENT, "VirtualHost.Queue");
                properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, virtualHostName);
                return _accessControl.authorise(LegacyOperation.UPDATE, METHOD, properties);

            }
            else if("publish".equals(methodName))
            {

                final ObjectProperties _props =
                        new ObjectProperties(queue.getParent().getName(), "", queue.getName(), (Boolean)arguments.get("immediate"));
                return _accessControl.authorise(PUBLISH, EXCHANGE, _props);
            }
        }
        else if(categoryClass == BrokerLogger.class)
        {
            if(LOG_ACCESS_METHOD_NAMES.contains(methodName))
            {
                return _accessControl.authorise(ACCESS_LOGS, ObjectType.BROKER, ObjectProperties.EMPTY);
            }
        }
        else if(categoryClass == VirtualHostLogger.class)
        {
            VirtualHostLogger logger = (VirtualHostLogger)configuredObject;
            if(LOG_ACCESS_METHOD_NAMES.contains(methodName))
            {
                return _accessControl.authorise(ACCESS_LOGS,
                                                ObjectType.VIRTUALHOST,
                                                new ObjectProperties(logger.getParent().getName()));
            }
        }
        else if(categoryClass == AuthenticationProvider.class)
        {
            if(LEGACY_PREFERENCES_METHOD_NAMES.contains(methodName))
            {
                if(arguments.get("userId") instanceof String)
                {
                    String userName = (String) arguments.get("userId");
                    AuthenticatedPrincipal principal = AuthenticatedPrincipal.getCurrentUser();
                    if (principal != null && principal.getName().equals(userName))
                    {
                        // allow user to update its own data
                        return Result.ALLOWED;
                    }
                    else
                    {
                        return _accessControl.authorise(UPDATE,
                                                        USER,
                                                        new ObjectProperties(userName));
                    }
                }
            }
        }
        else if(categoryClass == VirtualHostNode.class)
        {
            if(BDB_VIRTUAL_HOST_NODE_OPERATIONS.contains(methodName))
            {
                ObjectProperties properties = getACLObjectProperties(((ConfiguredObject)configuredObject).getParent(), LegacyOperation.UPDATE);
                return _accessControl.authorise(LegacyOperation.UPDATE, ObjectType.BROKER, properties);
            }
        }
        else if(categoryClass == Broker.class)
        {
            if(BROKER_CONFIGURE_OPERATIONS.contains(methodName))
            {
                _accessControl.authorise(LegacyOperation.CONFIGURE, ObjectType.BROKER, ObjectProperties.EMPTY);
            }
            else if("initiateShutdown".equals(methodName))
            {
                _accessControl.authorise(LegacyOperation.SHUTDOWN, ObjectType.BROKER, ObjectProperties.EMPTY);
            }

        }
        else if(categoryClass == VirtualHost.class)
        {
            if(VIRTUALHOST_UPDATE_OPERATIONS.contains(methodName))
            {
                authorise(LegacyOperation.UPDATE, configuredObject);
            }
        }
        else if (categoryClass == Exchange.class)
        {
            if ("bind".equals(methodName))
            {
                final ObjectProperties properties = createArgsForExchangeBind(arguments, configuredObject);
                return _accessControl.authorise(BIND, EXCHANGE, properties);
            }
            else if ("unbind".equals(methodName))
            {
                final ObjectProperties properties = createArgsForExchangeBind(arguments, configuredObject);
                return _accessControl.authorise(UNBIND, EXCHANGE, properties);
            }

        }
        return Result.ALLOWED;

    }

    private ObjectProperties createArgsForExchangeBind(final Map<String, Object> arguments,
                                           final PermissionedObject configuredObject)
    {
        ObjectProperties properties = new ObjectProperties();
        Exchange<?> exchange = (Exchange<?>) configuredObject;
        final QueueManagingVirtualHost virtualhost = exchange.getVirtualHost();

        properties.setName(exchange.getName());
        final String destination = (String) arguments.get("destination");
        properties.put(ObjectProperties.Property.QUEUE_NAME, destination);
        properties.put(ObjectProperties.Property.ROUTING_KEY, (String)arguments.get("bindingKey"));
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, virtualhost.getName());

        MessageDestination dest = virtualhost.getAttainedMessageDestination(destination);
        if (dest != null)
        {
            // The temporary attribute (inherited from the binding's queue) seems to exist to allow the user to
            // express rules about the binding of temporary queues (whose names cannot be predicted).
            if (dest instanceof ConfiguredObject)
            {
                ConfiguredObject queue = (ConfiguredObject) dest;
                properties.put(ObjectProperties.Property.TEMPORARY, queue.getLifetimePolicy() != LifetimePolicy.PERMANENT);
            }
            properties.put(ObjectProperties.Property.DURABLE, dest.isDurable());

        }
        return properties;
    }


    Result authorise(final Operation operation,
                     final PermissionedObject configuredObject,
                     final Map<String, Object> arguments)
    {
        switch(operation.getType())
        {
            case CREATE:
                return authorise(LegacyOperation.CREATE, configuredObject);
            case UPDATE:
                return authorise(LegacyOperation.UPDATE, configuredObject);
            case DELETE:
                return authorise(LegacyOperation.DELETE, configuredObject);
            case METHOD:
                return authoriseMethod(configuredObject, operation.getName(), arguments);
            case ACTION:
                return authoriseAction(configuredObject, operation.getName(), arguments);
            case DISCOVER:
            case READ:
                return Result.DEFER;

            default:
        }
        return null;
    }
}
