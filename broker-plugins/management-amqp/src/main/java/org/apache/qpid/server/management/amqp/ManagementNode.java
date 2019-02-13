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
package org.apache.qpid.server.management.amqp;

import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.getRawType;
import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.returnsCollectionOfConfiguredObjects;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;
import java.security.AccessControlException;
import java.security.AccessController;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.connection.AmqpConnectionMetaData;
import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.OperationParameter;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;

class ManagementNode implements MessageSource, MessageDestination, BaseQueue
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementNode.class);

    public static final String IDENTITY_ATTRIBUTE = "identity";
    public static final String INDEX_ATTRIBUTE = "index";
    public static final String KEY_ATTRIBUTE = "key";
    public static final String ACTUALS_ATTRIBUTE = "actuals";

    public static final String TYPE_ATTRIBUTE = "type";
    public static final String OPERATION_HEADER = "operation";
    public static final String SELF_NODE_NAME = "self";
    public static final String MANAGEMENT_TYPE = "org.amqp.management";
    public static final String GET_TYPES = "GET-TYPES";
    public static final String GET_ATTRIBUTES = "GET-ATTRIBUTES";
    public static final String GET_OPERATIONS = "GET-OPERATIONS";
    public static final String QUERY = "QUERY";
    public static final String ENTITY_TYPE_HEADER = "entityType";
    public static final String STATUS_CODE_HEADER = "statusCode";
    public static final String OFFSET_HEADER = "offset";
    public static final String COUNT_HEADER = "count";
    public static final String MANAGEMENT_NODE_NAME = "$management";
    public static final String STATUS_DESCRIPTION_HEADER = "statusDescription";
    public static final String ATTRIBUTES_HEADER = "attributes";
    public static final String ATTRIBUTE_NAMES = "attributeNames";
    public static final String RESULTS = "results";
    static final String OBJECT_PATH = "object-path";
    static final String QPID_TYPE = "qpid-type";


    public static final int STATUS_CODE_OK = 200;
    private static final int STATUS_CODE_CREATED = 201;
    public static final int STATUS_CODE_NO_CONTENT = 204;
    public static final int STATUS_CODE_BAD_REQUEST = 400;
    public static final int STATUS_CODE_FORBIDDEN = 403;
    public static final int STATUS_CODE_NOT_FOUND = 404;
    public static final int STATUS_CODE_CONFLICT = 409;
    public static final int STATUS_CODE_INTERNAL_ERROR = 500;
    public static final int STATUS_CODE_NOT_IMPLEMENTED = 501;
    private static final Comparator<? super ConfiguredObject<?>> OBJECT_COMPARATOR =
            new Comparator<ConfiguredObject<?>>()
            {
                @Override
                public int compare(final ConfiguredObject<?> o1, final ConfiguredObject<?> o2)
                {
                    if(o1 == o2)
                    {
                        return 0;
                    }
                    int result = o1.getCategoryClass().getSimpleName().compareTo(o2.getCategoryClass().getSimpleName());
                    if(result == 0)
                    {
                        result = o1.getName().compareTo(o2.getName());
                    }
                    if(result == 0)
                    {
                        result = o1.getId().compareTo(o2.getId());
                    }
                    return result;
                }
            };


    private final NamedAddressSpace _addressSpace;

    private final UUID _id;

    private final ConfiguredObject<?> _managedObject;
    private final Model _model;
    private final Map<Class<? extends ConfiguredObject>, ConfiguredObjectOperation<?>> _associatedChildrenOperations = new HashMap<>();
    private final ConfiguredObjectFinder _configuredObjectFinder;
    private List<ManagementNodeConsumer> _consumers = new CopyOnWriteArrayList<>();

    private final Set<Class<? extends ConfiguredObject>> _managedCategories = new HashSet<>();
    private final Map<String, Class<? extends ConfiguredObject>> _managedTypes = new HashMap<>();
    private final Map<Class<? extends ConfiguredObject>, Map<String, StandardOperation>> _standardOperations = new HashMap<>();

    private final ManagementOutputConverter _managementOutputConverter;

    private final ManagementInputConverter _managementInputConverter;

    private static final InstanceProperties CONSUMED_INSTANCE_PROPERTIES = prop -> null;

    ManagementNode(final NamedAddressSpace addressSpace,
                   final ConfiguredObject<?> configuredObject)
    {
        _addressSpace = addressSpace;
        final String name = configuredObject.getId() + MANAGEMENT_NODE_NAME;
        _id = UUID.nameUUIDFromBytes(name.getBytes(Charset.defaultCharset()));

        _model = configuredObject.getModel();
        _managedObject = configuredObject;

        populateMetaData();
        _managementOutputConverter = new ManagementOutputConverter(this);
        _managementInputConverter = new ManagementInputConverter(this);

        _configuredObjectFinder = new ConfiguredObjectFinder(configuredObject);
    }

    ConfiguredObject<?> getManagedObject()
    {
        return _managedObject;
    }

    boolean isSyntheticChildClass(Class<? extends ConfiguredObject> clazz)
    {
        return _associatedChildrenOperations.containsKey(clazz);
    }

    private void populateMetaData()
    {
        populateManagedCategories();

        populateManagedTypes();

        populateStandardOperations();
    }

    private void populateStandardOperations()
    {
        for(Class<? extends ConfiguredObject> type : _managedTypes.values())
        {
            HashMap<String, StandardOperation> operationsMap = new HashMap<>();
            _standardOperations.put(type, operationsMap);
            operationsMap.put(READ_OPERATION.getName(), READ_OPERATION);
            operationsMap.put(UPDATE_OPERATION.getName(), UPDATE_OPERATION);
            if(ConfiguredObjectTypeRegistry.getCategory(type) != _managedObject.getCategoryClass())
            {
                operationsMap.put(DELETE_OPERATION.getName(), DELETE_OPERATION);
                if(type.getAnnotation(ManagedObject.class).creatable())
                {
                    operationsMap.put(CREATE_OPERATION.getName(), CREATE_OPERATION);
                }
            }
        }
    }

    private void populateManagedTypes()
    {
        for(Class<? extends ConfiguredObject> category : _managedCategories)
        {
            _managedTypes.put(getAmqpName(category), category);
            if(category != _managedObject.getCategoryClass())
            {
                for (Class<? extends ConfiguredObject> type : _model.getTypeRegistry().getTypeSpecialisations(category))
                {
                    if (type.getAnnotation(ManagedObject.class) != null)
                    {
                        _managedTypes.put(getAmqpName(type), type);
                    }
                }
            }
            else if(_managedObject.getTypeClass() != _managedObject.getCategoryClass())
            {
                _managedTypes.put(getAmqpName(_managedObject.getTypeClass()), _managedObject.getTypeClass());
            }
        }
    }

    private void populateManagedCategories()
    {
        Class<? extends ConfiguredObject> managedCategory = _managedObject.getCategoryClass();
        addManagedCategories(managedCategory);


        for(ConfiguredObjectOperation<?> operation : _model.getTypeRegistry().getOperations(managedCategory).values())
        {
            if(operation.isAssociateAsIfChildren() && returnsCollectionOfConfiguredObjects(operation))
            {
                @SuppressWarnings("unchecked")
                Class<? extends ConfiguredObject> associatedChildCategory =
                        (getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()));
                _associatedChildrenOperations.put(associatedChildCategory, operation);
                addManagedCategories(associatedChildCategory);
            }
        }
    }

    private Class getCollectionMemberType(ParameterizedType collectionType)
    {
        return getRawType((collectionType).getActualTypeArguments()[0]);
    }

    String getAmqpName(final Class<? extends ConfiguredObject> type)
    {
        ManagedObject annotation = type.getAnnotation(ManagedObject.class);

        return "".equals(annotation.amqpName()) ? type.getName() : annotation.amqpName();
    }

    private void addManagedCategories(Class<? extends ConfiguredObject> category)
    {
        if(_managedCategories.add(category))
        {
            for(Class<? extends ConfiguredObject> childClass : _model.getChildTypes(category))
            {
                addManagedCategories(childClass);
            }

        }
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                               final String routingAddress,
                                                                                               final InstanceProperties instanceProperties)
    {
        final RoutingResult<M> result = new RoutingResult<>(message);
        if(message.isResourceAcceptable(this))
        {
            @SuppressWarnings("unchecked")
            MessageConverter<M, InternalMessage> converter =
                    MessageConverterRegistry.getConverter(((Class<M>) message.getClass()), InternalMessage.class);


            if (converter != null)
            {
                result.addQueue(this);
            }

        }
        return result;

    }

    @Override
    public boolean isDurable()
    {
        return true;
    }

    @Override
    public void linkAdded(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public void linkRemoved(final MessageSender sender, final PublishingLink link)
    {

    }

    @Override
    public MessageDestination getAlternateBindingDestination()
    {
        return null;
    }

    @Override
    public void removeReference(final DestinationReferrer destinationReferrer)
    {
    }

    @Override
    public void addReference(final DestinationReferrer destinationReferrer)
    {
    }

    private synchronized void processRequest(InternalMessage message)
    {
        String id = (String) message.getMessageHeader().getHeader(IDENTITY_ATTRIBUTE);
        String type = (String) message.getMessageHeader().getHeader(TYPE_ATTRIBUTE);
        String operation = (String) message.getMessageHeader().getHeader(OPERATION_HEADER);
        LOGGER.debug("Management Node identity: {}, type: {}, operation {}", id, type, operation);
        InternalMessage response;

        // TODO - handle runtime exceptions

        if(SELF_NODE_NAME.equals(id) && type.equals(MANAGEMENT_TYPE))
        {
            response = performManagementOperation(operation, message);
        }
        else if(_managedTypes.containsKey(type))
        {
            response = performOperation(_managedTypes.get(type), operation, message);

        }
        else
        {
            response = createFailureResponse(message,
                                             STATUS_CODE_NOT_FOUND,
                                             "Unknown type {0}", type);
        }

        sendResponse(message, response);

    }

    @Override
    public void enqueue(final ServerMessage message,
                        final Action<? super MessageInstance> action,
                        final MessageEnqueueRecord record)
    {
        if (!message.checkValid())
        {
            throw new MessageConversionException(String.format("Cannot convert malformed message '%s'", message));
        }
        @SuppressWarnings("unchecked")
        MessageConverter<ServerMessage, InternalMessage> converter =
                (MessageConverter<ServerMessage, InternalMessage>) MessageConverterRegistry.getConverter((message.getClass()), InternalMessage.class);

        final InternalMessage msg = converter.convert(message, _addressSpace);

        try
        {
            if (action != null)
            {
                action.performAction(new ConsumedMessageInstance(msg));
            }
            processRequest(msg);
        }
        finally
        {
            converter.dispose(msg);
        }

    }

    @Override
    public boolean isDeleted()
    {
        return false;
    }

    private interface StandardOperation
    {
        String getName();
        InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                         final InternalMessage message);

    }


    private final StandardOperation CREATE_OPERATION =
            new StandardOperation()
            {
                @Override
                public String getName()
                {
                    return "CREATE";
                }

                @Override
                public InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                                        final InternalMessage message)
                {
                    return performCreateOperation(clazz, message);
                }
            };


    private final StandardOperation READ_OPERATION =
            new StandardOperation()
            {
                @Override
                public String getName()
                {
                    return "READ";
                }

                @Override
                public InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                                        final InternalMessage message)
                {
                    return performReadOperation(clazz, message);
                }
            };


    private final StandardOperation UPDATE_OPERATION =
            new StandardOperation()
            {
                @Override
                public String getName()
                {
                    return "UPDATE";
                }

                @Override
                public InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                                        final InternalMessage message)
                {
                    return performUpdateOperation(clazz, message);
                }
            };


    private final StandardOperation DELETE_OPERATION =
            new StandardOperation()
            {
                @Override
                public String getName()
                {
                    return "DELETE";
                }

                @Override
                public InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                                        final InternalMessage message)
                {
                    return performDeleteOperation(clazz, message);
                }
            };

    private final Set<String> STANDARD_OPERATIONS = Sets.newHashSet(CREATE_OPERATION.getName(),
                                                                    READ_OPERATION.getName(),
                                                                    UPDATE_OPERATION.getName(),
                                                                    DELETE_OPERATION.getName());

    private InternalMessage performOperation(final Class<? extends ConfiguredObject> clazz,
                                             final String operation,
                                             InternalMessage message)
    {
        try
        {
            if (STANDARD_OPERATIONS.contains(operation))
            {
                StandardOperation standardOperation;
                if ((standardOperation = _standardOperations.get(clazz).get(operation)) != null)
                {
                    return standardOperation.performOperation(clazz, message);
                }
            }
            else
            {
                final InternalMessageHeader requestHeader = message.getMessageHeader();
                final Map<String, Object> headers = requestHeader.getHeaderMap();

                ConfiguredObject<?> object = findObject(clazz, headers);
                if (object == null)
                {
                    return createFailureResponse(message, STATUS_CODE_NOT_FOUND, "Not found");
                }

                final Map<String, ConfiguredObjectOperation<?>> operations =
                        _model.getTypeRegistry().getOperations(object.getClass());
                @SuppressWarnings("unchecked") final ConfiguredObjectOperation<ConfiguredObject<?>> method =
                        (ConfiguredObjectOperation<ConfiguredObject<?>>) operations.get(operation);

                if (method != null)
                {
                    return performConfiguredObjectOperation(object, message, method);
                }
            }
            return createFailureResponse(message, STATUS_CODE_NOT_IMPLEMENTED, "Not implemented");

        }
        catch (RuntimeException e)
        {
            return createFailureResponse(message, STATUS_CODE_INTERNAL_ERROR, e.getMessage());
        }
    }

    private InternalMessage performDeleteOperation(final Class<? extends ConfiguredObject> clazz,
                                                   final InternalMessage message)
    {
        InternalMessageHeader requestHeader = message.getMessageHeader();
        final Map<String, Object> headers = requestHeader.getHeaderMap();

        ConfiguredObject<?> object = findObject(clazz, headers);
        if(object != null)
        {
            try
            {
                object.delete();

                final MutableMessageHeader responseHeader = new MutableMessageHeader();
                responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                        ? requestHeader.getMessageId()
                                                        : requestHeader.getCorrelationId());
                responseHeader.setMessageId(UUID.randomUUID().toString());
                responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_NO_CONTENT);

                return InternalMessage.createMapMessage(_addressSpace.getMessageStore(),
                                                        responseHeader,
                                                        Collections.emptyMap());
            }
            catch (IntegrityViolationException e)
            {
                return createFailureResponse(message, STATUS_CODE_FORBIDDEN, e.getMessage());
            }
        }
        else
        {
            return createFailureResponse(message, STATUS_CODE_NOT_FOUND, "Not Found");
        }
    }

    private InternalMessage performUpdateOperation(final Class<? extends ConfiguredObject> clazz,
                                                   final InternalMessage message)
    {
        InternalMessageHeader requestHeader = message.getMessageHeader();

        final Map<String, Object> headers = requestHeader.getHeaderMap();

        ConfiguredObject<?> object = findObject(clazz, headers);
        if(object != null)
        {
            if(message.getMessageBody() instanceof Map)
            {
                @SuppressWarnings("unchecked")
                final HashMap<String, Object> attributes = new HashMap<>((Map) message.getMessageBody());
                Object id = attributes.remove(IDENTITY_ATTRIBUTE);
                if (id != null && !String.valueOf(id).equals(object.getId().toString()))
                {
                    return createFailureResponse(message,
                                                 STATUS_CODE_FORBIDDEN,
                                                 "Cannot change the value of '" + IDENTITY_ATTRIBUTE + "'");
                }
                String path = (String) attributes.remove(OBJECT_PATH);
                Class<? extends ConfiguredObject> parentType = _model.getParentType(clazz);
                if(parentType != null)
                {
                    String attributeName = parentType.getSimpleName().toLowerCase();
                    final Object parentValue = attributes.remove(attributeName);
                    if (parentValue != null && !String.valueOf(parentValue)
                                                      .equals(object.getParent().getName()))
                    {
                        return createFailureResponse(message,
                                                     STATUS_CODE_FORBIDDEN,
                                                     "Cannot change the value of '" + attributeName + "'");
                    }
                }
                if (path != null && !attributes.containsKey(ConfiguredObject.NAME))
                {
                    String[] pathElements = getPathElements(path);
                    attributes.put(ConfiguredObject.NAME, pathElements[pathElements.length - 1]);
                }
                object.setAttributes(attributes);

                final MutableMessageHeader responseHeader = new MutableMessageHeader();
                responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                        ? requestHeader.getMessageId()
                                                        : requestHeader.getCorrelationId());
                responseHeader.setMessageId(UUID.randomUUID().toString());
                responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_OK);

                return InternalMessage.createMapMessage(_addressSpace.getMessageStore(), responseHeader,
                                                        _managementOutputConverter.convertToOutput(object, true));
            }
            else
            {
                return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "Message body must be a map");
            }
        }
        else
        {
            return createFailureResponse(message, STATUS_CODE_NOT_FOUND, "No such object");
        }
    }

    private String[] getPathElements(final String path)
    {
        String[] pathElements = path.split("(?<!\\\\)" + Pattern.quote("/"));
        for(int i = 0; i<pathElements.length; i++)
        {
            pathElements[i] = pathElements[i].replaceAll("\\\\(.)","$1");
        }
        return pathElements;
    }


    private InternalMessage performReadOperation(final Class<? extends ConfiguredObject> clazz,
                                                 final InternalMessage message)
    {
        InternalMessageHeader requestHeader = message.getMessageHeader();

        final Map<String, Object> headers = requestHeader.getHeaderMap();
        final boolean actuals = headers.get(ACTUALS_ATTRIBUTE) == null || Boolean.parseBoolean(String.valueOf(headers.get(ACTUALS_ATTRIBUTE)));

        ConfiguredObject<?> object = findObject(clazz, headers);
        if(object != null)
        {
            final MutableMessageHeader responseHeader = new MutableMessageHeader();
            responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                    ? requestHeader.getMessageId()
                                                    : requestHeader.getCorrelationId());
            responseHeader.setMessageId(UUID.randomUUID().toString());
            responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_OK);

            return InternalMessage.createMapMessage(_addressSpace.getMessageStore(), responseHeader,
                                                    _managementOutputConverter.convertToOutput(object, actuals));
        }
        else
        {
            return createFailureResponse(message,
                                         STATUS_CODE_NOT_FOUND,
                                             "Not found");
        }
    }

    private InternalMessage performCreateOperation(final Class<? extends ConfiguredObject> clazz,
                                                   final InternalMessage message)
    {
        InternalMessageHeader requestHeader = message.getMessageHeader();

        final MutableMessageHeader responseHeader = new MutableMessageHeader();
        responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                ? requestHeader.getMessageId()
                                                : requestHeader.getCorrelationId());
        responseHeader.setMessageId(UUID.randomUUID().toString());
        responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_CREATED);

        if(message.getMessageBody() instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<String,Object> attributes = (Map<String,Object>) message.getMessageBody();
            if(attributes.containsKey(IDENTITY_ATTRIBUTE))
            {
                return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "The '"+IDENTITY_ATTRIBUTE+"' cannot be set when creating an object");
            }
            if(attributes.containsKey(ConfiguredObject.ID))
            {
                return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "The '"+ConfiguredObject.ID+"' cannot be set when creating an object");
            }
            if(!attributes.containsKey(QPID_TYPE) && ConfiguredObjectTypeRegistry.getCategory(clazz) != clazz)
            {
                Class<? extends ConfiguredObject> typeClass = _model.getTypeRegistry().getTypeClass(clazz);
                String type = typeClass.getAnnotation(ManagedObject.class).type();
                if(!"".equals(type))
                {
                    attributes.put(QPID_TYPE, type);
                }
            }

            if(attributes.containsKey(OBJECT_PATH))
            {
                String path = String.valueOf(attributes.remove(OBJECT_PATH));

                ConfiguredObject theParent = _managedObject;

                final Class<? extends ConfiguredObject>[] hierarchy = _configuredObjectFinder.getHierarchy(clazz);
                if (hierarchy.length > 1)
                {

                    ConfiguredObject parent =
                            _configuredObjectFinder.findObjectParentsFromPath(Arrays.asList(getPathElements(path)), hierarchy, ConfiguredObjectTypeRegistry.getCategory(clazz));
                    if(parent == null)
                    {
                        return createFailureResponse(message, STATUS_CODE_NOT_FOUND, "The '"+OBJECT_PATH+"' "+path+" does not identify a valid parent");
                    }
                    theParent = parent;
                }
                return doCreate(clazz, message, responseHeader, attributes, theParent);

            }
            else if(_configuredObjectFinder.getHierarchy(clazz).length == 1 && attributes.containsKey(ConfiguredObject.NAME))
            {
                return doCreate(clazz, message, responseHeader, attributes, _managedObject);
            }
            else
            {
                return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "The '"+OBJECT_PATH+"' must be supplied");
            }
        }
        else
        {
            return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "Message body must be a map");
        }

    }

    private InternalMessage doCreate(final Class<? extends ConfiguredObject> clazz,
                                     final InternalMessage message,
                                     final MutableMessageHeader responseHeader,
                                     final Map<String, Object> attributes,
                                     final ConfiguredObject<?> parent)
    {
        try
        {
            ManagedObject annotation = clazz.getAnnotation(ManagedObject.class);
            if(!annotation.category() || !"".equals(annotation.defaultType()) || attributes.containsKey(QPID_TYPE) || _model.getTypeRegistry().getTypeSpecialisations(clazz).size()==1)
            {
                if(attributes.containsKey(QPID_TYPE))
                {
                    attributes.put(ConfiguredObject.TYPE, attributes.remove(QPID_TYPE));
                }
                else
                {
                    attributes.remove(TYPE_ATTRIBUTE);
                }


                final ConfiguredObject object = parent.createChild(ConfiguredObjectTypeRegistry.getCategory(clazz), attributes);
                return InternalMessage.createMapMessage(_addressSpace.getMessageStore(), responseHeader,
                                                        _managementOutputConverter.convertToOutput(object, true));
            }
            else
            {
                return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, "type: '"+getAmqpName(clazz)+"' requires the '"+QPID_TYPE+"' attribute");
            }
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            return createFailureResponse(message, STATUS_CODE_CONFLICT, "Object already exists with the same path");

        }
        catch (IllegalArgumentException | IllegalStateException | IllegalConfigurationException e)
        {
            return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, e.getMessage());
        }
        catch (AccessControlException e)
        {
            return createFailureResponse(message, STATUS_CODE_FORBIDDEN, "Forbidden");
        }
    }

    private InternalMessage performConfiguredObjectOperation(final ConfiguredObject<?> object,
                                                             final InternalMessage message,
                                                             final ConfiguredObjectOperation<ConfiguredObject<?>> method)
    {
        final InternalMessageHeader requestHeader = message.getMessageHeader();
        final Map<String, Object> headers = requestHeader.getHeaderMap();

        try
        {
            Map<String,Object> parameters = new HashMap<>(headers);
            parameters.remove(KEY_ATTRIBUTE);
            parameters.remove(IDENTITY_ATTRIBUTE);
            parameters.remove(TYPE_ATTRIBUTE);
            parameters.remove(INDEX_ATTRIBUTE);
            parameters.remove(OPERATION_HEADER);

            parameters.keySet().removeIf(paramName -> paramName.startsWith("JMS_QPID"));

            AmqpConnectionMetaData callerConnectionMetaData = getCallerConnectionMetaData();

            if (method.isSecure(object, parameters) && !(callerConnectionMetaData.getTransport().isSecure() || callerConnectionMetaData.getPort().isAllowConfidentialOperationsOnInsecureChannels()))
            {
                return createFailureResponse(message, STATUS_CODE_FORBIDDEN, "Operation '" + method.getName() + "' can only be performed over a secure (AMQPS) connection");
            }

            final MutableMessageHeader responseHeader = new MutableMessageHeader();
            responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                    ? requestHeader.getMessageId()
                                                    : requestHeader.getCorrelationId());
            responseHeader.setMessageId(UUID.randomUUID().toString());
            responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_OK);

            final Object result = method.perform(object, parameters);
            return createManagedOperationResponse(method, responseHeader, result);
        }
        catch (IllegalArgumentException | IllegalStateException | IllegalConfigurationException e)
        {
            return createFailureResponse(message, STATUS_CODE_BAD_REQUEST, e.getMessage());
        }
        catch (AccessControlException e)
        {
            return createFailureResponse(message, STATUS_CODE_FORBIDDEN, "Forbidden");
        }
    }

    private InternalMessage createManagedOperationResponse(final ConfiguredObjectOperation<ConfiguredObject<?>> method,
                                                           final MutableMessageHeader responseHeader,
                                                           final Object result)
    {
        Object convertedValue = _managementOutputConverter.convertObjectToOutput(result);
        if (convertedValue instanceof byte[])
        {
            return InternalMessage.createBytesMessage(_addressSpace.getMessageStore(), responseHeader,
                                                      (byte[]) convertedValue, false);
        }
        else if (convertedValue instanceof Map)
        {
            return InternalMessage.createMapMessage(_addressSpace.getMessageStore(), responseHeader,
                                                    ((Map<?,?>) convertedValue));
        }
        else if (convertedValue instanceof Serializable)
        {
            return InternalMessage.createMessage(_addressSpace.getMessageStore(), responseHeader,
                                                 (Serializable) convertedValue, false, null);
        }
        else
        {
            return InternalMessage.createBytesMessage(_addressSpace.getMessageStore(), responseHeader,
                                                      new byte[0], false);
        }
    }

    String generatePath(final ConfiguredObject<?> object)
    {
        return _configuredObjectFinder.getPath(object);
    }

    private ConfiguredObject<?> findObject(final Class<? extends ConfiguredObject> clazz,
                                           final Map<String, Object> headers)
    {
        if(headers.containsKey(IDENTITY_ATTRIBUTE))
        {
            Object value = headers.get(IDENTITY_ATTRIBUTE);
            UUID id;
            if(value instanceof UUID)
            {
                id= (UUID) value;
            }
            else if(value instanceof String)
            {
                id = UUID.fromString((String) value);
            }
            else
            {
                return null;
            }

            return findObjectById(id, clazz);
        }
        else if(headers.containsKey(INDEX_ATTRIBUTE))
        {
            Object index = headers.get(INDEX_ATTRIBUTE);
            if(OBJECT_PATH.equals(index))
            {
                return _configuredObjectFinder.findObjectFromPath(String.valueOf(headers.get(KEY_ATTRIBUTE)), clazz);
            }
            else
            {
                throw new IllegalArgumentException("Unknown index: '"+index+'"');
            }
        }
        else
        {
            throw new IllegalArgumentException("Either "+IDENTITY_ATTRIBUTE+" or "+INDEX_ATTRIBUTE+" must be specified");

        }
    }

    private ConfiguredObject<?> findObjectById(final UUID id, final Class<? extends ConfiguredObject> clazz)
    {
        Collection<Class<? extends ConfiguredObject>> ancestorCategories = _model.getAncestorCategories(clazz);
        if(ancestorCategories.contains(_managedObject.getCategoryClass()))
        {
            return findDescendantById(clazz, id, _managedObject.getCategoryClass(), Collections.singleton(_managedObject));
        }
        else
        {
            for(Map.Entry<Class<? extends ConfiguredObject>,ConfiguredObjectOperation<?>> entry : _associatedChildrenOperations.entrySet())
            {
                @SuppressWarnings("unchecked")
                ConfiguredObjectOperation<ConfiguredObject<?>> operation =
                        (ConfiguredObjectOperation<ConfiguredObject<?>>) entry.getValue();

                final Class<?> returnType = operation.getReturnType();
                if (clazz.isAssignableFrom(returnType) || ancestorCategories.contains(returnType))
                {
                    @SuppressWarnings("unchecked")
                    Collection<? extends ConfiguredObject> associated =
                            (Collection<? extends ConfiguredObject>) operation
                                    .perform(_managedObject,
                                             Collections.emptyMap());
                    ConfiguredObject<?> object = findDescendantById(clazz, id,
                                                                    entry.getKey(),
                                                                    associated);
                    if (object != null)
                    {
                        return object;
                    }
                }
                else if (returnsCollectionOfConfiguredObjects(operation))
                {
                    @SuppressWarnings("unchecked")
                    Class<? extends ConfiguredObject> associatedChildCategory =
                            getCollectionMemberType((ParameterizedType) operation.getGenericReturnType());

                    if (clazz.isAssignableFrom(associatedChildCategory)
                        || ancestorCategories.contains(associatedChildCategory))
                    {
                        @SuppressWarnings("unchecked")
                        Collection<? extends ConfiguredObject> associated =
                                (Collection<? extends ConfiguredObject>) operation
                                        .perform(_managedObject,
                                                 Collections.emptyMap());
                        ConfiguredObject<?> object = findDescendantById(clazz, id,
                                                                        entry.getKey(),
                                                                        associated);
                        if (object != null)
                        {
                            return object;
                        }
                    }
                }
            }
        }
        return null;
    }

    private ConfiguredObject<?> findDescendantById(final Class<? extends ConfiguredObject> category,
                                                   final UUID id,
                                                   final Class<? extends ConfiguredObject> rootCategory,
                                                   final Collection<? extends ConfiguredObject> roots)
    {
        if(category == rootCategory)
        {
            for(ConfiguredObject<?> root : roots)
            {
                if(root.getId().equals(id))
                {
                    return root;
                }
            }
        }
        else
        {
            if(_model.getChildTypes(rootCategory).contains(category))
            {
                for(ConfiguredObject<?> root : roots)
                {
                    final ConfiguredObject<?> child = root.getChildById(category, id);
                    if(child != null)
                    {
                        return child;
                    }
                }
            }
            else
            {
                Collection<Class<? extends ConfiguredObject>> ancestorCategories = _model.getAncestorCategories(category);
                for(Class<? extends ConfiguredObject> childClass : _model.getChildTypes(rootCategory))
                {
                    if(ancestorCategories.contains(childClass))
                    {
                        List<ConfiguredObject> newRoots = new ArrayList<>();
                        for(ConfiguredObject<?> root : roots)
                        {
                            newRoots.addAll(root.getChildren(childClass));
                        }
                        if(!newRoots.isEmpty())
                        {
                            final ConfiguredObject<?> child = findDescendantById(category, id, childClass, newRoots);
                            if(child != null)
                            {
                                return child;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private void sendResponse(final InternalMessage message, final InternalMessage response)
    {
        String replyTo = message.getMessageHeader().getReplyTo();
        response.setInitialRoutingAddress(replyTo);

        final MessageDestination responseDestination = getResponseDestination(replyTo);
        RoutingResult<InternalMessage> result = responseDestination.route(response, replyTo, InstanceProperties.EMPTY);
        result.send(new AutoCommitTransaction(_addressSpace.getMessageStore()), null);

    }

    private MessageDestination getResponseDestination(String replyTo)
    {
        ManagementNodeConsumer consumer = null;
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        Set<SessionPrincipal> sessionPrincipals = currentSubject.getPrincipals(SessionPrincipal.class);
        if (!sessionPrincipals.isEmpty())
        {
            AMQPSession<?,?> publishingSession = sessionPrincipals.iterator().next().getSession();
            for (ManagementNodeConsumer candidate : _consumers)
            {
                if (candidate.getTarget().getTargetAddress().equals(replyTo) && candidate.getSession().getConnectionReference() == publishingSession.getConnectionReference())
                {
                    consumer = candidate;
                    break;
                }
            }
        }
        return consumer == null ? _addressSpace.getDefaultDestination() : consumer;
    }


    private InternalMessage createFailureResponse(final InternalMessage requestMessage,
                                       final int statusCode,
                                       final String stateDescription,
                                       Object... params)
    {
        final InternalMessageHeader requestHeader = requestMessage.getMessageHeader();
        final MutableMessageHeader responseHeader = new MutableMessageHeader();
        responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                ? requestHeader.getMessageId()
                                                : requestHeader.getCorrelationId());
        responseHeader.setMessageId(UUID.randomUUID().toString());
        for(String header : requestHeader.getHeaderNames())
        {
            responseHeader.setHeader(header, requestHeader.getHeader(header));
        }
        responseHeader.setHeader(STATUS_CODE_HEADER, statusCode);
        responseHeader.setHeader(STATUS_DESCRIPTION_HEADER, params.length == 0 ? stateDescription : MessageFormat.format(stateDescription, params));
        return InternalMessage.createBytesMessage(_addressSpace.getMessageStore(), responseHeader, new byte[0]);

    }

    private InternalMessage performManagementOperation(String operation, final InternalMessage msg)
    {
        final InternalMessage responseMessage;
        final InternalMessageHeader requestHeader = msg.getMessageHeader();
        final MutableMessageHeader responseHeader = new MutableMessageHeader();
        responseHeader.setCorrelationId(requestHeader.getCorrelationId() == null
                                                ? requestHeader.getMessageId()
                                                : requestHeader.getCorrelationId());
        responseHeader.setMessageId(UUID.randomUUID().toString());

        Map<?, ?> result;
        if(GET_TYPES.equals(operation))
        {
            result = performGetTypes(requestHeader.getHeaderMap());
        }
        else if(GET_ATTRIBUTES.equals(operation))
        {
            result = performGetAttributes(requestHeader.getHeaderMap());
        }
        else if(GET_OPERATIONS.equals(operation))
        {
            result = performGetOperations(requestHeader.getHeaderMap());
        }
        else if(QUERY.equals(operation))
        {
            if(msg.getMessageBody() instanceof Map)
            {
                result = performQuery(requestHeader.getHeaderMap(), (Map)(msg.getMessageBody()));
            }
            else
            {
                return createFailureResponse(msg, STATUS_CODE_BAD_REQUEST, "Body of a QUERY operation must be a map");
            }
        }
        else
        {
            return createFailureResponse(msg, STATUS_CODE_NOT_IMPLEMENTED, "Unknown operation {}", operation);
        }
        responseHeader.setHeader(STATUS_CODE_HEADER, STATUS_CODE_OK);

        responseMessage = InternalMessage.createMapMessage(_addressSpace.getMessageStore(), responseHeader, result);

        return responseMessage;
    }

    private Map<?, ?> performQuery(final Map<String, Object> headerMap, final Map messageBody)
    {
        @SuppressWarnings("unchecked")
        List<Object> attributeNameObjects = (List<Object>)_managementInputConverter.convert(List.class, messageBody.get(ATTRIBUTE_NAMES));
        List<String> attributeNames;
        if(attributeNameObjects == null)
        {
            attributeNames = Collections.emptyList();
        }
        else
        {
            attributeNames = new ArrayList<>(attributeNameObjects.size());
            for(Object nameObject : attributeNameObjects)
            {
                if(nameObject == null)
                {
                    throw new IllegalArgumentException("All attribute names must be non-null");
                }
                else
                {
                    attributeNames.add(nameObject.toString());
                }
            }
        }

        String entityType = (String)headerMap.get(ENTITY_TYPE_HEADER);

        if(attributeNames.isEmpty())
        {
            attributeNames = generateAttributeNames(entityType);
        }

        List<ConfiguredObject<?>> objects = getObjects(entityType);
        objects.sort(OBJECT_COMPARATOR);
        if(headerMap.containsKey(OFFSET_HEADER))
        {
            int offset;
            if(headerMap.get(OFFSET_HEADER) instanceof Number)
            {
                offset = ((Number) headerMap.get(OFFSET_HEADER)).intValue();
            }
            else
            {
                offset = Integer.parseInt(headerMap.get(OFFSET_HEADER).toString());
            }
            if(offset >= 0)
            {
                if(offset < objects.size())
                {
                    objects = objects.subList(offset, objects.size());
                }
                else
                {
                    objects = Collections.emptyList();
                }
            }
            else if(objects.size() + offset > 0)
            {
                objects = objects.subList(objects.size()+offset, objects.size());
            }
        }

        if(headerMap.containsKey(COUNT_HEADER))
        {
            int count;
            if(headerMap.get(COUNT_HEADER) instanceof Number)
            {
                count = ((Number) headerMap.get(COUNT_HEADER)).intValue();
            }
            else
            {
                count = Integer.parseInt(headerMap.get(OFFSET_HEADER).toString());
            }
            if(count >= 0)
            {
                if(count < objects.size())
                {
                    objects = objects.subList(0, count);
                }
                else
                {
                    objects = Collections.emptyList();
                }
            }
            else if(objects.size() + count > 0)
            {
                objects = objects.subList(0, objects.size()+count);
            }
        }
        List<List<Object>> resultList = new ArrayList<>(objects.size());

        for(ConfiguredObject<?> object : objects)
        {
            List<Object> attributes = new ArrayList<>(attributeNames.size());
            Map<?,?> convertedObject = _managementOutputConverter.convertToOutput(object, true);
            for(String attributeName : attributeNames)
            {
                attributes.add(convertedObject.get(attributeName));
            }
            resultList.add(attributes);
        }
        Map<Object, Object> result = new LinkedHashMap<>();
        result.put(ATTRIBUTE_NAMES, attributeNames);
        result.put(RESULTS, resultList);
        return result;
    }

    private Collection<ConfiguredObject<?>> getChildrenOfType(ConfiguredObject<?> object, Class<? extends ConfiguredObject> type)
    {
        Set<ConfiguredObject<?>> children = new HashSet<>();
        Class<? extends ConfiguredObject> categoryClass = ConfiguredObjectTypeRegistry.getCategory(type);
        for(Class<? extends ConfiguredObject> childClass : _model.getChildTypes(object.getCategoryClass()))
        {
            if(childClass == categoryClass)
            {
                for (ConfiguredObject<?> child : object.getChildren(childClass))
                {
                    if(categoryClass == type || child.getTypeClass() == type)
                    {
                        children.add(child);
                    }
                }
            }
            else
            {
                if(_model.getAncestorCategories(categoryClass).contains(childClass))
                {
                    for(ConfiguredObject<?> child : object.getChildren(childClass))
                    {
                        children.addAll(getChildrenOfType(child, type));
                    }
                }
            }
        }
        return children;
    }

    private List<ConfiguredObject<?>> getObjects(final String entityType)
    {
        Set<ConfiguredObject<?>> foundObjects;

        if(entityType == null)
        {
            foundObjects = findAllChildren();

        }
        else
        {
            final Class<? extends ConfiguredObject> type = _managedTypes.get(entityType);
            if(type != null)
            {
                foundObjects = new HashSet<>();
                Collection<Class<? extends ConfiguredObject>> ancestorCategories =
                        _model.getAncestorCategories(ConfiguredObjectTypeRegistry.getCategory(type));
                if(ancestorCategories.contains(_managedObject.getCategoryClass()))
                {
                    foundObjects.addAll(getChildrenOfType(_managedObject, type));
                }

                for(Map.Entry<Class<? extends ConfiguredObject>, ConfiguredObjectOperation<?>> entry : _associatedChildrenOperations.entrySet())
                {
                    if(ancestorCategories.contains(entry.getKey()))
                    {
                        @SuppressWarnings("unchecked")
                        final Collection<ConfiguredObject<?>> parents = getAssociatedChildren(entry.getValue(), _managedObject);

                        for(ConfiguredObject<?> parent : parents)
                        {
                            foundObjects.addAll(getChildrenOfType(parent, type));
                        }
                    }
                }
            }
            else
            {
                throw new IllegalArgumentException("Unknown entity type: '"+entityType+"'");
            }
        }
        // TODO - get the objects

        return new ArrayList<>(foundObjects);
    }

    private Set<ConfiguredObject<?>> findAllChildren()
    {
        final Set<ConfiguredObject<?>> foundObjects;
        foundObjects = new HashSet<>();
        Set<ConfiguredObject<?>> parents = new HashSet<>();
        Set<ConfiguredObject<?>> children;

        parents.add(_managedObject);
        for(ConfiguredObjectOperation op : _associatedChildrenOperations.values())
        {

            @SuppressWarnings("unchecked")
            final Collection<ConfiguredObject<?>> associated = getAssociatedChildren(op, _managedObject);

            parents.addAll(associated);
        }
        foundObjects.addAll(parents);
        do
        {
            children = new HashSet<>();
            for(ConfiguredObject<?> parent : parents)
            {
                for(Class<? extends ConfiguredObject> childClass : _model.getChildTypes(parent.getCategoryClass()))
                {
                    children.addAll((Collection<? extends ConfiguredObject<?>>) parent.getChildren(childClass));
                }
            }
            parents = children;


        }
        while(foundObjects.addAll(parents));
        return foundObjects;
    }

    private static <C extends ConfiguredObject<?>> Collection<ConfiguredObject<?>> getAssociatedChildren(final ConfiguredObjectOperation<C> op, final ConfiguredObject<?> managedObject)
    {
        @SuppressWarnings("unchecked")
        final Collection<ConfiguredObject<?>> associated = (Collection<ConfiguredObject<?>>) op.perform((C)managedObject, Collections.emptyMap());
        return associated;
    }

    private List<String> generateAttributeNames(String entityType)
    {
        Set<String> attrNameSet = new HashSet<>();
        List<String> attributeNames = new ArrayList<>();
        for(String standardAttribute : Arrays.asList(IDENTITY_ATTRIBUTE, TYPE_ATTRIBUTE, QPID_TYPE, OBJECT_PATH))
        {
            attrNameSet.add(standardAttribute);
            attributeNames.add(standardAttribute);
        }
        final ConfiguredObjectTypeRegistry typeRegistry = _model.getTypeRegistry();
        List<Class<? extends ConfiguredObject>> classes = new ArrayList<>();

        if(entityType != null && !entityType.trim().equals(""))
        {
            Class<? extends ConfiguredObject> clazz = _managedTypes.get(entityType);
            if(clazz != null)
            {
                classes.add(clazz);
                if(ConfiguredObjectTypeRegistry.getCategory(clazz) == clazz)
                {
                    classes.addAll(_model.getTypeRegistry().getTypeSpecialisations(clazz));
                }
            }
        }
        else
        {
            for (Class<? extends ConfiguredObject> clazz : _managedCategories)
            {
                classes.add(clazz);
                classes.addAll(_model.getTypeRegistry().getTypeSpecialisations(clazz));
            }
        }
        for(Class<? extends ConfiguredObject> clazz : classes)
        {
            for(String name : typeRegistry.getAttributeNames(clazz))
            {
                if(!ConfiguredObject.ID.equals(name) && attrNameSet.add(name))
                {
                    attributeNames.add(name);
                }
            }
            final Class<? extends ConfiguredObject> category = ConfiguredObjectTypeRegistry.getCategory(clazz);
            if(category != _managedObject.getCategoryClass()
               && !isSyntheticChildClass(category))
            {
                Class<? extends ConfiguredObject> parentType = _model.getParentType(category);
                if (parentType != _managedObject.getCategoryClass())
                {
                    attributeNames.add(parentType.getSimpleName().toLowerCase());
                }
            }

        }

        return attributeNames;
    }

    private <T> Map<String, T> performManagementOperation(final Map<String,Object> requestHeader, TypeOperation<T> operation, T selfValue)
    {
        Map<String,T> responseMap = new LinkedHashMap<>();

        if(requestHeader.containsKey(ENTITY_TYPE_HEADER))
        {
            String entityType = (String)requestHeader.get(ENTITY_TYPE_HEADER);
            Class<? extends ConfiguredObject> clazz = _managedTypes.get(entityType);
            if(clazz != null)
            {
                responseMap.put(entityType, operation.evaluateType(clazz));
                if(ConfiguredObjectTypeRegistry.getCategory(clazz) == clazz)
                {
                    for(Class<? extends ConfiguredObject> type : _model.getTypeRegistry().getTypeSpecialisations(clazz))
                    {
                        if(type.isAnnotationPresent(ManagedObject.class))
                        {
                            responseMap.put(getAmqpName(type), operation.evaluateType(type));
                        }
                    }
                }
            }
            else if(MANAGEMENT_TYPE.equals(entityType))
            {
                responseMap.put(entityType, selfValue);
            }
        }
        else
        {

            for(Map.Entry<String, Class<? extends ConfiguredObject>> entry : _managedTypes.entrySet())
            {
                responseMap.put(entry.getKey(), operation.evaluateType(entry.getValue()));
            }
            responseMap.put(MANAGEMENT_TYPE, selfValue);
        }
        return responseMap;
    }

    private Map<String,List<String>> performGetTypes(final Map<String, Object> header)
    {
        return performManagementOperation(header,
                                          clazz ->
                                          {
                                              Class<? extends ConfiguredObject> category =
                                                      ConfiguredObjectTypeRegistry.getCategory(clazz);
                                              if(category == clazz)
                                              {
                                                  return Collections.emptyList();
                                              }
                                              else
                                              {
                                                  return Collections.singletonList(getAmqpName(category));
                                              }
                                          }, Collections.<String>emptyList());

    }

    private Map<String,List<String>> performGetAttributes(final Map<String, Object> headers)
    {
        return performManagementOperation(headers,
                                          clazz -> new ArrayList<>(_model.getTypeRegistry().getAttributeNames(clazz)), Collections.<String>emptyList());

    }


    private Map<String,Map<String,List<String>>> performGetOperations(final Map<String, Object> headers)
    {
        // TODO - enumerate management operations
        final Map<String, List<String>> managementOperations = new HashMap<>();

        return performManagementOperation(headers,
                                          clazz ->
                                          {
                                              final Map<String, ConfiguredObjectOperation<?>> operations =
                                                      _model.getTypeRegistry().getOperations(clazz);
                                              Map<String,List<String>> result = new HashMap<>();
                                              for(Map.Entry<String, ConfiguredObjectOperation<?>> operation : operations.entrySet())
                                              {

                                                  List<String> arguments = new ArrayList<>();
                                                  for(OperationParameter param : operation.getValue().getParameters())
                                                  {
                                                      arguments.add(param.getName());
                                                  }
                                                  result.put(operation.getKey(), arguments);
                                              }
                                              return result;
                                          }, managementOperations);

    }


    @Override
    public synchronized <T extends ConsumerTarget<T>> ManagementNodeConsumer<T> addConsumer(final T target,
                                                            final FilterManager filters,
                                                            final Class<? extends ServerMessage> messageClass,
                                                            final String consumerName,
                                                            final EnumSet<ConsumerOption> options,
                                                            final Integer priority)
    {

        final ManagementNodeConsumer<T> managementNodeConsumer = new ManagementNodeConsumer<>(consumerName,this, target);
        target.consumerAdded(managementNodeConsumer);
        _consumers.add(managementNodeConsumer);
        return managementNodeConsumer;
    }

    @Override
    public synchronized Collection<ManagementNodeConsumer> getConsumers()
    {
        return Collections.unmodifiableCollection(_consumers);
    }

    @Override
    public boolean verifySessionAccess(final AMQPSession<?,?> session)
    {
        return true;
    }

    @Override
    public void close()
    {
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _addressSpace;
    }


    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {
        // ? special permissions to publish to the management node
    }

    @Override
    public String getName()
    {
        return MANAGEMENT_NODE_NAME;
    }

    @Override
    public UUID getId()
    {
        return _id;
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.NEVER;
    }

    void unregisterConsumer(ManagementNodeConsumer managementNodeConsumer)
    {
        _consumers.remove(managementNodeConsumer);
    }

    @Override
    public MessageConversionExceptionHandlingPolicy getMessageConversionExceptionHandlingPolicy()
    {
        return MessageConversionExceptionHandlingPolicy.CLOSE;
    }

    private AmqpConnectionMetaData getCallerConnectionMetaData()
    {
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        Set<ConnectionPrincipal> connectionPrincipals = currentSubject.getPrincipals(ConnectionPrincipal.class);
        if (connectionPrincipals.isEmpty())
        {
            throw new IllegalStateException("Cannot find connection principal on calling thread");
        }

        ConnectionPrincipal principal = connectionPrincipals.iterator().next();
        return principal.getConnectionMetaData();
    }

    private class ConsumedMessageInstance implements MessageInstance
    {

        private final ServerMessage _message;

        ConsumedMessageInstance(final ServerMessage message)
        {
            _message = message;
        }

        @Override
        public int getDeliveryCount()
        {
            return 0;
        }

        @Override
        public void incrementDeliveryCount()
        {

        }

        @Override
        public void decrementDeliveryCount()
        {

        }

        @Override
        public void addStateChangeListener(final StateChangeListener<? super MessageInstance, EntryState> listener)
        {

        }

        @Override
        public boolean removeStateChangeListener(final StateChangeListener<? super MessageInstance, EntryState> listener)
        {
            return false;
        }


        @Override
        public boolean acquiredByConsumer()
        {
            return false;
        }

        @Override
        public MessageInstanceConsumer getAcquiringConsumer()
        {
            return null;
        }

        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return null;
        }

        @Override
        public boolean isAcquiredBy(final MessageInstanceConsumer<?> consumer)
        {
            return false;
        }

        @Override
        public boolean removeAcquisitionFromConsumer(final MessageInstanceConsumer<?> consumer)
        {
            return false;
        }

        @Override
        public void setRedelivered()
        {

        }

        @Override
        public boolean isRedelivered()
        {
            return false;
        }

        @Override
        public void reject(final MessageInstanceConsumer<?> consumer)
        {

        }

        @Override
        public boolean isRejectedBy(final MessageInstanceConsumer<?> consumer)
        {
            return false;
        }

        @Override
        public boolean getDeliveredToConsumer()
        {
            return true;
        }

        @Override
        public boolean expired()
        {
            return false;
        }

        @Override
        public boolean acquire(final MessageInstanceConsumer<?> consumer)
        {
            return false;
        }

        @Override
        public boolean makeAcquisitionUnstealable(final MessageInstanceConsumer<?> consumer)
        {
            return false;
        }

        @Override
        public boolean makeAcquisitionStealable()
        {
            return false;
        }

        @Override
        public int getMaximumDeliveryCount()
        {
            return 0;
        }

        @Override
        public int routeToAlternate(final Action<? super MessageInstance> action,
                                    final ServerTransaction txn,
                                    final Predicate<BaseQueue> predicate)
        {
            return 0;
        }


        @Override
        public Filterable asFilterable()
        {
            return null;
        }

        @Override
        public boolean isAvailable()
        {
            return false;
        }

        @Override
        public boolean acquire()
        {
            return false;
        }

        @Override
        public boolean isAcquired()
        {
            return false;
        }

        @Override
        public void release()
        {

        }

        @Override
        public void release(final MessageInstanceConsumer<?> consumer)
        {

        }

        @Override
        public void delete()
        {

        }

        @Override
        public boolean isDeleted()
        {
            return false;
        }

        @Override
        public boolean isHeld()
        {
            return false;
        }

        @Override
        public boolean isPersistent()
        {
            return false;
        }

        @Override
        public ServerMessage getMessage()
        {
            return _message;
        }

        @Override
        public InstanceProperties getInstanceProperties()
        {
            return CONSUMED_INSTANCE_PROPERTIES;
        }

        @Override
        public TransactionLogResource getOwningResource()
        {
            return ManagementNode.this;
        }
    }

    private static class MutableMessageHeader implements AMQMessageHeader
    {
        private final LinkedHashMap<String, Object> _headers = new LinkedHashMap<>();
        private String _correlationId;
        private String _messageId;

        void setCorrelationId(final String correlationId)
        {
            _correlationId = correlationId;
        }

        void setMessageId(final String messageId)
        {
            _messageId = messageId;
        }

        @Override
        public String getCorrelationId()
        {
            return _correlationId;
        }

        @Override
        public long getExpiration()
        {
            return 0L;
        }

        @Override
        public String getUserId()
        {
            return null;
        }

        @Override
        public String getAppId()
        {
            return null;
        }

        @Override
        public String getGroupId()
        {
            return null;
        }

        @Override
        public String getMessageId()
        {
            return _messageId;
        }

        @Override
        public String getMimeType()
        {
            return null;
        }

        @Override
        public String getEncoding()
        {
            return null;
        }

        @Override
        public byte getPriority()
        {
            return 4;
        }

        @Override
        public long getTimestamp()
        {
            return 0L;
        }

        @Override
        public long getNotValidBefore()
        {
            return 0L;
        }

        @Override
        public String getType()
        {
            return null;
        }

        @Override
        public String getReplyTo()
        {
            return null;
        }

        @Override
        public Object getHeader(final String name)
        {
            return _headers.get(name);
        }

        @Override
        public boolean containsHeaders(final Set<String> names)
        {
            return _headers.keySet().containsAll(names);
        }

        @Override
        public boolean containsHeader(final String name)
        {
            return _headers.containsKey(name);
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return Collections.unmodifiableCollection(_headers.keySet());
        }

        void setHeader(String header, Object value)
        {
            _headers.put(header,value);
        }

    }

    private interface TypeOperation<T>
    {
        T evaluateType(Class<? extends ConfiguredObject> operation);
    }

}
