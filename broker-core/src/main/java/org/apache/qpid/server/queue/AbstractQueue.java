/*
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
 */
package org.apache.qpid.server.queue;

import static org.apache.qpid.server.util.GZIPUtils.GZIP_CONTENT_ENCODING;
import static org.apache.qpid.server.util.ParameterizedTypes.MAP_OF_STRING_STRING;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.security.auth.Subject;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.logging.subjects.QueueLogSubject;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInfo;
import org.apache.qpid.server.message.MessageInfoImpl;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.RejectType;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.MessageFilterFactory;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.TransactionMonitor;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.server.util.DeleteDeleteTask;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.HouseKeepingTask;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public abstract class AbstractQueue<X extends AbstractQueue<X>>
        extends AbstractConfiguredObject<X>
        implements Queue<X>,
                   MessageGroupManager.ConsumerResetHelper,
                   TransactionMonitor
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueue.class);

    private static final QueueNotificationListener NULL_NOTIFICATION_LISTENER = new QueueNotificationListener()
    {
        @Override
        public void notifyClients(final NotificationCheck notification,
                                  final Queue queue,
                                  final String notificationMsg)
        {

        }
    };

    private static final String UTF8 = StandardCharsets.UTF_8.name();
    private static final Operation PUBLISH_ACTION = Operation.PERFORM_ACTION("publish");

    private final QueueManagingVirtualHost<?> _virtualHost;
    private final DeletedChildListener _deletedChildListener = new DeletedChildListener();

    private QueueConsumerManagerImpl _queueConsumerManager;

    @ManagedAttributeField( beforeSet = "preSetAlternateBinding", afterSet = "postSetAlternateBinding")
    private AlternateBinding _alternateBinding;

    private volatile QueueConsumer<?,?> _exclusiveSubscriber;

    private final AtomicInteger _activeSubscriberCount = new AtomicInteger();

    private final QueueStatistics _queueStatistics = new QueueStatistics();

    /** max allowed size(KB) of a single message */
    @ManagedAttributeField( afterSet = "updateAlertChecks" )
    private long _alertThresholdMessageSize;

    /** max allowed number of messages on a queue. */
    @ManagedAttributeField( afterSet = "updateAlertChecks" )
    private long _alertThresholdQueueDepthMessages;

    /** max queue depth for the queue */
    @ManagedAttributeField( afterSet = "updateAlertChecks" )
    private long _alertThresholdQueueDepthBytes;

    /** maximum message age before alerts occur */
    @ManagedAttributeField( afterSet = "updateAlertChecks" )
    private long _alertThresholdMessageAge;

    /** the minimum interval between sending out consecutive alerts of the same type */
    @ManagedAttributeField
    private long _alertRepeatGap;

    @ManagedAttributeField
    private ExclusivityPolicy _exclusive;

    @ManagedAttributeField
    private MessageDurability _messageDurability;

    @ManagedAttributeField
    private Map<String, Map<String,List<String>>> _defaultFilters;

    private Object _exclusiveOwner; // could be connection, session, Principal or a String for the container name

    private final Set<NotificationCheck> _notificationChecks =
            Collections.synchronizedSet(EnumSet.noneOf(NotificationCheck.class));

    private AtomicBoolean _stopped = new AtomicBoolean(false);

    private final AtomicBoolean _deleted = new AtomicBoolean(false);
    private final SettableFuture<Integer> _deleteQueueDepthFuture = SettableFuture.create();

    private final List<Action<? super X>> _deleteTaskList = new CopyOnWriteArrayList<>();

    private LogSubject _logSubject;

    @ManagedAttributeField
    private boolean _noLocal;

    private final CopyOnWriteArrayList<Binding> _bindings = new CopyOnWriteArrayList<>();
    private Map<String, Object> _arguments;

    /** the maximum delivery count for each message on this queue or 0 if maximum delivery count is not to be enforced. */
    @ManagedAttributeField
    private int _maximumDeliveryAttempts;

    private MessageGroupManager _messageGroupManager;

    private final ConcurrentMap<MessageSender, Integer> _linkedSenders = new ConcurrentHashMap<>();


    private QueueNotificationListener  _notificationListener = NULL_NOTIFICATION_LISTENER;
    private final long[] _lastNotificationTimes = new long[NotificationCheck.values().length];

    @ManagedAttributeField
    private String _messageGroupKeyOverride;
    @ManagedAttributeField
    private boolean _messageGroupSharedGroups;
    @ManagedAttributeField
    private MessageGroupType _messageGroupType;
    @ManagedAttributeField
    private String _messageGroupDefaultGroup;
    @ManagedAttributeField
    private int _maximumDistinctGroups;
    @ManagedAttributeField(afterSet = "queueMessageTtlChanged")
    private long _minimumMessageTtl;
    @ManagedAttributeField(afterSet = "queueMessageTtlChanged")
    private long _maximumMessageTtl;
    @ManagedAttributeField
    private boolean _ensureNondestructiveConsumers;
    @ManagedAttributeField
    private volatile boolean _holdOnPublishEnabled;

    @ManagedAttributeField()
    private OverflowPolicy _overflowPolicy;
    @ManagedAttributeField
    private long _maximumQueueDepthMessages;
    @ManagedAttributeField
    private long _maximumQueueDepthBytes;
    @ManagedAttributeField
    private CreatingLinkInfo _creatingLinkInfo;

    @ManagedAttributeField
    private ExpiryPolicy _expiryPolicy;

    @ManagedAttributeField
    private volatile int _maximumLiveConsumers;

    private static final AtomicIntegerFieldUpdater<AbstractQueue> LIVE_CONSUMERS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractQueue.class, "_liveConsumers");
    private volatile int _liveConsumers;

    private static final int RECOVERING = 1;
    private static final int COMPLETING_RECOVERY = 2;
    private static final int RECOVERED = 3;

    private final AtomicInteger _recovering = new AtomicInteger(RECOVERING);
    private final AtomicInteger _enqueuingWhileRecovering = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<EnqueueRequest> _postRecoveryQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String, Callable<MessageFilter>> _defaultFiltersMap = new ConcurrentHashMap<>();
    private final List<HoldMethod> _holdMethods = new CopyOnWriteArrayList<>();
    private final Set<DestinationReferrer> _referrers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<LocalTransaction> _transactions = ConcurrentHashMap.newKeySet();
    private final LocalTransaction.LocalTransactionListener _localTransactionListener = _transactions::remove;

    private boolean _closing;
    private Map<String, String> _mimeTypeToFileExtension = Collections.emptyMap();
    private AdvanceConsumersTask _queueHouseKeepingTask;
    private volatile int _bindingCount;
    private volatile RejectPolicyHandler _rejectPolicyHandler;
    private volatile OverflowPolicyHandler _postEnqueueOverflowPolicyHandler;
    private long _flowToDiskThreshold;
    private volatile MessageDestination _alternateBindingDestination;
    private volatile MessageConversionExceptionHandlingPolicy _messageConversionExceptionHandlingPolicy;

    private interface HoldMethod
    {
        boolean isHeld(MessageReference<?> message, long evaluationTime);
    }

    protected AbstractQueue(Map<String, Object> attributes, QueueManagingVirtualHost<?> virtualHost)
    {
        super(virtualHost, attributes);
        _queueConsumerManager = new QueueConsumerManagerImpl(this);

        _virtualHost = virtualHost;
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();

        if(isDurable() && (getLifetimePolicy()  == LifetimePolicy.DELETE_ON_CONNECTION_CLOSE
                            || getLifetimePolicy() == LifetimePolicy.DELETE_ON_SESSION_END))
        {
            Subject.doAs(getSubjectWithAddedSystemRights(),
                         (PrivilegedAction<Object>) () -> {
                             setAttributes(Collections.<String, Object>singletonMap(AbstractConfiguredObject.DURABLE,
                                                                                    false));
                             return null;
                         });
        }

        if(!isDurable() && getMessageDurability() != MessageDurability.NEVER)
        {
            Subject.doAs(getSubjectWithAddedSystemRights(),
                         (PrivilegedAction<Object>) () -> {
                             setAttributes(Collections.<String, Object>singletonMap(Queue.MESSAGE_DURABILITY,
                                                                                    MessageDurability.NEVER));
                             return null;
                         });
        }

        validateOrCreateAlternateBinding(this, true);
        _recovering.set(RECOVERED);
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        if (getCreatingLinkInfo() != null && !isSystemProcess())
        {
            throw new IllegalConfigurationException(String.format("Cannot specify creatingLinkInfo for queue '%s'", getName()));
        }
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        Double flowResumeLimit = getContextValue(Double.class, QUEUE_FLOW_RESUME_LIMIT);
        if (flowResumeLimit != null && (flowResumeLimit < 0.0 || flowResumeLimit > 100.0))
        {
            throw new IllegalConfigurationException("Flow resume limit value cannot be greater than 100 or lower than 0");
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        Map<String,Object> attributes = getActualAttributes();

        final LinkedHashMap<String, Object> arguments = new LinkedHashMap<>(attributes);

        arguments.put(Queue.EXCLUSIVE, _exclusive);
        arguments.put(Queue.LIFETIME_POLICY, getLifetimePolicy());

        _arguments = Collections.synchronizedMap(arguments);

        _logSubject = new QueueLogSubject(this);

        _queueHouseKeepingTask = new AdvanceConsumersTask();
        Subject activeSubject = Subject.getSubject(AccessController.getContext());
        Set<SessionPrincipal> sessionPrincipals = activeSubject == null ? Collections.emptySet() : activeSubject.getPrincipals(SessionPrincipal.class);
        AMQPSession<?, ?> session;
        if(sessionPrincipals.isEmpty())
        {
            session = null;
        }
        else
        {
            final SessionPrincipal sessionPrincipal = sessionPrincipals.iterator().next();
            session = sessionPrincipal.getSession();
        }

        if(session != null)
        {

            switch(_exclusive)
            {

                case PRINCIPAL:
                    _exclusiveOwner = session.getAMQPConnection().getAuthorizedPrincipal();
                    break;
                case CONTAINER:
                    _exclusiveOwner = session.getAMQPConnection().getRemoteContainerName();
                    break;
                case CONNECTION:
                    _exclusiveOwner = session.getAMQPConnection();
                    addExclusivityConstraint(session.getAMQPConnection());
                    break;
                case SESSION:
                    _exclusiveOwner = session;
                    addExclusivityConstraint(session);
                    break;
                case NONE:
                case LINK:
                case SHARED_SUBSCRIPTION:
                    break;
                default:
                    throw new ServerScopedRuntimeException("Unknown exclusivity policy: "
                                                           + _exclusive
                                                           + " this is a coding error inside Qpid");
            }
        }
        else if(_exclusive == ExclusivityPolicy.PRINCIPAL)
        {
            if (attributes.get(Queue.OWNER) != null)
            {
                String owner = String.valueOf(attributes.get(Queue.OWNER));
                Principal ownerPrincipal;
                try
                {
                    ownerPrincipal = new GenericPrincipal(owner);
                }
                catch (IllegalArgumentException e)
                {
                    ownerPrincipal = new GenericPrincipal(owner + "@('')");
                }
                _exclusiveOwner = new AuthenticatedPrincipal(ownerPrincipal);
            }
        }
        else if(_exclusive == ExclusivityPolicy.CONTAINER)
        {
            if (attributes.get(Queue.OWNER) != null)
            {
                _exclusiveOwner = String.valueOf(attributes.get(Queue.OWNER));
            }
        }


        if(getLifetimePolicy() == LifetimePolicy.DELETE_ON_CONNECTION_CLOSE)
        {
            if(session != null)
            {
                addLifetimeConstraint(session.getAMQPConnection());
            }
            else
            {
                throw new IllegalArgumentException("Queues created with a lifetime policy of "
                                                   + getLifetimePolicy()
                                                   + " must be created from a connection.");
            }
        }
        else if(getLifetimePolicy() == LifetimePolicy.DELETE_ON_SESSION_END)
        {
            if(session != null)
            {
                addLifetimeConstraint(session);
            }
            else
            {
                throw new IllegalArgumentException("Queues created with a lifetime policy of "
                                                   + getLifetimePolicy()
                                                   + " must be created from a connection.");
            }
        }
        else if (getLifetimePolicy() == LifetimePolicy.DELETE_ON_CREATING_LINK_CLOSE)
        {
            if (_creatingLinkInfo != null)
            {
                final LinkModel link;
                if (_creatingLinkInfo.isSendingLink())
                {
                    link = _virtualHost.getSendingLink(_creatingLinkInfo.getRemoteContainerId(), _creatingLinkInfo.getLinkName());
                }
                else
                {
                    link = _virtualHost.getReceivingLink(_creatingLinkInfo.getRemoteContainerId(), _creatingLinkInfo.getLinkName());
                }
                addLifetimeConstraint(link);
            }
            else
            {
                throw new IllegalArgumentException("Queues created with a lifetime policy of "
                                                   + getLifetimePolicy()
                                                   + " must be created from a AMQP 1.0 link.");
            }
        }


        // Log the creation of this Queue.
        // The priorities display is toggled on if we set priorities > 0
        getEventLogger().message(_logSubject,
                                 getCreatedLogMessage());

        switch(getMessageGroupType())
        {
            case NONE:
                _messageGroupManager = null;
                break;
            case STANDARD:
                _messageGroupManager = new AssignedConsumerMessageGroupManager(getMessageGroupKeyOverride(), getMaximumDistinctGroups());
                break;
            case SHARED_GROUPS:
                _messageGroupManager =
                        new DefinedGroupMessageGroupManager(getMessageGroupKeyOverride(), getMessageGroupDefaultGroup(), this);
                break;
            default:
                throw new IllegalArgumentException("Unknown messageGroupType type " + _messageGroupType);
        }

        _mimeTypeToFileExtension = getContextValue(Map.class, MAP_OF_STRING_STRING, MIME_TYPE_TO_FILE_EXTENSION);
        _messageConversionExceptionHandlingPolicy = getContextValue(MessageConversionExceptionHandlingPolicy.class, MESSAGE_CONVERSION_EXCEPTION_HANDLING_POLICY);

        _flowToDiskThreshold = getAncestor(Broker.class).getFlowToDiskThreshold();

        if(_defaultFilters != null)
        {
            QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
            final Map<String, MessageFilterFactory> messageFilterFactories =
                    qpidServiceLoader.getInstancesByType(MessageFilterFactory.class);

            for (Map.Entry<String,Map<String,List<String>>> entry : _defaultFilters.entrySet())
            {
                String name = String.valueOf(entry.getKey());
                Map<String, List<String>> filterValue = entry.getValue();
                if(filterValue.size() == 1)
                {
                    String filterTypeName = String.valueOf(filterValue.keySet().iterator().next());
                    final MessageFilterFactory filterFactory = messageFilterFactories.get(filterTypeName);
                    if(filterFactory != null)
                    {
                        final List<String> filterArguments = filterValue.values().iterator().next();
                        // check the arguments are valid
                        filterFactory.newInstance(filterArguments);
                        _defaultFiltersMap.put(name, new Callable<MessageFilter>()
                        {
                            @Override
                            public MessageFilter call()
                            {
                                return filterFactory.newInstance(filterArguments);
                            }
                        });
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unknown filter type " + filterTypeName + ", known types are: " + messageFilterFactories.keySet());
                    }
                }
                else
                {
                    throw new IllegalArgumentException("Filter value should be a map with one entry, having the type as key and the value being the filter arguments, not " + filterValue);

                }

            }
        }

        if(isHoldOnPublishEnabled())
        {
            _holdMethods.add(new HoldMethod()
                            {
                                @Override
                                public boolean isHeld(final MessageReference<?> messageReference, final long evaluationTime)
                                {
                                    return messageReference.getMessage().getMessageHeader().getNotValidBefore() >= evaluationTime;
                                }
                            });
        }

        if (getAlternateBinding() != null)
        {
            String alternateDestination = getAlternateBinding().getDestination();
            _alternateBindingDestination = getOpenedMessageDestination(alternateDestination);
            if (_alternateBindingDestination != null)
            {
                _alternateBindingDestination.addReference(this);
            }
            else
            {
                LOGGER.warn("Cannot find alternate binding destination '{}' for queue '{}'", alternateDestination, toString());
            }
        }

        createOverflowPolicyHandlers(_overflowPolicy);

        updateAlertChecks();
    }

    private void createOverflowPolicyHandlers(final OverflowPolicy overflowPolicy)
    {
        RejectPolicyHandler rejectPolicyHandler = null;
        OverflowPolicyHandler overflowPolicyHandler;
        switch (overflowPolicy)
        {
            case RING:
                overflowPolicyHandler = new RingOverflowPolicyHandler(this, getEventLogger());
                break;
            case PRODUCER_FLOW_CONTROL:
                overflowPolicyHandler = new ProducerFlowControlOverflowPolicyHandler(this, getEventLogger());
                break;
            case FLOW_TO_DISK:
                overflowPolicyHandler = new FlowToDiskOverflowPolicyHandler(this);
                break;
            case NONE:
                overflowPolicyHandler = new NoneOverflowPolicyHandler();
                break;
            case REJECT:
                overflowPolicyHandler = new NoneOverflowPolicyHandler();
                rejectPolicyHandler = new RejectPolicyHandler(this);
                break;
            default:
                throw new IllegalStateException(String.format("Overflow policy '%s' is not implemented",
                                                              overflowPolicy.name()));
        }

        _rejectPolicyHandler = rejectPolicyHandler;
        _postEnqueueOverflowPolicyHandler = overflowPolicyHandler;
    }

    protected LogMessage getCreatedLogMessage()
    {
        String ownerString = getOwner();
        return QueueMessages.CREATED(getId().toString(),
                                     ownerString,
                                     0,
                                     ownerString != null,
                                     getLifetimePolicy() != LifetimePolicy.PERMANENT,
                                     isDurable(),
                                     !isDurable(),
                                     false);
    }

    private MessageDestination getOpenedMessageDestination(final String name)
    {
        MessageDestination destination = getVirtualHost().getSystemDestination(name);
        if(destination == null)
        {
            destination = getVirtualHost().getChildByName(Exchange.class, name);
        }

        if(destination == null)
        {
            destination = getVirtualHost().getChildByName(Queue.class, name);
        }
        return destination;
    }

    private void addLifetimeConstraint(final Deletable<? extends Deletable> lifetimeObject)
    {
        final Action<Deletable> deleteQueueTask = object -> Subject.doAs(getSubjectWithAddedSystemRights(),
                                                                         (PrivilegedAction<Void>) () ->
                                                                         {
                                                                             AbstractQueue.this.delete();
                                                                             return null;
                                                                         });

        lifetimeObject.addDeleteTask(deleteQueueTask);
        addDeleteTask(new DeleteDeleteTask(lifetimeObject, deleteQueueTask));
    }

    private void addExclusivityConstraint(final Deletable<? extends Deletable> lifetimeObject)
    {
        final ClearOwnerAction clearOwnerAction = new ClearOwnerAction(lifetimeObject);
        final DeleteDeleteTask deleteDeleteTask = new DeleteDeleteTask(lifetimeObject, clearOwnerAction);
        clearOwnerAction.setDeleteTask(deleteDeleteTask);
        lifetimeObject.addDeleteTask(clearOwnerAction);
        addDeleteTask(deleteDeleteTask);
    }

    // ------ Getters and Setters

    @Override
    public boolean isExclusive()
    {
        return _exclusive != ExclusivityPolicy.NONE;
    }

    @Override
    public AlternateBinding getAlternateBinding()
    {
        return _alternateBinding;
    }

    public void setAlternateBinding(AlternateBinding alternateBinding)
    {
        _alternateBinding = alternateBinding;
    }

    @SuppressWarnings("unused")
    private void postSetAlternateBinding()
    {
        if(_alternateBinding != null)
        {
            _alternateBindingDestination = getOpenedMessageDestination(_alternateBinding.getDestination());
            if (_alternateBindingDestination != null)
            {
                _alternateBindingDestination.addReference(this);
            }
        }
    }

    @SuppressWarnings("unused")
    private void preSetAlternateBinding()
    {
        if(_alternateBindingDestination != null)
        {
            _alternateBindingDestination.removeReference(this);
        }
    }

    @Override
    public MessageDestination getAlternateBindingDestination()
    {
        return _alternateBindingDestination;
    }

    @Override
    public Map<String, Map<String, List<String>>> getDefaultFilters()
    {
        return _defaultFilters;
    }

    @Override
    public final MessageDurability getMessageDurability()
    {
        return _messageDurability;
    }

    @Override
    public long getMinimumMessageTtl()
    {
        return _minimumMessageTtl;
    }

    @Override
    public long getMaximumMessageTtl()
    {
        return _maximumMessageTtl;
    }

    @Override
    public boolean isEnsureNondestructiveConsumers()
    {
        return _ensureNondestructiveConsumers;
    }

    @Override
    public boolean isHoldOnPublishEnabled()
    {
        return _holdOnPublishEnabled;
    }

    @Override
    public long getMaximumQueueDepthMessages()
    {
        return _maximumQueueDepthMessages;
    }

    @Override
    public long getMaximumQueueDepthBytes()
    {
        return _maximumQueueDepthBytes;
    }

    @Override
    public ExpiryPolicy getExpiryPolicy()
    {
        return _expiryPolicy;
    }

    @Override
    public Collection<String> getAvailableAttributes()
    {
        return new ArrayList<>(_arguments.keySet());
    }

    @Override
    public String getOwner()
    {
        if(_exclusiveOwner != null)
        {
            switch(_exclusive)
            {
                case CONTAINER:
                    return (String) _exclusiveOwner;
                case PRINCIPAL:
                    return ((Principal)_exclusiveOwner).getName();
            }
        }
        return null;
    }

    @Override
    public CreatingLinkInfo getCreatingLinkInfo()
    {
        return _creatingLinkInfo;
    }

    @Override
    public QueueManagingVirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    // ------ Manage Consumers


    @Override
    public <T extends ConsumerTarget<T>> QueueConsumerImpl<T> addConsumer(final T target,
                                         final FilterManager filters,
                                         final Class<? extends ServerMessage> messageClass,
                                         final String consumerName,
                                         final EnumSet<ConsumerOption> optionSet,
                                         final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {

        try
        {
            final QueueConsumerImpl<T> queueConsumer = getTaskExecutor().run(new Task<QueueConsumerImpl<T>, Exception>()
            {
                @Override
                public QueueConsumerImpl<T> execute() throws Exception
                {
                    return addConsumerInternal(target, filters, messageClass, consumerName, optionSet, priority);
                }

                @Override
                public String getObject()
                {
                    return AbstractQueue.this.toString();
                }

                @Override
                public String getAction()
                {
                    return "add consumer";
                }

                @Override
                public String getArguments()
                {
                    return "target=" + target + ", consumerName=" + consumerName + ", optionSet=" + optionSet;
                }
            });

            target.consumerAdded(queueConsumer);
            if(isEmpty() || queueConsumer.isNonLive())
            {
                target.noMessagesAvailable();
            }
            target.updateNotifyWorkDesired();
            target.notifyWork();
            return queueConsumer;
        }
        catch (ExistingExclusiveConsumer | ConsumerAccessRefused
                | ExistingConsumerPreventsExclusive | QueueDeleted
                | RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            // Should never happen
            throw new ServerScopedRuntimeException(e);
        }


    }

    private <T extends ConsumerTarget<T>> QueueConsumerImpl<T> addConsumerInternal(final T target,
                                                  FilterManager filters,
                                                  final Class<? extends ServerMessage> messageClass,
                                                  final String consumerName,
                                                  EnumSet<ConsumerOption> optionSet,
                                                  final Integer priority)
            throws ExistingExclusiveConsumer, ConsumerAccessRefused,
                   ExistingConsumerPreventsExclusive, QueueDeleted
    {
        if (isDeleted())
        {
            throw new QueueDeleted();
        }

        if (hasExclusiveConsumer())
        {
            throw new ExistingExclusiveConsumer();
        }

        Object exclusiveOwner = _exclusiveOwner;
        final AMQPSession<?, T> session = target.getSession();
        switch(_exclusive)
        {
            case CONNECTION:
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = session.getAMQPConnection();
                    addExclusivityConstraint(session.getAMQPConnection());
                }
                else
                {
                    if(exclusiveOwner != session.getAMQPConnection())
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case SESSION:
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = session;
                    addExclusivityConstraint(session);
                }
                else
                {
                    if(exclusiveOwner != session)
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case LINK:
                if(getConsumerCount() != 0)
                {
                    throw new ConsumerAccessRefused();
                }
                break;
            case PRINCIPAL:
                Principal currentAuthorizedPrincipal = session.getAMQPConnection().getAuthorizedPrincipal();
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = currentAuthorizedPrincipal;
                }
                else
                {
                    if(!Objects.equals(((Principal) exclusiveOwner).getName(), currentAuthorizedPrincipal.getName()))
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case CONTAINER:
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = session.getAMQPConnection().getRemoteContainerName();
                }
                else
                {
                    if(!exclusiveOwner.equals(session.getAMQPConnection().getRemoteContainerName()))
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case SHARED_SUBSCRIPTION:
                break;
            case NONE:
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown exclusivity policy " + _exclusive);
        }

        boolean exclusive =  optionSet.contains(ConsumerOption.EXCLUSIVE);
        boolean isTransient =  optionSet.contains(ConsumerOption.TRANSIENT);

        if(_noLocal && !optionSet.contains(ConsumerOption.NO_LOCAL))
        {
            optionSet = EnumSet.copyOf(optionSet);
            optionSet.add(ConsumerOption.NO_LOCAL);
        }

        if(exclusive && getConsumerCount() != 0)
        {
            throw new ExistingConsumerPreventsExclusive();
        }
        if(!_defaultFiltersMap.isEmpty())
        {
            if(filters == null)
            {
                filters = new FilterManager();
            }
            for (Map.Entry<String,Callable<MessageFilter>> filter : _defaultFiltersMap.entrySet())
            {
                if(!filters.hasFilter(filter.getKey()))
                {
                    MessageFilter f;
                    try
                    {
                        f = filter.getValue().call();
                    }
                    catch (Exception e)
                    {
                        if (e instanceof RuntimeException)
                        {
                            throw (RuntimeException) e;
                        }
                        else
                        {
                            // Should never happen
                            throw new ServerScopedRuntimeException(e);
                        }
                    }
                    filters.add(filter.getKey(), f);
                }
            }
        }

        if(_ensureNondestructiveConsumers)
        {
            optionSet = EnumSet.copyOf(optionSet);
            optionSet.removeAll(EnumSet.of(ConsumerOption.SEES_REQUEUES, ConsumerOption.ACQUIRES));
        }

        QueueConsumerImpl<T> consumer = new QueueConsumerImpl<>(this,
                                                           target,
                                                           consumerName,
                                                           filters,
                                                           messageClass,
                                                           optionSet,
                                                           priority);

        _exclusiveOwner = exclusiveOwner;

        if (exclusive && !isTransient)
        {
            _exclusiveSubscriber = consumer;
        }

        QueueContext queueContext;
        if(filters == null || !filters.startAtTail())
        {
            queueContext = new QueueContext(getEntries().getHead());
        }
        else
        {
            queueContext = new QueueContext(getEntries().getTail());
        }
        consumer.setQueueContext(queueContext);
        if (_maximumLiveConsumers > 0 && !incrementNumberOfLiveConsumersIfApplicable())
        {
            consumer.setNonLive(true);
        }

        _queueConsumerManager.addConsumer(consumer);
        if (consumer.isNotifyWorkDesired())
        {
            _activeSubscriberCount.incrementAndGet();
        }

        childAdded(consumer);
        consumer.addChangeListener(_deletedChildListener);

        session.consumerAdded(consumer);
        addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
            {
                if (child.equals(consumer))
                {
                    session.consumerRemoved(consumer);
                    removeChangeListener(this);
                }
            }
        });

        return consumer;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _closing = true;
        return super.beforeClose();
    }



    <T extends ConsumerTarget<T>> void unregisterConsumer(final QueueConsumerImpl<T> consumer)
    {
        if (consumer == null)
        {
            throw new NullPointerException("consumer argument is null");
        }

        boolean removed = _queueConsumerManager.removeConsumer(consumer);

        if (removed)
        {
            consumer.closeAsync();
            // No longer can the queue have an exclusive consumer
            clearExclusiveSubscriber();

            consumer.setQueueContext(null);

            if(_exclusive == ExclusivityPolicy.LINK)
            {
                _exclusiveOwner = null;
            }

            if(_messageGroupManager != null)
            {
                resetSubPointersForGroups(consumer);
            }

            if (_maximumLiveConsumers > 0 && !consumer.isNonLive())
            {
                decrementNumberOfLiveConsumersIfApplicable();
                consumer.setNonLive(true);
                assignNextLiveConsumerIfApplicable();
            }

            // auto-delete queues must be deleted if there are no remaining subscribers

            if(!consumer.isTransient()
               && ( getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS
                    || getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_LINKS )
               && getConsumerCount() == 0
               && !(consumer.isDurable() && _closing))
            {

                LOGGER.debug("Auto-deleting queue: {}", this);

                Subject.doAs(getSubjectWithAddedSystemRights(), (PrivilegedAction<Object>) () -> {
                    AbstractQueue.this.delete();
                    return null;
                });


                // we need to manually fire the event to the removed consumer (which was the last one left for this
                // queue. This is because the delete method uses the consumer set which has just been cleared
                consumer.queueDeleted();

            }
        }

    }

    private boolean incrementNumberOfLiveConsumersIfApplicable()
    {
        // this level of care over concurrency in maintaining the correct value for live consumers is probable not
        // necessary, as all this should take place serially in the configuration thread
        int maximumLiveConsumers = _maximumLiveConsumers;
        boolean added = false;
        int liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
        while (liveConsumers < maximumLiveConsumers)
        {
            if (LIVE_CONSUMERS_UPDATER.compareAndSet(this, liveConsumers, liveConsumers + 1))
            {
                added = true;
                break;
            }
            liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
        }

        return added;
    }

    private boolean decrementNumberOfLiveConsumersIfApplicable()
    {
        // this level of care over concurrency in maintaining the correct value for live consumers is probable not
        // necessary, as all this should take place serially in the configuration thread
        boolean updated = false;
        int liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
        while (liveConsumers > 0)
        {
            if (LIVE_CONSUMERS_UPDATER.compareAndSet(this, liveConsumers, liveConsumers - 1))
            {
                updated = true;
                break;
            }
            liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
        }
        return updated;
    }

    private void assignNextLiveConsumerIfApplicable()
    {
        int maximumLiveConsumers = _maximumLiveConsumers;
        int liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
        final Iterator<QueueConsumer<?, ?>> consumerIterator = _queueConsumerManager.getAllIterator();

        QueueConsumerImpl<?> otherConsumer;
        while (consumerIterator.hasNext() && liveConsumers < maximumLiveConsumers)
        {
            otherConsumer = (QueueConsumerImpl<?>) consumerIterator.next();

            if (otherConsumer != null
                && otherConsumer.isNonLive()
                && LIVE_CONSUMERS_UPDATER.compareAndSet(this, liveConsumers, liveConsumers + 1))
            {
                otherConsumer.setNonLive(false);
                otherConsumer.setNotifyWorkDesired(true);
                break;
            }
            liveConsumers = LIVE_CONSUMERS_UPDATER.get(this);
            maximumLiveConsumers = _maximumLiveConsumers;
        }
    }

    @Override
    public Collection<QueueConsumer<?,?>> getConsumers()
    {
        return getConsumersImpl();
    }

    private Collection<QueueConsumer<?,?>> getConsumersImpl()
    {
        return Lists.newArrayList(_queueConsumerManager.getAllIterator());
    }


    public void resetSubPointersForGroups(QueueConsumer<?,?> consumer)
    {
        QueueEntry entry = _messageGroupManager.findEarliestAssignedAvailableEntry(consumer);
        _messageGroupManager.clearAssignments(consumer);

        if(entry != null)
        {
            resetSubPointersForGroups(entry);
        }
    }


    @Override
    public Collection<PublishingLink> getPublishingLinks()
    {
        List<PublishingLink> links = new ArrayList<>();
        for(MessageSender sender : _linkedSenders.keySet())
        {
            final Collection<? extends PublishingLink> linksForDestination = sender.getPublishingLinks(this);
            links.addAll(linksForDestination);
        }
        return links;
    }

    @Override
    public int getBindingCount()
    {
        return _bindingCount;
    }

    @Override
    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    // ------ Enqueue / Dequeue

    @Override
    public final void enqueue(ServerMessage message, Action<? super MessageInstance> action, MessageEnqueueRecord enqueueRecord)
    {
        final QueueEntry entry;
        if(_recovering.get() != RECOVERED)
        {
            _enqueuingWhileRecovering.incrementAndGet();

            boolean addedToRecoveryQueue;
            try
            {
                if(addedToRecoveryQueue = (_recovering.get() == RECOVERING))
                {
                    _postRecoveryQueue.add(new EnqueueRequest(message, action, enqueueRecord));
                }
            }
            finally
            {
                _enqueuingWhileRecovering.decrementAndGet();
            }

            if(!addedToRecoveryQueue)
            {
                while(_recovering.get() != RECOVERED)
                {
                    Thread.yield();
                }
                entry = doEnqueue(message, action, enqueueRecord);
            }
            else
            {
                entry = null;
            }
        }
        else
        {
            entry = doEnqueue(message, action, enqueueRecord);
        }

        final StoredMessage storedMessage = message.getStoredMessage();
        if ((_virtualHost.isOverTargetSize()
             || QpidByteBuffer.getAllocatedDirectMemorySize() > _flowToDiskThreshold)
            && storedMessage.getInMemorySize() > 0)
        {
            if (message.checkValid())
            {
                storedMessage.flowToDisk();
            }
            else
            {
                if (entry != null)
                {
                    malformedEntry(entry);
                }
                else
                {
                    LOGGER.debug("Malformed message '{}' enqueued into '{}'", message, getName());
                }
            }
        }
    }

    @Override
    public final void recover(ServerMessage message, final MessageEnqueueRecord enqueueRecord)
    {
        doEnqueue(message, null, enqueueRecord);
    }


    @Override
    public final void completeRecovery()
    {
        if(_recovering.compareAndSet(RECOVERING, COMPLETING_RECOVERY))
        {
            while(_enqueuingWhileRecovering.get() != 0)
            {
                Thread.yield();
            }

            // at this point we can assert that any new enqueue to the queue will not try to put into the post recovery
            // queue (because the state is no longer RECOVERING, but also no threads are currently trying to enqueue
            // because the _enqueuingWhileRecovering count is 0.

            enqueueFromPostRecoveryQueue();

            _recovering.set(RECOVERED);

        }
    }

    private void enqueueFromPostRecoveryQueue()
    {
        while(!_postRecoveryQueue.isEmpty())
        {
            EnqueueRequest request = _postRecoveryQueue.poll();
            MessageReference<?> messageReference = request.getMessage();
            doEnqueue(messageReference.getMessage(), request.getAction(), request.getEnqueueRecord());
            messageReference.release();
        }
    }

    protected QueueEntry doEnqueue(final ServerMessage message, final Action<? super MessageInstance> action, MessageEnqueueRecord enqueueRecord)
    {
        final QueueEntry entry = getEntries().add(message, enqueueRecord);
        updateExpiration(entry);

        try
        {
            if (entry.isAvailable())
            {
                checkConsumersNotAheadOfDelivery(entry);
                notifyConsumers(entry);
            }

            checkForNotificationOnNewMessage(entry.getMessage());
        }
        finally
        {
            if(action != null)
            {
                action.performAction(entry);
            }

            RejectPolicyHandler rejectPolicyHandler = _rejectPolicyHandler;
            if (rejectPolicyHandler != null)
            {
                rejectPolicyHandler.postEnqueue(entry);
            }
            _postEnqueueOverflowPolicyHandler.checkOverflow(entry);
        }
        return entry;
    }

    private void updateExpiration(final QueueEntry entry)
    {
        long expiration = calculateExpiration(entry.getMessage());
        if (expiration > 0)
        {
            entry.setExpiration(expiration);
        }
    }

    private long calculateExpiration(final ServerMessage message)
    {
        long expiration = message.getExpiration();
        long arrivalTime = message.getArrivalTime();
        if (_minimumMessageTtl != 0L)
        {
            if (expiration != 0L)
            {
                long calculatedExpiration = calculateExpiration(arrivalTime, _minimumMessageTtl);
                if (calculatedExpiration > expiration)
                {
                    expiration = calculatedExpiration;
                }
            }
        }
        if (_maximumMessageTtl != 0L)
        {
            long calculatedExpiration = calculateExpiration(arrivalTime, _maximumMessageTtl);
            if (expiration == 0L || expiration > calculatedExpiration)
            {
                expiration = calculatedExpiration;
            }
        }
        return expiration;
    }

    private long calculateExpiration(final long arrivalTime, final long ttl)
    {
        long sum;
        try
        {
            sum = Math.addExact(arrivalTime == 0 ? System.currentTimeMillis() : arrivalTime, ttl);
        }
        catch (ArithmeticException e)
        {
            sum = Long.MAX_VALUE;
        }
        return sum;
    }

    private boolean assign(final QueueConsumer<?,?> sub, final QueueEntry entry)
    {
        if(_messageGroupManager == null)
        {
            //no grouping, try to acquire immediately.
            return entry.acquire(sub);
        }
        else
        {
            //the group manager is responsible for acquiring the message if/when appropriate
            return _messageGroupManager.acceptMessage(sub, entry);
        }
    }

    private boolean mightAssign(final QueueConsumer sub, final QueueEntry entry)
    {
        return _messageGroupManager == null || !sub.acquires() || _messageGroupManager.mightAssign(entry, sub);
    }

    protected void checkConsumersNotAheadOfDelivery(final QueueEntry entry)
    {
        // This method is only required for queues which mess with ordering
        // Simple Queues don't :-)
    }

    @Override
    public long getTotalDequeuedMessages()
    {
        return _queueStatistics.getDequeueCount();
    }

    @Override
    public long getTotalEnqueuedMessages()
    {
        return _queueStatistics.getEnqueueCount();
    }

    private void setLastSeenEntry(final QueueConsumer<?,?> sub, final QueueEntry entry)
    {
        QueueContext subContext = sub.getQueueContext();
        if (subContext != null)
        {
            QueueEntry releasedEntry = subContext.getReleasedEntry();

            QueueContext._lastSeenUpdater.set(subContext, entry);
            if(releasedEntry == entry)
            {
               QueueContext._releasedUpdater.compareAndSet(subContext, releasedEntry, null);
            }
        }
    }

    private void updateSubRequeueEntry(final QueueConsumer<?,?> sub, final QueueEntry entry)
    {
        QueueContext subContext = sub.getQueueContext();
        if(subContext != null)
        {
            QueueEntry oldEntry;

            while((oldEntry  = subContext.getReleasedEntry()) == null || oldEntry.compareTo(entry) > 0)
            {
                if(QueueContext._releasedUpdater.compareAndSet(subContext, oldEntry, entry))
                {
                    notifyConsumer(sub);
                    break;
                }
            }
        }
    }


    @Override
    public void resetSubPointersForGroups(final QueueEntry entry)
    {
        resetSubPointers(entry, true);
    }

    @Override
    public void requeue(QueueEntry entry)
    {
        resetSubPointers(entry, false);
    }

    private void resetSubPointers(final QueueEntry entry, final boolean ignoreAvailable)
    {
        Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getAllIterator();
        // iterate over all the subscribers, and if they are in advance of this queue entry then move them backwards
        while (consumerIterator.hasNext() && (ignoreAvailable || entry.isAvailable()))
        {
            QueueConsumer<?,?> sub = consumerIterator.next();

            // we don't make browsers send the same stuff twice
            if (sub.seesRequeues())
            {
                updateSubRequeueEntry(sub, entry);
            }
        }
    }

    @Override
    public int getConsumerCount()
    {
        return _queueConsumerManager.getAllSize();
    }

    @Override
    public int getConsumerCountWithCredit()
    {
        return _activeSubscriberCount.get();
    }

    @Override
    public boolean isUnused()
    {
        return getConsumerCount() == 0;
    }

    @Override
    public boolean isEmpty()
    {
        return getQueueDepthMessages() == 0;
    }

    @Override
    public int getQueueDepthMessages()
    {
        return _queueStatistics.getQueueCount();
    }

    @Override
    public long getQueueDepthBytes()
    {
        return _queueStatistics.getQueueSize();
    }

    @Override
    public long getAvailableBytes()
    {
        return _queueStatistics.getAvailableSize();
    }

    @Override
    public int getAvailableMessages()
    {
        return _queueStatistics.getAvailableCount();
    }

    @Override
    public long getAvailableBytesHighWatermark()
    {
        return _queueStatistics.getAvailableSizeHwm();
    }

    @Override
    public int getAvailableMessagesHighWatermark()
    {
        return _queueStatistics.getAvailableCountHwm();
    }

    @Override
    public long getQueueDepthBytesHighWatermark()
    {
        return _queueStatistics.getQueueSizeHwm();
    }

    @Override
    public int getQueueDepthMessagesHighWatermark()
    {
        return _queueStatistics.getQueueCountHwm();
    }

    @Override
    public long getOldestMessageArrivalTime()
    {
        long oldestMessageArrivalTime = -1L;

        while(oldestMessageArrivalTime == -1L)
        {
            QueueEntryList entries = getEntries();
            QueueEntry entry = entries == null ? null : entries.getOldestEntry();
            if (entry != null)
            {
                ServerMessage message = entry.getMessage();

                if(message != null)
                {
                    try(MessageReference reference = message.newReference())
                    {
                        oldestMessageArrivalTime = reference.getMessage().getArrivalTime();
                    }
                    catch (MessageDeletedException e)
                    {
                        // ignore - the oldest message was deleted after it was discovered - we need to find the new oldest message
                    }
                }
            }
            else
            {
                oldestMessageArrivalTime = 0;
            }
        }
        return oldestMessageArrivalTime;
    }

    @Override
    public long getOldestMessageAge()
    {
        long oldestMessageArrivalTime = getOldestMessageArrivalTime();
        return oldestMessageArrivalTime == 0 ? 0 : System.currentTimeMillis() - oldestMessageArrivalTime;
    }

    @Override
    public boolean isDeleted()
    {
        return _deleted.get();
    }

    @Override
    public int getMaximumLiveConsumers()
    {
        return _maximumLiveConsumers;
    }

    boolean wouldExpire(final ServerMessage message)
    {
        long expiration = calculateExpiration(message);
        return expiration != 0 && expiration <= System.currentTimeMillis();
    }

    @Override
    public List<QueueEntry> getMessagesOnTheQueue()
    {
        ArrayList<QueueEntry> entryList = new ArrayList<>();
        QueueEntryIterator queueListIterator = getEntries().iterator();
        while (queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            if (node != null && !node.isDeleted())
            {
                entryList.add(node);
            }
        }
        return entryList;

    }

    @Override
    public QueueEntryIterator queueEntryIterator()
    {
        return getEntries().iterator();
    }

    @Override
    public int compareTo(final X o)
    {
        return getName().compareTo(o.getName());
    }

    private boolean hasExclusiveConsumer()
    {
        return _exclusiveSubscriber != null;
    }

    private void clearExclusiveSubscriber()
    {
        _exclusiveSubscriber = null;
    }

    /** Used to track bindings to exchanges so that on deletion they can easily be cancelled. */
    abstract QueueEntryList getEntries();

    final QueueStatistics getQueueStatistics()
    {
        return _queueStatistics;
    }

    protected final QueueConsumerManagerImpl getQueueConsumerManager()
    {
        return _queueConsumerManager;
    }

    public EventLogger getEventLogger()
    {
        return _virtualHost.getEventLogger();
    }

    public interface QueueEntryFilter
    {
        boolean accept(QueueEntry entry);

        boolean filterComplete();
    }


    @Override
    public QueueEntry getMessageOnTheQueue(final long messageId)
    {
        List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
        {
            private boolean _complete;

            @Override
            public boolean accept(QueueEntry entry)
            {
                _complete = entry.getMessage().getMessageNumber() == messageId;
                return _complete;
            }

            @Override
            public boolean filterComplete()
            {
                return _complete;
            }
        });
        return entries.isEmpty() ? null : entries.get(0);
    }

    List<QueueEntry> getMessagesOnTheQueue(QueueEntryFilter filter)
    {
        ArrayList<QueueEntry> entryList = new ArrayList<>();
        QueueEntryIterator queueListIterator = getEntries().iterator();
        while (queueListIterator.advance() && !filter.filterComplete())
        {
            QueueEntry node = queueListIterator.getNode();
            MessageReference reference = node.newMessageReference();
            if (reference != null)
            {
                try
                {
                    if (!node.isDeleted() && filter.accept(node))
                    {
                        entryList.add(node);
                    }
                }
                finally
                {
                    reference.release();
                }
            }

        }
        return entryList;

    }

    @Override
    public void visit(final QueueEntryVisitor visitor)
    {
        QueueEntryIterator queueListIterator = getEntries().iterator();

        while(queueListIterator.advance())
        {
            QueueEntry node = queueListIterator.getNode();
            MessageReference reference = node.newMessageReference();
            if(reference != null)
            {
                try
                {
                    if (!node.isDeleted() && reference.getMessage().checkValid() && visitor.visit(node))
                    {
                        break;
                    }
                }
                finally
                {
                    reference.release();
                }
            }
        }
    }

    // ------ Management functions

    @Override
    public long clearQueue()
    {
        QueueEntryIterator queueListIterator = getEntries().iterator();
        long count = 0;

        ServerTransaction txn = new LocalTransaction(getVirtualHost().getMessageStore());

        while (queueListIterator.advance())
        {
            final QueueEntry node = queueListIterator.getNode();
            boolean acquired = node.acquireOrSteal(new DequeueEntryTask(node, null));

            if (acquired)
            {
                dequeueEntry(node, txn);
                count++;
            }
        }

        txn.commit();

        return count;
    }

    private void dequeueEntry(final QueueEntry node)
    {
        ServerTransaction txn = new AsyncAutoCommitTransaction(getVirtualHost().getMessageStore(), (future, action) -> action.postCommit());
        dequeueEntry(node, txn);
    }

    private void dequeueEntry(final QueueEntry node, ServerTransaction txn)
    {
        txn.dequeue(node.getEnqueueRecord(),
                    new ServerTransaction.Action()
                    {

                        @Override
                        public void postCommit()
                        {
                            node.delete();
                        }

                        @Override
                        public void onRollback()
                        {

                        }
                    });
    }

    @Override
    public void deleteEntry(final QueueEntry entry)
    {
        deleteEntry(entry, null);
    }

    private final class DequeueEntryTask implements Runnable
    {
        private final QueueEntry _entry;
        private final Runnable _postDequeueTask;

        public DequeueEntryTask(final QueueEntry entry, final Runnable postDequeueTask)
        {
            _entry = entry;
            _postDequeueTask = postDequeueTask;
        }

        @Override
        public void run()
        {
            LOGGER.debug("Dequeuing stolen node {}", _entry);
            dequeueEntry(_entry);
            if (_postDequeueTask != null)
            {
                _postDequeueTask.run();
            }
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final DequeueEntryTask that = (DequeueEntryTask) o;
            return _entry == that._entry &&
                   Objects.equals(_postDequeueTask, that._postDequeueTask);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(_entry, _postDequeueTask);
        }
    }

    private void deleteEntry(final QueueEntry entry, final Runnable postDequeueTask)
    {
        boolean acquiredForDequeueing = entry.acquireOrSteal(new DequeueEntryTask(entry, postDequeueTask));

        if (acquiredForDequeueing)
        {
            LOGGER.debug("Dequeuing node {}", entry);
            dequeueEntry(entry);
            if (postDequeueTask != null)
            {
                postDequeueTask.run();
            }
        }
    }

    private void routeToAlternate(QueueEntry entry,
                                  Runnable postRouteTask,
                                  Predicate<BaseQueue> predicate)
    {
        boolean acquiredForDequeueing = entry.acquireOrSteal(() ->
                                                             {
                                                                 LOGGER.debug("routing stolen node {} to alternate", entry);
                                                                 entry.routeToAlternate(null, null, predicate);
                                                                 if (postRouteTask != null)
                                                                 {
                                                                     postRouteTask.run();
                                                                 }
                                                             });

        if (acquiredForDequeueing)
        {
            LOGGER.debug("routing node {} to alternate", entry);
            entry.routeToAlternate(null, null, predicate);
            if (postRouteTask != null)
            {
                postRouteTask.run();
            }
        }
    }


    @Override
    public void addDeleteTask(final Action<? super X> task)
    {
        _deleteTaskList.add(task);
    }

    @Override
    public void removeDeleteTask(final Action<? super X> task)
    {
        _deleteTaskList.remove(task);
    }

    @Override
    public int deleteAndReturnCount()
    {
        return doSync(deleteAndReturnCountAsync());
    }

    @Override
    public ListenableFuture<Integer> deleteAndReturnCountAsync()
    {
        return Futures.transformAsync(deleteAsync(), v -> _deleteQueueDepthFuture, getTaskExecutor());
    }

    private ListenableFuture<Integer> performDelete()
    {
        if (_deleted.compareAndSet(false, true))
        {
            if (getState() == State.UNINITIALIZED)
            {
                preSetAlternateBinding();
                _deleteQueueDepthFuture.set(0);
            }
            else
            {
                if (_transactions.isEmpty())
                {
                    doDelete();
                }
                else
                {
                    deleteAfterCompletionOfDischargingTransactions();
                }
            }
        }
        return _deleteQueueDepthFuture;
    }

    private void doDelete()
    {
        try
        {
            final int queueDepthMessages = getQueueDepthMessages();

            for(MessageSender sender : _linkedSenders.keySet())
            {
                sender.destinationRemoved(this);
            }

                Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getAllIterator();

                while (consumerIterator.hasNext())
                {
                    QueueConsumer<?,?> consumer = consumerIterator.next();

                    if (consumer != null)
                    {
                        consumer.queueDeleted();
                    }
                }

                final List<QueueEntry> entries = getMessagesOnTheQueue(new AcquireAllQueueEntryFilter());

                routeToAlternate(entries);

                preSetAlternateBinding();
                _alternateBinding = null;

                _stopped.set(true);
                _queueHouseKeepingTask.cancel();

                performQueueDeleteTasks();

                //Log Queue Deletion
                getEventLogger().message(_logSubject, QueueMessages.DELETED(getId().toString()));
                _deleteQueueDepthFuture.set(queueDepthMessages);

            _transactions.clear();
            }
            catch(Throwable e)
            {
                _deleteQueueDepthFuture.setException(e);
            }
    }

    private void deleteAfterCompletionOfDischargingTransactions()
    {
        final List<SettableFuture<Void>> dischargingTxs =
                _transactions.stream()
                             .filter(t -> !t.isDischarged() && !t.isRollbackOnly() && !t.setRollbackOnly())
                             .map(t -> {
                                 final SettableFuture<Void> future = SettableFuture.create();
                                 LocalTransaction.LocalTransactionListener listener = tx -> future.set(null);
                                 t.addTransactionListener(listener);
                                 if (t.isRollbackOnly() || t.isDischarged())
                                 {
                                     future.set(null);
                                     t.removeTransactionListener(listener);
                                 }
                                 return future;
                             })
                             .collect(Collectors.toList());

        if (dischargingTxs.isEmpty())
        {
            doDelete();
        }
        else
        {
            ListenableFuture<Void> dischargingFuture = Futures.transform(Futures.allAsList(dischargingTxs),
                                                                         input -> null,
                                                                         MoreExecutors.directExecutor());

            Futures.addCallback(dischargingFuture, new FutureCallback<Void>()
            {
                @Override
                public void onSuccess(final Void result)
                {
                    doDelete();
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    _deleteQueueDepthFuture.setException(t);
                }
            }, MoreExecutors.directExecutor());
        }
    }

    private void routeToAlternate(List<QueueEntry> entries)
    {
        ServerTransaction txn = new LocalTransaction(getVirtualHost().getMessageStore());

        for(final QueueEntry entry : entries)
        {
            // TODO log requeues with a post enqueue action
            int requeues = entry.routeToAlternate(null, txn, null);

            if(requeues == 0)
            {
                // TODO log discard
            }
        }

        txn.commit();
    }

    private void performQueueDeleteTasks()
    {
        for (Action<? super X> task : _deleteTaskList)
        {
            task.performAction((X)this);
        }

        _deleteTaskList.clear();
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        _stopped.set(true);
        _closing = false;
        _queueHouseKeepingTask.cancel();
        return Futures.immediateFuture(null);
    }

    @Override
    public void checkCapacity()
    {
        _postEnqueueOverflowPolicyHandler.checkOverflow(null);
    }

    void notifyConsumers(QueueEntry entry)
    {

        Iterator<QueueConsumer<?,?>> nonAcquiringIterator = _queueConsumerManager.getNonAcquiringIterator();
        while (nonAcquiringIterator.hasNext())
        {
            QueueConsumer<?,?> consumer = nonAcquiringIterator.next();
            if(consumer.hasInterest(entry))
            {
                notifyConsumer(consumer);
            }
        }

        final Iterator<QueueConsumer<?,?>> interestedIterator = _queueConsumerManager.getInterestedIterator();
        while (entry.isAvailable() && interestedIterator.hasNext())
        {
            QueueConsumer<?,?> consumer = interestedIterator.next();
            if(consumer.hasInterest(entry))
            {
                if(notifyConsumer(consumer))
                {
                    break;
                }
                else if(!noHigherPriorityWithCredit(consumer, entry))
                {
                    // there exists a higher priority consumer that would take this message, therefore no point in
                    // continuing to loop
                    break;
                }
            }
        }
    }

    void notifyOtherConsumers(final QueueConsumer<?,?> excludedConsumer)
    {
        final Iterator<QueueConsumer<?,?>> interestedIterator = _queueConsumerManager.getInterestedIterator();
        while (hasAvailableMessages() && interestedIterator.hasNext())
        {
            QueueConsumer<?,?> consumer = interestedIterator.next();

            if (excludedConsumer != consumer)
            {
                if (notifyConsumer(consumer))
                {
                    break;
                }
            }
        }
    }


    MessageContainer deliverSingleMessage(QueueConsumer<?,?> consumer)
    {
        boolean queueEmpty = false;
        MessageContainer messageContainer = null;
        _queueConsumerManager.setNotified(consumer, false);
        try
        {

            if (!consumer.isSuspended())
            {
                if(consumer.isNonLive())
                {
                    messageContainer = NO_MESSAGES;
                }
                else
                {
                    messageContainer = attemptDelivery(consumer);
                }

                if(messageContainer.getMessageInstance() == null)
                {
                    if (consumer.acquires())
                    {
                        if (hasAvailableMessages())
                        {
                            notifyOtherConsumers(consumer);
                        }
                    }

                    consumer.noMessagesAvailable();
                    messageContainer = null;
                }
                else
                {
                    _queueConsumerManager.setNotified(consumer, true);
                }
            }
            else
            {
                // avoid referring old deleted queue entry in sub._queueContext._lastSeen
                getNextAvailableEntry(consumer);
            }
        }
        finally
        {
            consumer.flushBatched();
        }

        return messageContainer;
    }

    private boolean hasAvailableMessages()
    {
        return _queueStatistics.getAvailableCount() != 0;
    }

    private static final MessageContainer NO_MESSAGES = new MessageContainer();

    /**
     * Attempt delivery for the given consumer.
     *
     * Looks up the next node for the consumer and attempts to deliver it.
     *
     *
     * @param sub the consumer
     * @return true if we have completed all possible deliveries for this sub.
     */
    private MessageContainer attemptDelivery(QueueConsumer<?,?> sub)
    {
        // avoid referring old deleted queue entry in sub._queueContext._lastSeen
        QueueEntry node = getNextAvailableEntry(sub);
        boolean subActive = sub.isActive() && !sub.isSuspended() && !sub.isNonLive();

        if (node != null && subActive
            && (sub.getPriority() == Integer.MAX_VALUE || noHigherPriorityWithCredit(sub, node)))
        {

            if (_virtualHost.getState() != State.ACTIVE)
            {
                throw new ConnectionScopedRuntimeException("Delivery halted owing to " +
                                                           "virtualhost state " + _virtualHost.getState());
            }

            if (node.isAvailable() && mightAssign(sub, node))
            {
                if (sub.allocateCredit(node))
                {
                    MessageReference messageReference = null;
                    if ((sub.acquires() && !assign(sub, node))
                        || (!sub.acquires() && (messageReference = node.newMessageReference()) == null))
                    {
                        // restore credit here that would have been taken away by allocateCredit since we didn't manage
                        // to acquire the entry for this consumer
                        sub.restoreCredit(node);
                    }
                    else
                    {
                        setLastSeenEntry(sub, node);
                        return new MessageContainer(node, messageReference);
                    }
                }
                else
                {
                    sub.awaitCredit(node);
                }
            }
        }

        return NO_MESSAGES;
    }

    private boolean noHigherPriorityWithCredit(final QueueConsumer<?,?> sub, final QueueEntry queueEntry)
    {
        Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getAllIterator();

        while (consumerIterator.hasNext())
        {
            QueueConsumer<?,?> consumer = consumerIterator.next();
            if(consumer.getPriority() > sub.getPriority())
            {
                if(consumer.isNotifyWorkDesired()
                   && consumer.acquires()
                   && consumer.hasInterest(queueEntry)
                   && getNextAvailableEntry(consumer) != null)
                {
                    return false;
                }
            }
            else
            {
                break;
            }
        }
        return true;
    }


    private QueueEntry getNextAvailableEntry(final QueueConsumer<?, ?> sub)
    {
        QueueContext context = sub.getQueueContext();
        if(context != null)
        {
            QueueEntry lastSeen = context.getLastSeenEntry();
            QueueEntry releasedNode = context.getReleasedEntry();

            QueueEntry node = (releasedNode != null && lastSeen.compareTo(releasedNode)>=0) ? releasedNode : getEntries()
                    .next(lastSeen);

            boolean expired = false;
            while (node != null && (!node.isAvailable() || (expired = node.expired()) || !sub.hasInterest(node) ||
                                    !mightAssign(sub,node)))
            {
                if (expired)
                {
                    expired = false;
                    expireEntry(node);
                }

                if(QueueContext._lastSeenUpdater.compareAndSet(context, lastSeen, node))
                {
                    QueueContext._releasedUpdater.compareAndSet(context, releasedNode, null);
                }

                lastSeen = context.getLastSeenEntry();
                releasedNode = context.getReleasedEntry();
                node = (releasedNode != null && lastSeen.compareTo(releasedNode)>=0)
                        ? releasedNode
                        : getEntries().next(lastSeen);
            }
            return node;
        }
        else
        {
            return null;
        }
    }

    @Override
    public boolean isEntryAheadOfConsumer(QueueEntry entry, QueueConsumer<?,?> sub)
    {
        QueueContext context = sub.getQueueContext();
        if(context != null)
        {
            QueueEntry releasedNode = context.getReleasedEntry();
            return releasedNode != null && releasedNode.compareTo(entry) < 0;
        }
        else
        {
            return false;
        }
    }


    @Override
    public void checkMessageStatus()
    {
        QueueEntryIterator queueListIterator = getEntries().iterator();

        final Set<NotificationCheck> perMessageChecks = new HashSet<>();
        final Set<NotificationCheck> queueLevelChecks = new HashSet<>();

        for(NotificationCheck check : getNotificationChecks())
        {
            if(check.isMessageSpecific())
            {
                perMessageChecks.add(check);
            }
            else
            {
                queueLevelChecks.add(check);
            }
        }
        QueueNotificationListener listener = _notificationListener;
        final long currentTime = System.currentTimeMillis();
        final long thresholdTime = currentTime - getAlertRepeatGap();

        while (!_stopped.get() && queueListIterator.advance())
        {
            final QueueEntry node = queueListIterator.getNode();
            // Only process nodes that are not currently deleted and not dequeued
            if (!node.isDeleted())
            {
                // If the node has expired then acquire it
                if (node.expired())
                {
                    expireEntry(node);
                }
                else
                {
                    node.checkHeld(currentTime);

                    // There is a chance that the node could be deleted by
                    // the time the check actually occurs. So verify we
                    // can actually get the message to perform the check.
                    ServerMessage msg = node.getMessage();
                    if (msg != null)
                    {
                        try (MessageReference messageReference = msg.newReference())
                        {
                            if (!msg.checkValid())
                            {
                                malformedEntry(node);
                            }
                            else
                            {
                                for (NotificationCheck check : perMessageChecks)
                                {
                                    checkForNotification(msg, listener, currentTime, thresholdTime, check);
                                }
                            }
                        }
                        catch(MessageDeletedException e)
                        {
                            // Ignore
                        }
                    }
                }
            }
        }

        for(NotificationCheck check : queueLevelChecks)
        {
            checkForNotification(null, listener, currentTime, thresholdTime, check);
        }
    }

    private void expireEntry(final QueueEntry node)
    {
        ExpiryPolicy expiryPolicy = getExpiryPolicy();
        long sizeWithHeader = node.getSizeWithHeader();
        switch (expiryPolicy)
        {
            case DELETE:
                deleteEntry(node, () -> _queueStatistics.addToExpired(sizeWithHeader) );
                break;
            case ROUTE_TO_ALTERNATE:
                routeToAlternate(node, () -> _queueStatistics.addToExpired(sizeWithHeader),
                                 q -> !((q instanceof AbstractQueue) && ((AbstractQueue) q).wouldExpire(node.getMessage())));
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown expiry policy: "
                                                       + expiryPolicy
                                                       + " this is a coding error inside Qpid");
        }
    }

    private void malformedEntry(final QueueEntry node)
    {
        deleteEntry(node, () -> {
            _queueStatistics.addToMalformed(node.getSizeWithHeader());
            logMalformedMessage(node);
        });
    }

    private void logMalformedMessage(final QueueEntry node)
    {
        final EventLogger eventLogger = getEventLogger();
        final ServerMessage<?> message = node.getMessage();
        final StringBuilder messageId = new StringBuilder();
        messageId.append(message.getMessageNumber());
        final String id = message.getMessageHeader().getMessageId();
        if (id != null)
        {
            messageId.append('/').append(id);
        }
        eventLogger.message(getLogSubject(), QueueMessages.MALFORMED_MESSAGE( messageId.toString(), "DELETE"));
    }

    @Override
    public boolean checkValid(final QueueEntry queueEntry)
    {
        final ServerMessage message = queueEntry.getMessage();
        boolean isValid = true;
        try (MessageReference ref = message.newReference())
        {
            isValid = message.checkValid();
        }
        catch (MessageDeletedException e)
        {
            // noop
        }
        return isValid;
    }

    @Override
    public long getTotalMalformedBytes()
    {
        return _queueStatistics.getMalformedSize();
    }

    @Override
    public long getTotalMalformedMessages()
    {
        return _queueStatistics.getMalformedCount();
    }

    @Override
    public void reallocateMessages()
    {
        QueueEntryIterator queueListIterator = getEntries().iterator();

        while (!_stopped.get() && queueListIterator.advance())
        {
            final QueueEntry node = queueListIterator.getNode();
            if (!node.isDeleted() && !node.expired())
            {
                try
                {
                    final ServerMessage message = node.getMessage();
                    final MessageReference messageReference = message.newReference();
                    try
                    {
                        if (!message.checkValid())
                        {
                            malformedEntry(node);
                        }
                        else
                        {
                            message.getStoredMessage().reallocate();
                        }
                    }
                    finally
                    {
                        messageReference.release();
                    }
                }
                catch (MessageDeletedException mde)
                {
                    // Ignore
                }
            }
        }
    }

    private boolean consumerHasAvailableMessages(final QueueConsumer consumer)
    {
        final QueueEntry queueEntry;
        return !consumer.acquires() || ((queueEntry = getNextAvailableEntry(consumer)) != null
                                        && noHigherPriorityWithCredit(consumer, queueEntry));
    }

    void setNotifyWorkDesired(final QueueConsumer consumer, final boolean desired)
    {
        if (_queueConsumerManager.setInterest(consumer, desired))
        {
            if (desired)
            {
                _activeSubscriberCount.incrementAndGet();
                notifyConsumer(consumer);
            }
            else
            {
                _activeSubscriberCount.decrementAndGet();

                // iterate over interested and notify one as long as its priority is higher than any notified
                final Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getInterestedIterator();
                final int highestNotifiedPriority = _queueConsumerManager.getHighestNotifiedPriority();
                while (consumerIterator.hasNext())
                {
                    QueueConsumer<?,?> queueConsumer = consumerIterator.next();
                    if (queueConsumer.getPriority() < highestNotifiedPriority || notifyConsumer(queueConsumer))
                    {
                        break;
                    }
                }
            }
        }
    }

    private boolean notifyConsumer(final QueueConsumer<?,?> consumer)
    {
        if(consumerHasAvailableMessages(consumer) && _queueConsumerManager.setNotified(consumer, true))
        {
            consumer.notifyWork();
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public long getAlertRepeatGap()
    {
        return _alertRepeatGap;
    }

    @Override
    public long getAlertThresholdMessageAge()
    {
        return _alertThresholdMessageAge;
    }

    @Override
    public long getAlertThresholdQueueDepthMessages()
    {
        return _alertThresholdQueueDepthMessages;
    }

    private void updateAlertChecks()
    {
        updateNotificationCheck(getAlertThresholdQueueDepthMessages(), NotificationCheck.MESSAGE_COUNT_ALERT);
        updateNotificationCheck(getAlertThresholdQueueDepthBytes(), NotificationCheck.QUEUE_DEPTH_ALERT);
        updateNotificationCheck(getAlertThresholdMessageAge(), NotificationCheck.MESSAGE_AGE_ALERT);
        updateNotificationCheck(getAlertThresholdMessageSize(), NotificationCheck.MESSAGE_SIZE_ALERT);
    }

    private void updateNotificationCheck(final long checkValue, final NotificationCheck notificationCheck)
    {
        if (checkValue == 0L)
        {
            _notificationChecks.remove(notificationCheck);
        }
        else
        {
            _notificationChecks.add(notificationCheck);
        }
    }

    @Override
    public long getAlertThresholdQueueDepthBytes()
    {
        return _alertThresholdQueueDepthBytes;
    }

    @Override
    public long getAlertThresholdMessageSize()
    {
        return _alertThresholdMessageSize;
    }

    @Override
    public Set<NotificationCheck> getNotificationChecks()
    {
        return _notificationChecks;
    }

    abstract class BaseMessageContent implements Content, CustomRestHeaders
    {
        public static final int UNLIMITED = -1;
        protected final MessageReference<?> _messageReference;
        protected final long _limit;
        private final boolean _truncated;

        BaseMessageContent(MessageReference<?> messageReference, long limit)
        {
            _messageReference = messageReference;
            _limit = limit;
            _truncated = limit >= 0 && _messageReference.getMessage().getSize() > limit;
        }

        @Override
        public final void release()
        {
            _messageReference.release();
        }

        protected boolean isTruncated()
        {
            return _truncated;
        }

        @SuppressWarnings("unused")
        @RestContentHeader("X-Content-Truncated")
        public String getContentTruncated()
        {
            return String.valueOf(isTruncated());
        }

        @SuppressWarnings("unused")
        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return _messageReference.getMessage().getMessageHeader().getMimeType();
        }

        @SuppressWarnings("unused")
        @RestContentHeader("Content-Encoding")
        public String getContentEncoding()
        {
            return _messageReference.getMessage().getMessageHeader().getEncoding();
        }

        @SuppressWarnings("unused")
        @RestContentHeader("Content-Disposition")
        public String getContentDisposition()
        {
            try
            {
                String queueName = getName();
                // replace all non-ascii and non-printable characters and all backslashes and percent encoded characters
                // as suggested by rfc6266 Appendix D
                String asciiQueueName = queueName.replaceAll("[^\\x20-\\x7E]", "?")
                                                 .replace('\\', '?')
                                                 .replaceAll("%[0-9a-fA-F]{2}", "?");
                long messageNumber = _messageReference.getMessage().getMessageNumber();
                String filenameExtension = _mimeTypeToFileExtension.get(getContentType());
                filenameExtension = (filenameExtension == null ? "" : filenameExtension);
                String disposition = String.format("attachment; filename=\"%s_msg%09d%s\"; filename*=\"UTF-8''%s_msg%09d%s\"",
                                                   asciiQueueName,
                                                   messageNumber,
                                                   filenameExtension,
                                                   URLEncoder.encode(queueName, UTF8),
                                                   messageNumber,
                                                   filenameExtension);
                return disposition;
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("JVM does not support UTF8", e);
            }
        }
    }

    class JsonMessageContent extends BaseMessageContent
    {
        private final InternalMessage _internalMessage;

        JsonMessageContent(MessageReference<?> messageReference, InternalMessage message, long limit)
        {
            super(messageReference, limit);
            _internalMessage = message;
        }

        @Override
        public void write(OutputStream outputStream) throws IOException
        {
            Object messageBody = _internalMessage.getMessageBody();
            new MessageContentJsonConverter(messageBody, isTruncated() ? _limit : UNLIMITED).convertAndWrite(outputStream);
        }

        @SuppressWarnings("unused")
        @Override
        @RestContentHeader("Content-Encoding")
        public String getContentEncoding()
        {
            return "identity";
        }

        @SuppressWarnings("unused")
        @Override
        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "application/json";
        }
    }

    class MessageContent extends BaseMessageContent
    {

        private boolean _decompressBeforeLimiting;

        MessageContent(MessageReference<?> messageReference, long limit, boolean decompressBeforeLimiting)
        {
            super(messageReference, limit);
            if (decompressBeforeLimiting)
            {
                String contentEncoding = getContentEncoding();
                if (GZIP_CONTENT_ENCODING.equals(contentEncoding))
                {
                    _decompressBeforeLimiting = true;
                }
                else if (contentEncoding != null && !"".equals(contentEncoding) && !"identity".equals(contentEncoding))
                {
                    throw new IllegalArgumentException(String.format(
                            "Requested decompression of message with unknown compression '%s'", contentEncoding));
                }
            }
        }

        @Override
        public void write(OutputStream outputStream) throws IOException
        {
            ServerMessage message = _messageReference.getMessage();

            int length = (int) ((_limit == UNLIMITED || _decompressBeforeLimiting) ? message.getSize() : _limit);
            try (QpidByteBuffer content = message.getContent(0, length))
            {
                InputStream inputStream = content.asInputStream();
                if (_limit != UNLIMITED && _decompressBeforeLimiting)
                {
                    inputStream = new GZIPInputStream(inputStream);
                    inputStream = ByteStreams.limit(inputStream, _limit);
                    outputStream = new GZIPOutputStream(outputStream, true);
                }

                try
                {
                    ByteStreams.copy(inputStream, outputStream);
                }
                finally
                {
                    inputStream.close();
                    // Seems weird to close the outputStream here but otherwise the GZIPOutputStream will be in an
                    // invalid state. Calling flush() did not solve the problem.
                    outputStream.close();
                }
            }

        }
    }

    private static class AcquireAllQueueEntryFilter implements QueueEntryFilter
    {
        @Override
        public boolean accept(QueueEntry entry)
        {
            return entry.acquire();
        }

        @Override
        public boolean filterComplete()
        {
            return false;
        }
    }

    @Override
    public long getTotalEnqueuedBytes()
    {
        return _queueStatistics.getEnqueueSize();
    }

    @Override
    public long getTotalDequeuedBytes()
    {
        return _queueStatistics.getDequeueSize();
    }

    @Override
    public long getPersistentEnqueuedBytes()
    {
        return _queueStatistics.getPersistentEnqueueSize();
    }

    @Override
    public long getPersistentDequeuedBytes()
    {
        return _queueStatistics.getPersistentDequeueSize();
    }

    @Override
    public long getPersistentEnqueuedMessages()
    {
        return _queueStatistics.getPersistentEnqueueCount();
    }

    @Override
    public long getPersistentDequeuedMessages()
    {
        return _queueStatistics.getPersistentDequeueCount();
    }

    @Override
    public boolean isHeld(final QueueEntry queueEntry, final long evaluationTime)
    {
        if(!_holdMethods.isEmpty())
        {
            ServerMessage message = queueEntry.getMessage();
            try
            {
                MessageReference ref = message.newReference();
                try
                {
                    for(HoldMethod method : _holdMethods)
                    {
                        if(method.isHeld(ref, evaluationTime))
                        {
                            return true;
                        }
                    }
                    return false;
                }
                finally
                {
                    ref.release();
                }
            }
            catch (MessageDeletedException e)
            {
                return false;
            }
        }
        else
        {
            return false;
        }

    }

    @Override
    public String toString()
    {
        return getName();
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return _queueStatistics.getUnackedCount();
    }

    @Override
    public long getUnacknowledgedBytes()
    {
        return _queueStatistics.getUnackedSize();
    }

    @Override
    public int getMaximumDeliveryAttempts()
    {
        return _maximumDeliveryAttempts;
    }

    @Override
    public long getTotalExpiredBytes()
    {
        return _queueStatistics.getExpiredSize();
    }

    @Override
    public long getTotalExpiredMessages()
    {
        return _queueStatistics.getExpiredCount();
    }

    private void checkForNotification(final ServerMessage<?> msg,
                                      final QueueNotificationListener listener,
                                      final long currentTime,
                                      final long thresholdTime,
                                      final NotificationCheck check)
    {
        if (check.isMessageSpecific() || (_lastNotificationTimes[check.ordinal()] < thresholdTime))
        {
            if (check.notifyIfNecessary(msg, this, listener))
            {
                _lastNotificationTimes[check.ordinal()] = currentTime;
            }
        }
    }

    private void checkForNotificationOnNewMessage(final ServerMessage<?> msg)
    {
        final Set<NotificationCheck> notificationChecks = getNotificationChecks();
        QueueNotificationListener listener = _notificationListener;
        if (!notificationChecks.isEmpty())
        {
            final long currentTime = System.currentTimeMillis();
            final long thresholdTime = currentTime - getAlertRepeatGap();

            for (NotificationCheck check : notificationChecks)
            {
                if (check.isCheckOnMessageArrival())
                {
                    checkForNotification(msg, listener, currentTime, thresholdTime, check);
                }
            }
        }
    }

    @Override
    public void setNotificationListener(QueueNotificationListener  listener)
    {
        _notificationListener = listener == null ? NULL_NOTIFICATION_LISTENER : listener;
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                               final String routingAddress,
                                                                                               final InstanceProperties instanceProperties)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }
        RoutingResult<M> result = new RoutingResult<>(message);
        if (!message.isResourceAcceptable(this))
        {
            result.addRejectReason(this,
                                   RejectType.PRECONDITION_FAILED,
                                   String.format("Not accepted by queue '%s'", getName()));
        }
        else if (message.isReferenced(this))
        {
            result.addRejectReason(this,
                                   RejectType.ALREADY_ENQUEUED,
                                   String.format("Already enqueued on queue '%s'", getName()));
        }
        else
        {
            try
            {
                RejectPolicyHandler rejectPolicyHandler = _rejectPolicyHandler;
                if (rejectPolicyHandler != null)
                {
                    rejectPolicyHandler.checkReject(message);
                }
                result.addQueue(this);
            }
            catch (MessageUnacceptableException e)
            {
                result.addRejectReason(this, RejectType.LIMIT_EXCEEDED, e.getMessage());
            }
        }
        return result;
    }

    @Override
    public boolean verifySessionAccess(final AMQPSession<?,?> session)
    {
        boolean allowed;
        switch(_exclusive)
        {
            case NONE:
                allowed = true;
                break;
            case SESSION:
                allowed = _exclusiveOwner == null || _exclusiveOwner == session;
                break;
            case CONNECTION:
                allowed = _exclusiveOwner == null || _exclusiveOwner == session.getAMQPConnection();
                break;
            case PRINCIPAL:
                allowed = _exclusiveOwner == null || Objects.equals(((Principal) _exclusiveOwner).getName(),
                                                                    session.getAMQPConnection().getAuthorizedPrincipal().getName());
                break;
            case CONTAINER:
                allowed = _exclusiveOwner == null || _exclusiveOwner.equals(session.getAMQPConnection().getRemoteContainerName());
                break;
            case LINK:
                allowed = _exclusiveSubscriber == null || _exclusiveSubscriber.getSession() == session;
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown exclusivity policy " + _exclusive);
        }
        return allowed;
    }

    private void updateExclusivityPolicy(ExclusivityPolicy desiredPolicy)
            throws ExistingConsumerPreventsExclusive
    {
        if(desiredPolicy == null)
        {
            desiredPolicy = ExclusivityPolicy.NONE;
        }

        if(desiredPolicy != _exclusive)
        {
            switch(desiredPolicy)
            {
                case NONE:
                    _exclusiveOwner = null;
                    break;
                case PRINCIPAL:
                    switchToPrincipalExclusivity();
                    break;
                case CONTAINER:
                    switchToContainerExclusivity();
                    break;
                case CONNECTION:
                    switchToConnectionExclusivity();
                    break;
                case SESSION:
                    switchToSessionExclusivity();
                    break;
                case LINK:
                    switchToLinkExclusivity();
                    break;
            }
            _exclusive = desiredPolicy;
        }
    }

    private void switchToLinkExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch (getConsumerCount())
        {
            case 1:
                Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getAllIterator();

                if (consumerIterator.hasNext())
                {
                    _exclusiveSubscriber = consumerIterator.next();
                }
                // deliberate fall through
            case 0:
                _exclusiveOwner = null;
                break;
            default:
                throw new ExistingConsumerPreventsExclusive();
        }

    }

    private void switchToSessionExclusivity() throws ExistingConsumerPreventsExclusive
    {

        switch(_exclusive)
        {
            case NONE:
            case PRINCIPAL:
            case CONTAINER:
            case CONNECTION:
                AMQPSession<?,?> session = null;
                Iterator<QueueConsumer<?,?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?,?> c = queueConsumerIterator.next();

                    if(session == null)
                    {
                        session = c.getSession();
                    }
                    else if(!session.equals(c.getSession()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = session;
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSession().getAMQPConnection();
        }
    }

    private void switchToConnectionExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch(_exclusive)
        {
            case NONE:
            case CONTAINER:
            case PRINCIPAL:
                AMQPConnection con = null;
                Iterator<QueueConsumer<?,?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?,?> c = queueConsumerIterator.next();
                    if(con == null)
                    {
                        con = c.getSession().getAMQPConnection();
                    }
                    else if(!con.equals(c.getSession().getAMQPConnection()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = con;
                break;
            case SESSION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQPSession<?,?>)_exclusiveOwner).getAMQPConnection();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSession().getAMQPConnection();
        }
    }

    private void switchToContainerExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch(_exclusive)
        {
            case NONE:
            case PRINCIPAL:
                String containerID = null;
                Iterator<QueueConsumer<?,?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?,?> c = queueConsumerIterator.next();
                    if(containerID == null)
                    {
                        containerID = c.getSession().getAMQPConnection().getRemoteContainerName();
                    }
                    else if(!containerID.equals(c.getSession().getAMQPConnection().getRemoteContainerName()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = containerID;
                break;
            case CONNECTION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQPConnection)_exclusiveOwner).getRemoteContainerName();
                break;
            case SESSION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQPSession<?,?>)_exclusiveOwner).getAMQPConnection().getRemoteContainerName();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSession().getAMQPConnection().getRemoteContainerName();
        }
    }

    private void switchToPrincipalExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch(_exclusive)
        {
            case NONE:
            case CONTAINER:
                Principal principal = null;
                Iterator<QueueConsumer<?,?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?,?> c = queueConsumerIterator.next();
                    if(principal == null)
                    {
                        principal = c.getSession().getAMQPConnection().getAuthorizedPrincipal();
                    }
                    else if(!Objects.equals(principal.getName(),
                                            c.getSession().getAMQPConnection().getAuthorizedPrincipal().getName()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = principal;
                break;
            case CONNECTION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQPConnection)_exclusiveOwner).getAuthorizedPrincipal();
                break;
            case SESSION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQPSession<?,?>)_exclusiveOwner).getAMQPConnection().getAuthorizedPrincipal();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSession().getAMQPConnection().getAuthorizedPrincipal();
        }
    }

    private class ClearOwnerAction implements Action<Deletable>
    {
        private final Deletable<? extends Deletable> _lifetimeObject;
        private DeleteDeleteTask _deleteTask;

        public ClearOwnerAction(final Deletable<? extends Deletable> lifetimeObject)
        {
            _lifetimeObject = lifetimeObject;
        }

        @Override
        public void performAction(final Deletable object)
        {
            if(AbstractQueue.this._exclusiveOwner == _lifetimeObject)
            {
                AbstractQueue.this._exclusiveOwner = null;
            }
            if(_deleteTask != null)
            {
                removeDeleteTask(_deleteTask);
            }
        }

        public void setDeleteTask(final DeleteDeleteTask deleteTask)
        {
            _deleteTask = deleteTask;
        }
    }

    //=============

    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> activate()
    {
        _virtualHost.scheduleHouseKeepingTask(_virtualHost.getHousekeepingCheckPeriod(), _queueHouseKeepingTask);
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        return Futures.transform(performDelete(), i -> null, getTaskExecutor());
    }

    @Override
    public ExclusivityPolicy getExclusive()
    {
        return _exclusive;
    }

    @Override
    public OverflowPolicy getOverflowPolicy()
    {
        return _overflowPolicy;
    }

    @Override
    public boolean isNoLocal()
    {
        return _noLocal;
    }

    @Override
    public String getMessageGroupKeyOverride()
    {
        return _messageGroupKeyOverride;
    }

    @Override
    public MessageGroupType getMessageGroupType()
    {
        return _messageGroupType;
    }

    @Override
    public String getMessageGroupDefaultGroup()
    {
        return _messageGroupDefaultGroup;
    }

    @Override
    public int getMaximumDistinctGroups()
    {
        return _maximumDistinctGroups;
    }

    @Override
    public boolean isQueueFlowStopped()
    {
        if (_postEnqueueOverflowPolicyHandler instanceof ProducerFlowControlOverflowPolicyHandler)
        {
            return ((ProducerFlowControlOverflowPolicyHandler) _postEnqueueOverflowPolicyHandler).isQueueFlowStopped();
        }
        return false;
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
    {
        if(clazz == org.apache.qpid.server.model.Consumer.class)
        {
            return _queueConsumerManager == null
                    ? Collections.<C>emptySet()
                    : (Collection<C>) Lists.newArrayList(_queueConsumerManager.getAllIterator());
        }
        else return Collections.emptySet();
    }

    @Override
    protected void changeAttributes(final Map<String, Object> attributes)
    {
        final OverflowPolicy existingOverflowPolicy = getOverflowPolicy();
        final ExclusivityPolicy existingExclusivePolicy = getExclusive();

        super.changeAttributes(attributes);

        // Overflow policies depend on queue depth attributes.
        // Thus, we need to create and invoke  overflow policy handler
        // after all required attributes are changed.
        if (attributes.containsKey(OVERFLOW_POLICY) && existingOverflowPolicy != _overflowPolicy)
        {
            if (existingOverflowPolicy == OverflowPolicy.REJECT)
            {
                _rejectPolicyHandler = null;
            }
            createOverflowPolicyHandlers(_overflowPolicy);

            _postEnqueueOverflowPolicyHandler.checkOverflow(null);
        }

        if (attributes.containsKey(EXCLUSIVE) && existingExclusivePolicy != _exclusive)
        {
            ExclusivityPolicy newPolicy = _exclusive;
            try
            {
                _exclusive = existingExclusivePolicy;
                updateExclusivityPolicy(newPolicy);
            }
            catch (ExistingConsumerPreventsExclusive existingConsumerPreventsExclusive)
            {
                throw new IllegalArgumentException("Unable to set exclusivity policy to " + newPolicy + " as an existing combinations of consumers prevents this");
            }
        }
    }

    private static final String[] NON_NEGATIVE_NUMBERS = {
        ALERT_REPEAT_GAP,
        ALERT_THRESHOLD_MESSAGE_AGE,
        ALERT_THRESHOLD_MESSAGE_SIZE,
        ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
        ALERT_THRESHOLD_QUEUE_DEPTH_BYTES,
        MAXIMUM_DELIVERY_ATTEMPTS
    };

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        Queue<?> queue = (Queue) proxyForValidation;

        for (String attrName : NON_NEGATIVE_NUMBERS)
        {
            if (changedAttributes.contains(attrName))
            {
                Object value = queue.getAttribute(attrName);
                if (!(value instanceof Number) || ((Number) value).longValue() < 0)
                {
                    throw new IllegalConfigurationException(
                            "Only positive integer value can be specified for the attribute "
                            + attrName);
                }
            }
        }

        if (changedAttributes.contains(ALTERNATE_BINDING))
        {
            validateOrCreateAlternateBinding(queue, false);
        }

        if (changedAttributes.contains(ConfiguredObject.DESIRED_STATE) && proxyForValidation.getDesiredState() == State.DELETED)
        {
            if(hasReferrers())
            {
                throw new MessageDestinationIsAlternateException(getName());
            }
        }
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _virtualHost;
    }


    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {
        authorise(token, PUBLISH_ACTION, arguments);
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(QueueMessages.OPERATION(operation));
    }

    private class DeletedChildListener extends AbstractConfigurationChangeListener
    {
        @Override
        public void stateChanged(final ConfiguredObject object, final State oldState, final State newState)
        {
            if(newState == State.DELETED)
            {
                AbstractQueue.this.childRemoved(object);
            }
        }
    }

    private static class EnqueueRequest
    {
        private final MessageReference<?> _message;
        private final Action<? super MessageInstance> _action;
        private final MessageEnqueueRecord _enqueueRecord;

        public EnqueueRequest(final ServerMessage message,
                              final Action<? super MessageInstance> action,
                              final MessageEnqueueRecord enqueueRecord)
        {
            _enqueueRecord = enqueueRecord;
            _message = message.newReference();
            _action = action;
        }

        public MessageReference<?> getMessage()
        {
            return _message;
        }

        public Action<? super MessageInstance> getAction()
        {
            return _action;
        }

        public MessageEnqueueRecord getEnqueueRecord()
        {
            return _enqueueRecord;
        }
    }

    @Override
    public List<Long> moveMessages(Queue<?> destination, List<Long> messageIds, final String selector, final int limit)
    {
        MoveMessagesTransaction transaction = new MoveMessagesTransaction(this,
                                                                          messageIds,
                                                                          destination,
                                                                          parseSelector(selector),
                                                                          limit);
        _virtualHost.executeTransaction(transaction);
        return transaction.getModifiedMessageIds();

    }

    @Override
    public List<Long> copyMessages(Queue<?> destination, List<Long> messageIds, final String selector, int limit)
    {
        CopyMessagesTransaction transaction = new CopyMessagesTransaction(this,
                                                                          messageIds,
                                                                          destination,
                                                                          parseSelector(selector),
                                                                          limit);
        _virtualHost.executeTransaction(transaction);
        return transaction.getModifiedMessageIds();

    }

    @Override
    public List<Long> deleteMessages(final List<Long> messageIds, final String selector, int limit)
    {
        DeleteMessagesTransaction transaction = new DeleteMessagesTransaction(this,
                                                                              messageIds,
                                                                              parseSelector(selector),
                                                                              limit);
        _virtualHost.executeTransaction(transaction);

        return transaction.getModifiedMessageIds();
    }

    private JMSSelectorFilter parseSelector(final String selector)
    {
        try
        {
            return selector == null ? null : new JMSSelectorFilter(selector);
        }
        catch (ParseException | SelectorParsingException | TokenMgrError e)
        {
            throw new IllegalArgumentException("Cannot parse JMS selector \"" + selector + "\"", e);
        }
    }

    @Override
    public Content getMessageContent(final long messageId, final long limit, boolean returnJson, boolean decompressBeforeLimiting)
    {
        final MessageContentFinder messageFinder = new MessageContentFinder(messageId);
        visit(messageFinder);
        if (messageFinder.isFound())
        {
            return createMessageContent(messageFinder.getMessageReference(), returnJson, limit, decompressBeforeLimiting);
        }
        else
        {
            return null;
        }
    }

    private Content createMessageContent(final MessageReference<?> messageReference,
                                         final boolean returnJson,
                                         final long limit,
                                         final boolean decompressBeforeLimiting)
    {
        if (returnJson)
        {
            ServerMessage message = messageReference.getMessage();
            if (message instanceof InternalMessage)
            {
                return new JsonMessageContent(messageReference, (InternalMessage) message, limit);
            }
            else
            {
                MessageConverter messageConverter =
                        MessageConverterRegistry.getConverter(message.getClass(), InternalMessage.class);
                if (messageConverter != null && message.checkValid())
                {
                    InternalMessage convertedMessage = null;
                    try
                    {
                        convertedMessage = (InternalMessage) messageConverter.convert(message, getVirtualHost());
                        return new JsonMessageContent(messageReference, convertedMessage, limit);
                    }
                    finally
                    {
                        if (convertedMessage != null)
                        {
                            messageConverter.dispose(convertedMessage);
                        }
                    }
                }
                else
                {
                    throw new IllegalArgumentException(String.format("Unable to convert message %d on queue '%s' to JSON",
                                                                     message.getMessageNumber(), getName()));
                }

            }
        }
        else
        {
            return new MessageContent(messageReference, limit, decompressBeforeLimiting);
        }
    }

    @Override
    public List<MessageInfo> getMessageInfo(int first, int last, boolean includeHeaders)
    {
        final MessageCollector messageCollector = new MessageCollector(first, last, includeHeaders);
        visit(messageCollector);
        return messageCollector.getMessages();

    }

    @Override
    public MessageInfo getMessageInfoById(final long messageId, boolean includeHeaders)
    {
        final MessageFinder messageFinder = new MessageFinder(messageId, includeHeaders);
        visit(messageFinder);
        return messageFinder.getMessageInfo();
    }

    @Override
    public QueueEntry getLeastSignificantOldestEntry()
    {
        return getEntries().getLeastSignificantOldestEntry();
    }

    @Override
    public void removeReference(DestinationReferrer destinationReferrer)
    {
        _referrers.remove(destinationReferrer);
    }

    @Override
    public void addReference(DestinationReferrer destinationReferrer)
    {
        _referrers.add(destinationReferrer);
    }

    private boolean hasReferrers()
    {
        return !_referrers.isEmpty();
    }

    private class MessageFinder implements QueueEntryVisitor
    {
        private final long _messageNumber;
        private final boolean _includeHeaders;
        private MessageInfo _messageInfo;

        private MessageFinder(long messageNumber, final boolean includeHeaders)
        {
            _messageNumber = messageNumber;
            _includeHeaders = includeHeaders;
        }

        @Override
        public boolean visit(QueueEntry entry)
        {
            ServerMessage message = entry.getMessage();
            if(message != null)
            {
                if (_messageNumber == message.getMessageNumber())
                {
                    _messageInfo = new MessageInfoImpl(entry, _includeHeaders);
                    return true;
                }
            }
            return false;
        }

        public MessageInfo getMessageInfo()
        {
            return _messageInfo;
        }
    }

    private class MessageContentFinder implements QueueEntryVisitor
    {
        private final long _messageNumber;
        private boolean _found;
        private MessageReference<?> _messageReference;

        private MessageContentFinder(long messageNumber)
        {
            _messageNumber = messageNumber;
        }


        @Override
        public boolean visit(QueueEntry entry)
        {
            ServerMessage message = entry.getMessage();
            if(message != null)
            {
                if(_messageNumber == message.getMessageNumber())
                {
                    try
                    {
                        _messageReference = message.newReference();
                        _found = true;
                        return true;
                    }
                    catch (MessageDeletedException e)
                    {
                        // ignore - the message was deleted as we tried too look at it, treat as if no message found
                    }
                }

            }
            return false;
        }

        MessageReference<?> getMessageReference()
        {
            return _messageReference;
        }

        public boolean isFound()
        {
            return _found;
        }
    }

    private class MessageCollector implements QueueEntryVisitor
    {



        private class MessageRangeList extends ArrayList<MessageInfo> implements CustomRestHeaders
        {
            @RestContentHeader("Content-Range")
            public String getContentRange()
            {
                String min = isEmpty() ? "0" : String.valueOf(_first);
                String max = isEmpty() ? "0" : String.valueOf(_first + size() - 1);
                return "" + min + "-" + max + "/" + getQueueDepthMessages();
            }
        }

        private final int _first;
        private final int _last;
        private int _position = -1;
        private final List<MessageInfo> _messages = new MessageRangeList();
        private final boolean _includeHeaders;

        private MessageCollector(int first, int last, boolean includeHeaders)
        {
            _first = first;
            _last = last;
            _includeHeaders = includeHeaders;
        }


        @Override
        public boolean visit(QueueEntry entry)
        {

            _position++;
            if((_first == -1 || _position >= _first) && (_last == -1 || _position <= _last))
            {
                _messages.add(new MessageInfoImpl(entry, _includeHeaders));
            }
            return _last != -1 && _position > _last;
        }

        public List<MessageInfo> getMessages()
        {
            return _messages;
        }
    }

    private class AdvanceConsumersTask extends HouseKeepingTask
    {

        AdvanceConsumersTask()
        {
            super("Queue Housekeeping: " + AbstractQueue.this.getName(),
                  _virtualHost, getSystemTaskControllerContext("Queue Housekeeping", _virtualHost.getPrincipal()));
        }

        @Override
        public void execute()
        {
            // if there's (potentially) more than one consumer the others will potentially not have been advanced to the
            // next entry they are interested in yet.  This would lead to holding on to references to expired messages, etc
            // which would give us memory "leak".

            Iterator<QueueConsumer<?,?>> consumerIterator = _queueConsumerManager.getAllIterator();

            while (consumerIterator.hasNext() && !isDeleted())
            {
                QueueConsumer<?,?> sub = consumerIterator.next();
                if(sub.acquires())
                {
                    getNextAvailableEntry(sub);
                }
            }
        }
    }

    @Override
    public void linkAdded(final MessageSender sender, final PublishingLink link)
    {

        Integer oldValue = _linkedSenders.putIfAbsent(sender, 1);
        if(oldValue != null)
        {
            _linkedSenders.put(sender, oldValue+1);
        }
        if(Binding.TYPE.equals(link.getType()))
        {
            _bindingCount++;
        }
    }

    @Override
    public void linkRemoved(final MessageSender sender, final PublishingLink link)
    {
        int oldValue = _linkedSenders.remove(sender);
        if(oldValue != 1)
        {
            _linkedSenders.put(sender, oldValue-1);
        }
        if(Binding.TYPE.equals(link.getType()))
        {
            _bindingCount--;
        }
    }

    @Override
    public MessageConversionExceptionHandlingPolicy getMessageConversionExceptionHandlingPolicy()
    {
        return _messageConversionExceptionHandlingPolicy;
    }

    private void validateOrCreateAlternateBinding(final Queue<?> queue, final boolean mayCreate)
    {
        Object value = queue.getAttribute(ALTERNATE_BINDING);
        if (value instanceof AlternateBinding)
        {
            AlternateBinding alternateBinding = (AlternateBinding) value;
            String destinationName = alternateBinding.getDestination();
            MessageDestination messageDestination =
                    _virtualHost.getAttainedMessageDestination(destinationName, mayCreate);
            if (messageDestination == null)
            {
                throw new UnknownAlternateBindingException(destinationName);
            }
            else if (messageDestination == this)
            {
                throw new IllegalConfigurationException(String.format(
                        "Cannot create alternate binding for '%s' : Alternate binding destination cannot refer to self.",
                        getName()));
            }
            else if (isDurable() && !messageDestination.isDurable())
            {
                throw new IllegalConfigurationException(String.format(
                        "Cannot create alternate binding for '%s' : Alternate binding destination '%s' is not durable.",
                        getName(),
                        destinationName));
            }
        }
    }

    @Override
    public void registerTransaction(final ServerTransaction tx)
    {
        if (tx instanceof LocalTransaction)
        {
            LocalTransaction localTransaction = (LocalTransaction) tx;
            if (!isDeleted())
            {
                if (_transactions.add(localTransaction))
                {
                    localTransaction.addTransactionListener(_localTransactionListener);
                    if (isDeleted())
                    {
                        localTransaction.setRollbackOnly();
                        unregisterTransaction(localTransaction);
                    }
                }
            }
            else
            {
                localTransaction.setRollbackOnly();
            }
        }
    }

    @Override
    public void unregisterTransaction(final ServerTransaction tx)
    {
        if (tx instanceof LocalTransaction)
        {
            LocalTransaction localTransaction = (LocalTransaction) tx;
            localTransaction.removeTransactionListener(_localTransactionListener);
            _transactions.remove(localTransaction);
        }
    }

    @SuppressWarnings("unused")
    private void queueMessageTtlChanged()
    {
        if (getState() == State.ACTIVE)
        {
            String taskName = String.format("Queue Housekeeping : %s : TTL Update", getName());
            getVirtualHost().executeTask(taskName,
                                         this::updateQueueEntryExpiration,
                                         getSystemTaskControllerContext(taskName, _virtualHost.getPrincipal()));
        }
    }

    private void updateQueueEntryExpiration()
    {
        final QueueEntryList entries = getEntries();
        if (entries != null)
        {
            final QueueEntryIterator queueListIterator = entries.iterator();
            while (!_stopped.get() && queueListIterator.advance())
            {
                final QueueEntry node = queueListIterator.getNode();
                if (!node.isDeleted())
                {
                    ServerMessage msg = node.getMessage();
                    if (msg != null)
                    {
                        try (MessageReference messageReference = msg.newReference())
                        {
                            updateExpiration(node);
                        }
                        catch (MessageDeletedException e)
                        {
                            // Ignore
                        }
                    }
                    if (node.expired())
                    {
                        expireEntry(node);
                    }
                }
            }
        }
    }

}
