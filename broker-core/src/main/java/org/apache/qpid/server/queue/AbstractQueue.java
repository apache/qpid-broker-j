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

import static org.apache.qpid.server.util.ParameterizedTypes.MAP_OF_STRING_STRING;
import static org.apache.qpid.util.GZIPUtils.GZIP_CONTENT_ENCODING;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.security.auth.Subject;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBufferInputStream;
import org.apache.qpid.filter.SelectorParsingException;
import org.apache.qpid.filter.selector.ParseException;
import org.apache.qpid.filter.selector.TokenMgrError;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.logging.subjects.QueueLogSubject;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageInfo;
import org.apache.qpid.server.message.MessageInfoImpl;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.plugin.MessageFilterFactory;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.server.util.MapValueConverter;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.HouseKeepingTask;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public abstract class AbstractQueue<X extends AbstractQueue<X>>
        extends AbstractConfiguredObject<X>
        implements Queue<X>,
                   MessageGroupManager.ConsumerResetHelper
{

    private static final Logger _logger = LoggerFactory.getLogger(AbstractQueue.class);

    public static final String SHARED_MSG_GROUP_ARG_VALUE = "1";

    private static final QueueNotificationListener NULL_NOTIFICATION_LISTENER = new QueueNotificationListener()
    {
        @Override
        public void notifyClients(final NotificationCheck notification,
                                  final Queue queue,
                                  final String notificationMsg)
        {

        }
    };

    private static final long INITIAL_TARGET_QUEUE_SIZE = 102400l;
    private static final String UTF8 = StandardCharsets.UTF_8.name();
    private static final Operation PUBLISH_ACTION = Operation.ACTION("publish");

    private final QueueManagingVirtualHost<?> _virtualHost;
    private final DeletedChildListener _deletedChildListener = new DeletedChildListener();

    private QueueConsumerManagerImpl _queueConsumerManager;

    @ManagedAttributeField( beforeSet = "preSetAlternateExchange", afterSet = "postSetAlternateExchange")
    private Exchange _alternateExchange;

    private volatile QueueConsumer<?> _exclusiveSubscriber;

    private final AtomicLong _targetQueueSize = new AtomicLong(INITIAL_TARGET_QUEUE_SIZE);

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
    private long _queueFlowControlSizeBytes;

    @ManagedAttributeField( afterSet = "checkCapacity" )
    private long _queueFlowResumeSizeBytes;

    @ManagedAttributeField
    private ExclusivityPolicy _exclusive;

    @ManagedAttributeField
    private MessageDurability _messageDurability;

    @ManagedAttributeField
    private Map<String, Map<String,List<String>>> _defaultFilters;

    private Object _exclusiveOwner; // could be connection, session, Principal or a String for the container name

    private final Set<NotificationCheck> _notificationChecks =
            Collections.synchronizedSet(EnumSet.noneOf(NotificationCheck.class));


    private volatile long _estimatedAverageMessageHeaderSize;

    private AtomicBoolean _stopped = new AtomicBoolean(false);

    private final Set<AMQSessionModel> _blockedChannels = new ConcurrentSkipListSet<AMQSessionModel>();

    private final AtomicBoolean _deleted = new AtomicBoolean(false);
    private final SettableFuture<Integer> _deleteFuture = SettableFuture.create();

    private final List<Action<? super X>> _deleteTaskList =
            new CopyOnWriteArrayList<>();


    private LogSubject _logSubject;

    @ManagedAttributeField
    private boolean _noLocal;

    private final AtomicBoolean _overfull = new AtomicBoolean(false);
    private final FlowToDiskChecker _flowToDiskChecker = new FlowToDiskChecker();
    private final CopyOnWriteArrayList<Binding<?>> _bindings = new CopyOnWriteArrayList<>();
    private Map<String, Object> _arguments;

    /** the maximum delivery count for each message on this queue or 0 if maximum delivery count is not to be enforced. */
    @ManagedAttributeField
    private int _maximumDeliveryAttempts;

    private MessageGroupManager _messageGroupManager;

    private QueueNotificationListener  _notificationListener = NULL_NOTIFICATION_LISTENER;
    private final long[] _lastNotificationTimes = new long[NotificationCheck.values().length];

    @ManagedAttributeField
    private String _messageGroupKey;
    @ManagedAttributeField
    private boolean _messageGroupSharedGroups;
    @ManagedAttributeField
    private String _messageGroupDefaultGroup;
    @ManagedAttributeField
    private int _maximumDistinctGroups;
    @ManagedAttributeField
    private long _minimumMessageTtl;
    @ManagedAttributeField
    private long _maximumMessageTtl;
    @ManagedAttributeField
    private boolean _ensureNondestructiveConsumers;
    @ManagedAttributeField
    private volatile boolean _holdOnPublishEnabled;


    private static final int RECOVERING = 1;
    private static final int COMPLETING_RECOVERY = 2;
    private static final int RECOVERED = 3;

    private final AtomicInteger _recovering = new AtomicInteger(RECOVERING);
    private final AtomicInteger _enqueuingWhileRecovering = new AtomicInteger(0);

    private final ConcurrentLinkedQueue<EnqueueRequest> _postRecoveryQueue = new ConcurrentLinkedQueue<>();

    private boolean _closing;
    private final ConcurrentMap<String, Callable<MessageFilter>> _defaultFiltersMap = new ConcurrentHashMap<>();
    private final List<HoldMethod> _holdMethods = new CopyOnWriteArrayList<>();
    private Map<String, String> _mimeTypeToFileExtension = Collections.emptyMap();
    private AdvanceConsumersTask _queueHouseKeepingTask;

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
                final Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getInterestedIterator();
                while (consumerIterator.hasNext())
                {
                    QueueConsumer<?> queueConsumer = consumerIterator.next();
                    //TODO - break here if the consumer has lower priority than the highest notified (presuming iterator is priority ordered)
                    if (notifyConsumer(queueConsumer))
                    {
                        break;
                    }
                }
            }
        }
    }

    private boolean notifyConsumer(final QueueConsumer<?> consumer)
    {
        if(_queueConsumerManager.setNotified(consumer, true))
        {
            consumer.notifyWork();
            return true;
        }
        else
        {
            return false;
        }
    }

    private interface HoldMethod
    {
        boolean isHeld(MessageReference<?> message, long evalutaionTime);
    }

    protected AbstractQueue(Map<String, Object> attributes, QueueManagingVirtualHost<?> virtualHost)
    {
        super(parentsMap(virtualHost), attributes);

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
                         new PrivilegedAction<Object>()
                         {
                             @Override
                             public Object run()
                             {
                                 setAttributes(Collections.<String, Object>singletonMap(AbstractConfiguredObject.DURABLE,
                                                                                        false));
                                 return null;
                             }
                         });
        }

        if(!isDurable() && getMessageDurability() != MessageDurability.NEVER)
        {
            Subject.doAs(getSubjectWithAddedSystemRights(),
                         new PrivilegedAction<Object>()
                         {
                             @Override
                             public Object run()
                             {
                                 setAttributes(Collections.<String, Object>singletonMap(Queue.MESSAGE_DURABILITY,
                                                                                        MessageDurability.NEVER));
                                 return null;
                             }
                         });
        }

        _recovering.set(RECOVERED);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        if (_queueFlowResumeSizeBytes > _queueFlowControlSizeBytes)
        {
            throw new IllegalConfigurationException("Flow resume size can't be greater than flow control size");
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

        _queueConsumerManager = new QueueConsumerManagerImpl(this);
        _logSubject = new QueueLogSubject(this);
        _queueHouseKeepingTask = new AdvanceConsumersTask();
        Subject activeSubject = Subject.getSubject(AccessController.getContext());
        Set<SessionPrincipal> sessionPrincipals = activeSubject == null ? Collections.<SessionPrincipal>emptySet() : activeSubject.getPrincipals(SessionPrincipal.class);
        AMQSessionModel<?> sessionModel;
        if(sessionPrincipals.isEmpty())
        {
            sessionModel = null;
        }
        else
        {
            final SessionPrincipal sessionPrincipal = sessionPrincipals.iterator().next();
            sessionModel = sessionPrincipal.getSession();
        }

        if(sessionModel != null)
        {

            switch(_exclusive)
            {

                case PRINCIPAL:
                    _exclusiveOwner = sessionModel.getAMQPConnection().getAuthorizedPrincipal();
                    break;
                case CONTAINER:
                    _exclusiveOwner = sessionModel.getAMQPConnection().getRemoteContainerName();
                    break;
                case CONNECTION:
                    _exclusiveOwner = sessionModel.getAMQPConnection();
                    addExclusivityConstraint(sessionModel.getAMQPConnection());
                    break;
                case SESSION:
                    _exclusiveOwner = sessionModel;
                    addExclusivityConstraint(sessionModel);
                    break;
                case NONE:
                case LINK:
                    // nothing to do as if link no link associated until there is a consumer associated
                    break;
                default:
                    throw new ServerScopedRuntimeException("Unknown exclusivity policy: "
                                                           + _exclusive
                                                           + " this is a coding error inside Qpid");
            }
        }
        else if(_exclusive == ExclusivityPolicy.PRINCIPAL)
        {
            String owner = MapValueConverter.getStringAttribute(Queue.OWNER, attributes, null);
            if(owner != null)
            {
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
            String owner = MapValueConverter.getStringAttribute(Queue.OWNER, attributes, null);
            if(owner != null)
            {
                _exclusiveOwner = owner;
            }
        }


        if(getLifetimePolicy() == LifetimePolicy.DELETE_ON_CONNECTION_CLOSE)
        {
            if(sessionModel != null)
            {
                addLifetimeConstraint(sessionModel.getAMQPConnection());
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
            if(sessionModel != null)
            {
                addLifetimeConstraint(sessionModel);
            }
            else
            {
                throw new IllegalArgumentException("Queues created with a lifetime policy of "
                                                   + getLifetimePolicy()
                                                   + " must be created from a connection.");
            }
        }


        // Log the creation of this Queue.
        // The priorities display is toggled on if we set priorities > 0
        getEventLogger().message(_logSubject,
                                 getCreatedLogMessage());

        if(getMessageGroupKey() != null)
        {
            if(isMessageGroupSharedGroups())
            {
                _messageGroupManager =
                        new DefinedGroupMessageGroupManager(getMessageGroupKey(), getMessageGroupDefaultGroup(), this);
            }
            else
            {
                _messageGroupManager = new AssignedConsumerMessageGroupManager(getMessageGroupKey(), getMaximumDistinctGroups());
            }
        }
        else
        {
            _messageGroupManager = null;
        }

        _estimatedAverageMessageHeaderSize = getContextValue(Long.class, QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD);
        _mimeTypeToFileExtension = getContextValue(Map.class, MAP_OF_STRING_STRING, MIME_TYPE_TO_FILE_EXTENSION);

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

        updateAlertChecks();
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

    private void addLifetimeConstraint(final Deletable<? extends Deletable> lifetimeObject)
    {
        final Action<Deletable> deleteQueueTask = new Action<Deletable>()
        {
            @Override
            public void performAction(final Deletable object)
            {
                Subject.doAs(getSubjectWithAddedSystemRights(),
                             new PrivilegedAction<Void>()
                             {
                                 @Override
                                 public Void run()
                                 {
                                     AbstractQueue.this.delete();
                                     return null;
                                 }
                             });
            }
        };

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

    public void execute(final String name, Runnable runnable, AccessControlContext context)
    {
        try
        {
            if (_virtualHost.getState() != State.UNAVAILABLE)
            {
                _virtualHost.executeTask(name, runnable, context);
            }
        }
        catch (RejectedExecutionException ree)
        {
            // Ignore - QueueRunner submitted execution as queue was being stopped.
            if(!_stopped.get())
            {
                _logger.error("Unexpected rejected execution", ree);
                throw ree;
            }
        }
    }

    public boolean isExclusive()
    {
        return _exclusive != ExclusivityPolicy.NONE;
    }

    public Exchange<?> getAlternateExchange()
    {
        return _alternateExchange;
    }

    public void setAlternateExchange(Exchange<?> exchange)
    {
        _alternateExchange = exchange;
    }

    @SuppressWarnings("unused")
    private void postSetAlternateExchange()
    {
        if(_alternateExchange != null)
        {
            _alternateExchange.addReference(this);
        }
    }

    @SuppressWarnings("unused")
    private void preSetAlternateExchange()
    {
        if(_alternateExchange != null)
        {
            _alternateExchange.removeReference(this);
        }
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
    public Collection<String> getAvailableAttributes()
    {
        return new ArrayList<String>(_arguments.keySet());
    }

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

    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    // ------ Manage Consumers


    @Override
    public QueueConsumerImpl addConsumer(final ConsumerTarget target,
                                         final FilterManager filters,
                                         final Class<? extends ServerMessage> messageClass,
                                         final String consumerName,
                                         final EnumSet<ConsumerImpl.Option> optionSet,
                                         final Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted
    {

        try
        {
            final QueueConsumerImpl queueConsumer = getTaskExecutor().run(new Task<QueueConsumerImpl, Exception>()
            {
                @Override
                public QueueConsumerImpl execute() throws Exception
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
            if(isEmpty())
            {
                target.queueEmpty();
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

    private QueueConsumerImpl addConsumerInternal(final ConsumerTarget target,
                                                  FilterManager filters,
                                                  final Class<? extends ServerMessage> messageClass,
                                                  final String consumerName,
                                                  EnumSet<ConsumerImpl.Option> optionSet,
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
        switch(_exclusive)
        {
            case CONNECTION:
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = target.getSessionModel().getAMQPConnection();
                    addExclusivityConstraint(target.getSessionModel().getAMQPConnection());
                }
                else
                {
                    if(exclusiveOwner != target.getSessionModel().getAMQPConnection())
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case SESSION:
                if(exclusiveOwner == null)
                {
                    exclusiveOwner = target.getSessionModel();
                    addExclusivityConstraint(target.getSessionModel());
                }
                else
                {
                    if(exclusiveOwner != target.getSessionModel())
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
                Principal currentAuthorizedPrincipal = target.getSessionModel().getAMQPConnection().getAuthorizedPrincipal();
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
                    exclusiveOwner = target.getSessionModel().getAMQPConnection().getRemoteContainerName();
                }
                else
                {
                    if(!exclusiveOwner.equals(target.getSessionModel().getAMQPConnection().getRemoteContainerName()))
                    {
                        throw new ConsumerAccessRefused();
                    }
                }
                break;
            case NONE:
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown exclusivity policy " + _exclusive);
        }

        boolean exclusive =  optionSet.contains(ConsumerImpl.Option.EXCLUSIVE);
        boolean isTransient =  optionSet.contains(ConsumerImpl.Option.TRANSIENT);

        if(_noLocal && !optionSet.contains(ConsumerImpl.Option.NO_LOCAL))
        {
            optionSet = EnumSet.copyOf(optionSet);
            optionSet.add(ConsumerImpl.Option.NO_LOCAL);
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
            optionSet.removeAll(EnumSet.of(ConsumerImpl.Option.SEES_REQUEUES, ConsumerImpl.Option.ACQUIRES));
        }

        QueueConsumerImpl consumer = new QueueConsumerImpl(this,
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

        _queueConsumerManager.addConsumer(consumer);
        if (consumer.isNotifyWorkDesired())
        {
            _activeSubscriberCount.incrementAndGet();
        }

        childAdded(consumer);
        consumer.addChangeListener(_deletedChildListener);

        return consumer;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _closing = true;
        return super.beforeClose();
    }



    void unregisterConsumer(final QueueConsumerImpl consumer)
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
            setExclusiveSubscriber(null);

            consumer.setQueueContext(null);

            if(_exclusive == ExclusivityPolicy.LINK)
            {
                _exclusiveOwner = null;
            }

            if(_messageGroupManager != null)
            {
                resetSubPointersForGroups(consumer);
            }

            // auto-delete queues must be deleted if there are no remaining subscribers

            if(!consumer.isTransient()
               && ( getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS
                    || getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_LINKS )
               && getConsumerCount() == 0
               && !(consumer.isDurable() && _closing))
            {

                _logger.debug("Auto-deleting queue: {}", this);

                Subject.doAs(getSubjectWithAddedSystemRights(), new PrivilegedAction<Object>()
                             {
                                 @Override
                                 public Object run()
                                 {
                                     AbstractQueue.this.delete();
                                     return null;
                                 }
                             });


                // we need to manually fire the event to the removed consumer (which was the last one left for this
                // queue. This is because the delete method uses the consumer set which has just been cleared
                consumer.queueDeleted();

            }
        }

    }

    @Override
    public Collection<QueueConsumer<?>> getConsumers()
    {
        return Lists.newArrayList(_queueConsumerManager.getAllIterator());
    }


    public void resetSubPointersForGroups(QueueConsumer<?> consumer)
    {
        QueueEntry entry = _messageGroupManager.findEarliestAssignedAvailableEntry(consumer);
        _messageGroupManager.clearAssignments(consumer);

        if(entry != null)
        {
            resetSubPointersForGroups(entry);
        }
    }


    public void addBinding(final Binding<?> binding)
    {
        _bindings.add(binding);
        childAdded(binding);
    }

    public void removeBinding(final Binding<?> binding)
    {
        _bindings.remove(binding);
        childRemoved(binding);
    }

    public Collection<Binding<?>> getBindings()
    {
        return Collections.unmodifiableList(_bindings);
    }

    public int getBindingCount()
    {
        return getBindings().size();
    }

    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    // ------ Enqueue / Dequeue

    public final void enqueue(ServerMessage message, Action<? super MessageInstance> action, MessageEnqueueRecord enqueueRecord)
    {

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
                doEnqueue(message, action, enqueueRecord);
            }
        }
        else
        {
            doEnqueue(message, action, enqueueRecord);
        }

        long estimatedQueueSize = _queueStatistics.getQueueSize() + _queueStatistics.getQueueCount() * _estimatedAverageMessageHeaderSize;
        _flowToDiskChecker.flowToDiskAndReportIfNecessary(message.getStoredMessage(), estimatedQueueSize,
                                                          _targetQueueSize.get());
    }

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

    protected void doEnqueue(final ServerMessage message, final Action<? super MessageInstance> action, MessageEnqueueRecord enqueueRecord)
    {
        final QueueConsumer<?> exclusiveSub = _exclusiveSubscriber;
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
        }

    }

    private void updateExpiration(final QueueEntry entry)
    {
        long expiration = entry.getMessage().getExpiration();
        long arrivalTime = entry.getMessage().getArrivalTime();
        if(_minimumMessageTtl != 0l)
        {
            if(arrivalTime == 0)
            {
                arrivalTime = System.currentTimeMillis();
            }
            if(expiration != 0L)
            {
                long calculatedExpiration = arrivalTime+_minimumMessageTtl;
                if(calculatedExpiration > expiration)
                {
                    entry.setExpiration(calculatedExpiration);
                    expiration = calculatedExpiration;
                }
            }
        }
        if(_maximumMessageTtl != 0L)
        {
            if(arrivalTime == 0)
            {
                arrivalTime = System.currentTimeMillis();
            }
            long calculatedExpiration = arrivalTime+_maximumMessageTtl;
            if(expiration == 0L || expiration > calculatedExpiration)
            {
                entry.setExpiration(calculatedExpiration);
            }
        }
    }

    private boolean assign(final QueueConsumer<?> sub, final QueueEntry entry)
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
    public void setTargetSize(final long targetSize)
    {
        if (_targetQueueSize.compareAndSet(_targetQueueSize.get(), targetSize))
        {
            _logger.debug("Queue '{}' target size : {}", getName(), targetSize);
        }
    }

    public long getTotalDequeuedMessages()
    {
        return _queueStatistics.getDequeueCount();
    }

    public long getTotalEnqueuedMessages()
    {
        return _queueStatistics.getEnqueueCount();
    }

    private void setLastSeenEntry(final QueueConsumer<?> sub, final QueueEntry entry)
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

    private void updateSubRequeueEntry(final QueueConsumer<?> sub, final QueueEntry entry)
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
        Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getAllIterator();
        // iterate over all the subscribers, and if they are in advance of this queue entry then move them backwards
        while (consumerIterator.hasNext() && (ignoreAvailable || entry.isAvailable()))
        {
            QueueConsumer<?> sub = consumerIterator.next();

            // we don't make browsers send the same stuff twice
            if (sub.seesRequeues())
            {
                updateSubRequeueEntry(sub, entry);
            }
        }
    }

    public int getConsumerCount()
    {
        return _queueConsumerManager.getAllSize();
    }

    public int getConsumerCountWithCredit()
    {
        return _activeSubscriberCount.get();
    }

    public boolean isUnused()
    {
        return getConsumerCount() == 0;
    }

    public boolean isEmpty()
    {
        return getQueueDepthMessages() == 0;
    }

    @Override
    public int getQueueDepthMessages()
    {
        return _queueStatistics.getQueueCount();
    }

    public long getQueueDepthBytes()
    {
        return _queueStatistics.getQueueSize();
    }

    @Override
    public long getOldestMessageArrivalTime()
    {
        long oldestMessageArrivalTime = -1L;

        while(oldestMessageArrivalTime == -1L)
        {
            QueueEntry entry = getEntries().getOldestEntry();
            if (entry != null)
            {
                ServerMessage message = entry.getMessage();

                if(message != null)
                {
                    try
                    {
                        MessageReference reference = message.newReference();
                        try
                        {
                            oldestMessageArrivalTime = reference.getMessage().getArrivalTime();
                        }
                        finally
                        {
                            reference.release();
                        }


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

    public boolean isDeleted()
    {
        return _deleted.get();
    }

    public List<QueueEntry> getMessagesOnTheQueue()
    {
        ArrayList<QueueEntry> entryList = new ArrayList<QueueEntry>();
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

    public int compareTo(final X o)
    {
        return getName().compareTo(o.getName());
    }

    private boolean hasExclusiveConsumer()
    {
        return _exclusiveSubscriber != null;
    }

    private void setExclusiveSubscriber(QueueConsumer<?> exclusiveSubscriber)
    {
        _exclusiveSubscriber = exclusiveSubscriber;
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


    public QueueEntry getMessageOnTheQueue(final long messageId)
    {
        List<QueueEntry> entries = getMessagesOnTheQueue(new QueueEntryFilter()
        {
            private boolean _complete;

            public boolean accept(QueueEntry entry)
            {
                _complete = entry.getMessage().getMessageNumber() == messageId;
                return _complete;
            }

            public boolean filterComplete()
            {
                return _complete;
            }
        });
        return entries.isEmpty() ? null : entries.get(0);
    }

    public List<QueueEntry> getMessagesOnTheQueue(QueueEntryFilter filter)
    {
        ArrayList<QueueEntry> entryList = new ArrayList<QueueEntry>();
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

                    final boolean done = !node.isDeleted() && visitor.visit(node);
                    if(done)
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

    /**
     * Returns a list of QueueEntries from a given range of queue positions, eg messages 5 to 10 on the queue.
     *
     * The 'queue position' index starts from 1. Using 0 in 'from' will be ignored and continue from 1.
     * Using 0 in the 'to' field will return an empty list regardless of the 'from' value.
     * @param fromPosition first message position
     * @param toPosition last message position
     * @return list of messages
     */
    public List<QueueEntry> getMessagesRangeOnTheQueue(final long fromPosition, final long toPosition)
    {
        return getMessagesOnTheQueue(new QueueEntryFilter()
                                        {
                                            private long position = 0;

                                            public boolean accept(QueueEntry entry)
                                            {
                                                position++;
                                                return (position >= fromPosition) && (position <= toPosition);
                                            }

                                            public boolean filterComplete()
                                            {
                                                return position >= toPosition;
                                            }
                                        });

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
            boolean acquired = node.acquireOrSteal(new Runnable()
                                                    {
                                                        @Override
                                                        public void run()
                                                        {
                                                            dequeueEntry(node);
                                                        }
                                                    });

            if (acquired)
            {
                dequeueEntry(node, txn);
            }

        }

        txn.commit();

        return count;
    }

    private void dequeueEntry(final QueueEntry node)
    {
        ServerTransaction txn = new AutoCommitTransaction(getVirtualHost().getMessageStore());
        dequeueEntry(node, txn);
    }

    private void dequeueEntry(final QueueEntry node, ServerTransaction txn)
    {
        txn.dequeue(node.getEnqueueRecord(),
                    new ServerTransaction.Action()
                    {

                        public void postCommit()
                        {
                            node.delete();
                        }

                        public void onRollback()
                        {

                        }
                    });
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
        // Check access
        authorise(Operation.DELETE);

        if (_deleted.compareAndSet(false, true))
        {
            final int queueDepthMessages = getQueueDepthMessages();
            final List<ListenableFuture<Void>> removeBindingFutures = new ArrayList<>(_bindings.size());
            final ArrayList<Binding<?>> bindingCopy = new ArrayList<>(_bindings);

            // TODO - RG - Need to sort out bindings!
            for (Binding<?> b : bindingCopy)
            {
                removeBindingFutures.add(b.deleteAsync());
            }

            ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(removeBindingFutures);

            addFutureCallback(combinedFuture, new FutureCallback<List<Void>>()
            {
                @Override
                public void onSuccess(final List<Void> result)
                {
                    try
                    {

                        Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getAllIterator();

                        while (consumerIterator.hasNext())
                        {
                            QueueConsumer<?> consumer = consumerIterator.next();

                            if (consumer != null)
                            {
                                consumer.queueDeleted();
                            }
                        }

                        final List<QueueEntry> entries = getMessagesOnTheQueue(new AcquireAllQueueEntryFilter());

                        routeToAlternate(entries);

                        preSetAlternateExchange();
                        _alternateExchange = null;

                        performQueueDeleteTasks();
                        deleted();

                        //Log Queue Deletion
                        getEventLogger().message(_logSubject, QueueMessages.DELETED(getId().toString()));

                        _deleteFuture.set(queueDepthMessages);
                        setState(State.DELETED);
                    }
                    catch(Throwable e)
                    {
                        _deleteFuture.setException(e);
                    }
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    _deleteFuture.setException(t);
                }
            }, getTaskExecutor());

        }
        return _deleteFuture;
    }

    private void routeToAlternate(List<QueueEntry> entries)
    {
        ServerTransaction txn = new LocalTransaction(getVirtualHost().getMessageStore());

        for(final QueueEntry entry : entries)
        {
            // TODO log requeues with a post enqueue action
            int requeues = entry.routeToAlternate(null, txn);

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
    public void checkCapacity(AMQSessionModel<?> channel)
    {
        if(_queueFlowControlSizeBytes != 0L)
        {
            if(_queueStatistics.getQueueSize() > _queueFlowControlSizeBytes)
            {
                _overfull.set(true);
                //Overfull log message
                getEventLogger().message(_logSubject, QueueMessages.OVERFULL(_queueStatistics.getQueueSize(),
                                                                             _queueFlowControlSizeBytes));

                _blockedChannels.add(channel);

                channel.block(this);

                if(_queueStatistics.getQueueSize() <= _queueFlowResumeSizeBytes)
                {

                    //Underfull log message
                    getEventLogger().message(_logSubject,
                                             QueueMessages.UNDERFULL(_queueStatistics.getQueueSize(), _queueFlowResumeSizeBytes));

                   channel.unblock(this);
                   _blockedChannels.remove(channel);

                }
            }



        }
    }

    public void checkCapacity()
    {
        if(getEntries() != null)
        {
            if (_queueFlowControlSizeBytes != 0L)
            {
                if (_overfull.get() && _queueStatistics.getQueueSize() <= _queueFlowResumeSizeBytes)
                {
                    if (_overfull.compareAndSet(true, false))
                    {
                        //Underfull log message
                        getEventLogger().message(_logSubject,
                                                 QueueMessages.UNDERFULL(_queueStatistics.getQueueSize(),
                                                                         _queueFlowResumeSizeBytes));
                    }

                    for (final AMQSessionModel<?> blockedChannel : _blockedChannels)
                    {
                        blockedChannel.unblock(this);
                        _blockedChannels.remove(blockedChannel);
                    }
                }
            }
        }
    }

    void notifyConsumers(QueueEntry entry)
    {

        Iterator<QueueConsumer<?>> nonAcquiringIterator = _queueConsumerManager.getNonAcquiringIterator();
        while (nonAcquiringIterator.hasNext())
        {
            QueueConsumer<?> consumer = nonAcquiringIterator.next();
            if(consumer.hasInterest(entry))
            {
                notifyConsumer(consumer);
            }
        }

        // TODO need to take account of priority and potentially and existing notified
        // we don't want to notify lower priority consumers if there exists a consumer in the notified set
        // which can take the message (implies iterating such that you look at for each priority look at interested then at notified)
        final Iterator<QueueConsumer<?>> interestedIterator = _queueConsumerManager.getInterestedIterator();
        while (entry.isAvailable() && interestedIterator.hasNext())
        {
            QueueConsumer<?> consumer = interestedIterator.next();
            if(consumer.hasInterest(entry) && notifyConsumer(consumer))
            {
                break;
            }
        }
    }

    void notifyOtherConsumers(final QueueConsumer<?> excludedConsumer)
    {
        final Iterator<QueueConsumer<?>> interestedIterator = _queueConsumerManager.getInterestedIterator();
        while (interestedIterator.hasNext())
        {
            QueueConsumer<?> consumer = interestedIterator.next();

            if (excludedConsumer != consumer)
            {
                if (notifyConsumer(consumer))
                {
                    break;
                }
            }
        }
    }


    MessageContainer deliverSingleMessage(QueueConsumer<?> consumer)
    {
        boolean queueEmpty = false;
        MessageContainer messageContainer = null;

        _queueConsumerManager.setNotified(consumer, false);

        try
        {
            if (!consumer.isSuspended())
            {
                messageContainer = attemptDelivery(consumer);
                if(messageContainer != null)
                {
                    _queueConsumerManager.setNotified(consumer, true);
                }

                if (messageContainer == null && getNextAvailableEntry(consumer) == null)
                {
                    queueEmpty = true;
                }

                if(messageContainer == null && consumer.acquires())
                {
                    // TODO - Should be only checking for available messages
                    if(!isEmpty())
                    {
                        notifyOtherConsumers(consumer);
                    }
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
            if(queueEmpty)
            {
                consumer.queueEmpty();
            }

            consumer.flushBatched();
        }

        return messageContainer;
    }

    public static class MessageContainer
    {
        public final MessageInstance _messageInstance;
        public final MessageReference<?> _messageReference;

        public MessageContainer(final MessageInstance messageInstance,
                                final MessageReference<?> messageReference)
        {
            _messageInstance = messageInstance;
            _messageReference = messageReference;
        }
    }

    /**
     * Attempt delivery for the given consumer.
     *
     * Looks up the next node for the consumer and attempts to deliver it.
     *
     *
     * @param sub the consumer
     * @return true if we have completed all possible deliveries for this sub.
     */
    private MessageContainer attemptDelivery(QueueConsumer<?> sub)
    {
        MessageContainer messageContainer = null;
        // avoid referring old deleted queue entry in sub._queueContext._lastSeen
        QueueEntry node  = getNextAvailableEntry(sub);
        boolean subActive = sub.isActive() && !sub.isSuspended();

        if (node != null && subActive && (sub.getPriority() == Integer.MAX_VALUE || noHigherPriorityWithCredit(sub, node)))
        {

            if (_virtualHost.getState() != State.ACTIVE)
            {
                throw new ConnectionScopedRuntimeException("Delivery halted owing to " +
                                                           "virtualhost state " + _virtualHost.getState());
            }

            if (node.isAvailable())
            {
                if (sub.hasInterest(node) && mightAssign(sub, node))
                {
                    if (sub.allocateCredit(node))
                    {
                        MessageReference messageReference = null;
                        try
                        {

                            if ((sub.acquires() && !assign(sub, node))
                                || (!sub.acquires() && (messageReference = node.newMessageReference()) == null))
                            {
                                // restore credit here that would have been taken away by wouldSuspend since we didn't manage
                                // to acquire the entry for this consumer
                                sub.restoreCredit(node);
                            }
                            else
                            {
                                setLastSeenEntry(sub, node);
                                messageContainer = new MessageContainer(node, messageReference);
                            }
                        }
                        finally
                        {
                            if (messageReference != null)
                            {
                                messageReference.release();
                            }
                        }
                    }
                    else
                    {
                        sub.awaitCredit(node);
                    }
                }
            }
        }
        return messageContainer;
    }

    boolean noHigherPriorityWithCredit(final QueueConsumer<?> sub, final QueueEntry queueEntry)
    {
        Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getAllIterator();

        while (consumerIterator.hasNext())
        {
            QueueConsumer<?> consumer = consumerIterator.next();
            if(consumer.getPriority() > sub.getPriority())
            {
                if(consumer.isNotifyWorkDesired() && consumer.hasInterest(queueEntry) && getNextAvailableEntry(consumer) != null)
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


    QueueEntry getNextAvailableEntry(final QueueConsumer sub)
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
                    if (node.acquire())
                    {
                        dequeueEntry(node);
                    }
                }

                if(QueueContext._lastSeenUpdater.compareAndSet(context, lastSeen, node))
                {
                    QueueContext._releasedUpdater.compareAndSet(context, releasedNode, null);
                }

                lastSeen = context.getLastSeenEntry();
                releasedNode = context.getReleasedEntry();
                node = (releasedNode != null && lastSeen.compareTo(releasedNode)>=0) ? releasedNode : getEntries().next(
                        lastSeen);
            }
            return node;
        }
        else
        {
            return null;
        }
    }

    public boolean isEntryAheadOfConsumer(QueueEntry entry, QueueConsumer<?> sub)
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


    public void checkMessageStatus()
    {
        QueueEntryIterator queueListIterator = getEntries().iterator();

        final long estimatedQueueSize = _queueStatistics.getQueueSize() + _queueStatistics.getQueueCount() * _estimatedAverageMessageHeaderSize;
        _flowToDiskChecker.reportFlowToDiskStatusIfNecessary(estimatedQueueSize, _targetQueueSize.get());

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

        long cumulativeQueueSize = 0;
        while (!_stopped.get() && queueListIterator.advance())
        {
            final QueueEntry node = queueListIterator.getNode();
            // Only process nodes that are not currently deleted and not dequeued
            if (!node.isDeleted())
            {
                // If the node has expired then acquire it
                if (node.expired())
                {
                    boolean acquiredForDequeueing = node.acquireOrSteal(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            dequeueEntry(node);
                        }
                    });

                    if(acquiredForDequeueing)
                    {
                        _logger.debug("Dequeuing expired node {}", node);
                        // Then dequeue it.
                        dequeueEntry(node);
                    }
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
                        cumulativeQueueSize += msg.getSize() + _estimatedAverageMessageHeaderSize;
                        _flowToDiskChecker.flowToDiskIfNecessary(msg.getStoredMessage(), cumulativeQueueSize,
                                                                 _targetQueueSize.get());

                        for(NotificationCheck check : perMessageChecks)
                        {
                            checkForNotification(msg, listener, currentTime, thresholdTime, check);
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

    @Override
    public long getPotentialMemoryFootprint()
    {
        return Math.max(getContextValue(Long.class,QUEUE_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT),
                        getQueueDepthBytes() + getContextValue(Long.class, QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD) * getQueueDepthMessages());
    }

    public long getAlertRepeatGap()
    {
        return _alertRepeatGap;
    }

    public long getAlertThresholdMessageAge()
    {
        return _alertThresholdMessageAge;
    }

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

    public long getAlertThresholdQueueDepthBytes()
    {
        return _alertThresholdQueueDepthBytes;
    }

    public long getAlertThresholdMessageSize()
    {
        return _alertThresholdMessageSize;
    }

    public long getQueueFlowControlSizeBytes()
    {
        return _queueFlowControlSizeBytes;
    }

    public long getQueueFlowResumeSizeBytes()
    {
        return _queueFlowResumeSizeBytes;
    }

    public Set<NotificationCheck> getNotificationChecks()
    {
        return _notificationChecks;
    }

    private static class DeleteDeleteTask implements Action<Deletable>
    {

        private final Deletable<? extends Deletable> _lifetimeObject;
        private final Action<? super Deletable> _deleteQueueOwnerTask;

        public DeleteDeleteTask(final Deletable<? extends Deletable> lifetimeObject,
                                final Action<? super Deletable> deleteQueueOwnerTask)
        {
            _lifetimeObject = lifetimeObject;
            _deleteQueueOwnerTask = deleteQueueOwnerTask;
        }

        @Override
        public void performAction(final Deletable object)
        {
            _lifetimeObject.removeDeleteTask(_deleteQueueOwnerTask);
        }
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
            InputStream inputStream = new QpidByteBufferInputStream(message.getContent(0, length));

            if (_limit != UNLIMITED && _decompressBeforeLimiting)
            {
                inputStream = new GZIPInputStream(inputStream);
                inputStream = ByteStreams.limit(inputStream, _limit);
                outputStream = new GZIPOutputStream(outputStream);
            }

            try
            {
                ByteStreams.copy(inputStream, outputStream);
            }
            finally
            {
                outputStream.close();
                inputStream.close();
            }
        }
    }

    private static class AcquireAllQueueEntryFilter implements QueueEntryFilter
    {
        public boolean accept(QueueEntry entry)
        {
            return entry.acquire();
        }

        public boolean filterComplete()
        {
            return false;
        }
    }

    public List<Long> getMessagesOnTheQueue(int num)
    {
        return getMessagesOnTheQueue(num, 0);
    }

    public List<Long> getMessagesOnTheQueue(int num, int offset)
    {
        ArrayList<Long> ids = new ArrayList<Long>(num);
        QueueEntryIterator it = getEntries().iterator();
        for (int i = 0; i < offset; i++)
        {
            it.advance();
        }

        for (int i = 0; i < num && !it.atTail(); i++)
        {
            it.advance();
            ids.add(it.getNode().getMessage().getMessageNumber());
        }
        return ids;
    }

    public long getTotalEnqueuedBytes()
    {
        return _queueStatistics.getEnqueueSize();
    }

    public long getTotalDequeuedBytes()
    {
        return _queueStatistics.getDequeueSize();
    }

    public long getPersistentEnqueuedBytes()
    {
        return _queueStatistics.getPersistentEnqueueSize();
    }

    public long getPersistentDequeuedBytes()
    {
        return _queueStatistics.getPersistentDequeueSize();
    }

    public long getPersistentEnqueuedMessages()
    {
        return _queueStatistics.getPersistentEnqueueCount();
    }

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

    public long getUnacknowledgedMessages()
    {
        return _queueStatistics.getUnackedCount();
    }

    public long getUnacknowledgedBytes()
    {
        return _queueStatistics.getUnackedSize();
    }

    @Override
    public int getMaximumDeliveryAttempts()
    {
        return _maximumDeliveryAttempts;
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

    public void setNotificationListener(QueueNotificationListener  listener)
    {
        _notificationListener = listener == null ? NULL_NOTIFICATION_LISTENER : listener;
    }

    public final  <M extends ServerMessage<? extends StorableMessageMetaData>> int send(final M message,
                                                                                        final String routingAddress,
                                                                                        final InstanceProperties instanceProperties,
                                                                                        final ServerTransaction txn,
                                                                                        final Action<? super MessageInstance> postEnqueueAction)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }

        if(!message.isReferenced(this))
        {
            txn.enqueue(this, message, new ServerTransaction.EnqueueAction()
            {
                MessageReference _reference = message.newReference();

                public void postCommit(MessageEnqueueRecord... records)
                {
                    try
                    {
                        AbstractQueue.this.enqueue(message, postEnqueueAction, records[0]);
                    }
                    finally
                    {
                        _reference.release();
                    }
                }

                public void onRollback()
                {
                    _reference.release();
                }
            });
            return 1;
        }
        else
        {
            return 0;
        }

    }

    @Override
    public boolean verifySessionAccess(final AMQSessionModel<?> session)
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
                allowed = _exclusiveSubscriber == null || _exclusiveSubscriber.getSessionModel() == session;
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
                Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getAllIterator();

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
                AMQSessionModel session = null;
                Iterator<QueueConsumer<?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?> c = queueConsumerIterator.next();

                    if(session == null)
                    {
                        session = c.getSessionModel();
                    }
                    else if(!session.equals(c.getSessionModel()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = session;
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSessionModel().getAMQPConnection();
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
                Iterator<QueueConsumer<?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?> c = queueConsumerIterator.next();
                    if(con == null)
                    {
                        con = c.getSessionModel().getAMQPConnection();
                    }
                    else if(!con.equals(c.getSessionModel().getAMQPConnection()))
                    {
                        throw new ExistingConsumerPreventsExclusive();
                    }
                }
                _exclusiveOwner = con;
                break;
            case SESSION:
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQSessionModel)_exclusiveOwner).getAMQPConnection();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSessionModel().getAMQPConnection();
        }
    }

    private void switchToContainerExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch(_exclusive)
        {
            case NONE:
            case PRINCIPAL:
                String containerID = null;
                Iterator<QueueConsumer<?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?> c = queueConsumerIterator.next();
                    if(containerID == null)
                    {
                        containerID = c.getSessionModel().getAMQPConnection().getRemoteContainerName();
                    }
                    else if(!containerID.equals(c.getSessionModel().getAMQPConnection().getRemoteContainerName()))
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
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQSessionModel)_exclusiveOwner).getAMQPConnection().getRemoteContainerName();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSessionModel().getAMQPConnection().getRemoteContainerName();
        }
    }

    private void switchToPrincipalExclusivity() throws ExistingConsumerPreventsExclusive
    {
        switch(_exclusive)
        {
            case NONE:
            case CONTAINER:
                Principal principal = null;
                Iterator<QueueConsumer<?>> queueConsumerIterator = _queueConsumerManager.getAllIterator();
                while(queueConsumerIterator.hasNext())
                {
                    QueueConsumer<?> c = queueConsumerIterator.next();
                    if(principal == null)
                    {
                        principal = c.getSessionModel().getAMQPConnection().getAuthorizedPrincipal();
                    }
                    else if(!Objects.equals(principal.getName(),
                                            c.getSessionModel().getAMQPConnection().getAuthorizedPrincipal().getName()))
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
                _exclusiveOwner = _exclusiveOwner == null ? null : ((AMQSessionModel)_exclusiveOwner).getAMQPConnection().getAuthorizedPrincipal();
                break;
            case LINK:
                _exclusiveOwner = _exclusiveSubscriber == null ? null : _exclusiveSubscriber.getSessionModel().getAMQPConnection().getAuthorizedPrincipal();
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

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.DELETED)
    private ListenableFuture<Void> doDeleteBeforeInitialize()
    {
        preSetAlternateExchange();
        setState(State.DELETED);
        return Futures.immediateFuture(null);
    }

    @StateTransition(currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        ListenableFuture<Integer> removeFuture = deleteAndReturnCountAsync();
        return doAfter(removeFuture, new Runnable()
        {
            @Override
            public void run()
            {

            }
        });

    }


    @Override
    public ExclusivityPolicy getExclusive()
    {
        return _exclusive;
    }

    @Override
    public boolean isNoLocal()
    {
        return _noLocal;
    }

    @Override
    public String getMessageGroupKey()
    {
        return _messageGroupKey;
    }

    @Override
    public boolean isMessageGroupSharedGroups()
    {
        return _messageGroupSharedGroups;
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
        return _overfull.get();
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
    {
        if(clazz == Binding.class)
        {
            return (Collection<C>) getBindings();
        }
        else if(clazz == org.apache.qpid.server.model.Consumer.class)
        {
            return _queueConsumerManager == null
                    ? Collections.<C>emptySet()
                    : (Collection<C>) Lists.newArrayList(_queueConsumerManager.getAllIterator());
        }
        else return Collections.emptySet();
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                      final Map<String, Object> attributes,
                                                      final ConfiguredObject... otherParents)
    {
        if(childClass == Binding.class && otherParents.length == 1 && otherParents[0] instanceof Exchange)
        {
            final String bindingKey = (String) attributes.get("name");
            ((Exchange<?>)otherParents[0]).addBinding(bindingKey, this,
                                                           (Map<String,Object>) attributes.get(Binding.ARGUMENTS));
            for(Binding binding : _bindings)
            {
                if(binding.getExchange() == otherParents[0] && binding.getName().equals(bindingKey))
                {
                    return Futures.immediateFuture((C) binding);
                }
            }
            return null;
        }
        return super.addChildAsync(childClass, attributes, otherParents);
    }

    @Override
    public boolean changeAttribute(String name, Object desired) throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        if(EXCLUSIVE.equals(name))
        {
            ExclusivityPolicy existingPolicy = getExclusive();
            if(super.changeAttribute(name, desired))
            {
                try
                {
                    if(existingPolicy != _exclusive)
                    {
                        ExclusivityPolicy newPolicy = _exclusive;
                        _exclusive = existingPolicy;
                        updateExclusivityPolicy(newPolicy);
                    }
                    return true;
                }
                catch (ExistingConsumerPreventsExclusive existingConsumerPreventsExclusive)
                {
                    throw new IllegalArgumentException("Unable to set exclusivity policy to " + desired + " as an existing combinations of consumers prevents this");
                }
            }
            return false;
        }

        return super.changeAttribute(name, desired);

    }

    private static final String[] NON_NEGATIVE_NUMBERS = {
        ALERT_REPEAT_GAP,
        ALERT_THRESHOLD_MESSAGE_AGE,
        ALERT_THRESHOLD_MESSAGE_SIZE,
        ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
        ALERT_THRESHOLD_QUEUE_DEPTH_BYTES,
        QUEUE_FLOW_CONTROL_SIZE_BYTES,
        QUEUE_FLOW_RESUME_SIZE_BYTES,
        MAXIMUM_DELIVERY_ATTEMPTS
    };

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        Queue<?> queue = (Queue) proxyForValidation;
        long queueFlowControlSize = queue.getQueueFlowControlSizeBytes();
        long queueFlowControlResumeSize = queue.getQueueFlowResumeSizeBytes();
        if (queueFlowControlResumeSize > queueFlowControlSize)
        {
            throw new IllegalConfigurationException("Flow resume size can't be greater than flow control size");
        }

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
                if (messageConverter != null)
                {
                    return new JsonMessageContent(messageReference,
                                                  (InternalMessage) messageConverter.convert(message, getVirtualHost()),
                                                  limit);
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

    private class FlowToDiskChecker
    {
        final AtomicBoolean _lastReportedFlowToDiskStatus = new AtomicBoolean(false);

        void flowToDiskIfNecessary(StoredMessage<?> storedMessage, long estimatedQueueSize, final long targetQueueSize)
        {
            if ((estimatedQueueSize > targetQueueSize) && storedMessage.isInMemory())
            {
                storedMessage.flowToDisk();
            }
        }

        void flowToDiskAndReportIfNecessary(StoredMessage<?> storedMessage,
                                            final long estimatedQueueSize,
                                            final long targetQueueSize)
        {
            flowToDiskIfNecessary(storedMessage, estimatedQueueSize, targetQueueSize);
            reportFlowToDiskStatusIfNecessary(estimatedQueueSize, targetQueueSize);
        }

        void reportFlowToDiskStatusIfNecessary(final long estimatedQueueSize, final long targetQueueSize)
        {
            if (estimatedQueueSize > targetQueueSize)
            {
                reportFlowToDiskActiveIfNecessary(estimatedQueueSize, targetQueueSize);
            }
            else
            {
                reportFlowToDiskInactiveIfNecessary(estimatedQueueSize, targetQueueSize);
            }
        }

        private void reportFlowToDiskActiveIfNecessary(long estimatedQueueSize, long targetQueueSize)
        {
            if (!_lastReportedFlowToDiskStatus.getAndSet(true))
            {
                getEventLogger().message(_logSubject, QueueMessages.FLOW_TO_DISK_ACTIVE(estimatedQueueSize / 1024,
                                                                                        targetQueueSize / 1024));
            }
        }

        private void reportFlowToDiskInactiveIfNecessary(long estimatedQueueSize, long targetQueueSize)
        {
            if (_lastReportedFlowToDiskStatus.getAndSet(false))
            {
                getEventLogger().message(_logSubject, QueueMessages.FLOW_TO_DISK_INACTIVE(estimatedQueueSize / 1024,
                                                                                          targetQueueSize / 1024));
            }
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

            Iterator<QueueConsumer<?>> consumerIterator = _queueConsumerManager.getAllIterator();

            while (consumerIterator.hasNext() && !isDeleted())
            {
                QueueConsumer<?> sub = consumerIterator.next();
                if(sub.acquires())
                {
                    getNextAvailableEntry(sub);
                }
            }
        }
    }
}
