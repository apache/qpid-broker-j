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
package org.apache.qpid.server.virtualhost;

import static java.util.Collections.newSetFromMap;

import java.io.File;
import java.security.AccessControlContext;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.pool.SuppressingInheritedAccessControlContextThreadFactory;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.exchange.DefaultDestination;
import org.apache.qpid.server.exchange.ExchangeImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.MessageStoreMessages;
import org.apache.qpid.server.logging.messages.VirtualHostMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageNode;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ConnectionValidator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemNodeCreator;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.stats.StatisticsCounter;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.GenericRecoverer;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreProvider;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.NetworkConnectionScheduler;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.MapValueConverter;

public abstract class AbstractVirtualHost<X extends AbstractVirtualHost<X>> extends AbstractConfiguredObject<X>
        implements VirtualHostImpl<X, AMQQueue<?>, ExchangeImpl<?>>, EventListener, StatisticsGatherer
{
    private final Collection<ConnectionValidator> _connectionValidators = new ArrayList<>();


    private final Set<AMQPConnection<?>> _connections = newSetFromMap(new ConcurrentHashMap<AMQPConnection<?>, Boolean>());
    private final Set<VirtualHostConnectionListener> _connectionAssociationListeners = new CopyOnWriteArraySet<>();
    private final AccessControlContext _housekeepingJobContext;
    private final AccessControlContext _fileSystemSpaceCheckerJobContext;
    private final AtomicBoolean _acceptsConnections = new AtomicBoolean(false);

    private static enum BlockingType { STORE, FILESYSTEM };

    private static final String USE_ASYNC_RECOVERY = "use_async_message_store_recovery";

    public static final String DEFAULT_DLQ_NAME_SUFFIX = "_DLQ";
    public static final String DLQ_ROUTING_KEY = "dlq";
    public static final String CREATE_DLQ_ON_CREATION = "x-qpid-dlq-enabled"; // TODO - this value should change
    private static final int MAX_LENGTH = 255;

    private static final Logger _logger = LoggerFactory.getLogger(AbstractVirtualHost.class);

    private static final int HOUSEKEEPING_SHUTDOWN_TIMEOUT = 5;

    private ScheduledThreadPoolExecutor _houseKeepingTaskExecutor;

    private final Broker<?> _broker;

    private final DtxRegistry _dtxRegistry;

    private final SystemNodeRegistry _systemNodeRegistry = new SystemNodeRegistry();

    private final StatisticsCounter _messagesDelivered, _dataDelivered, _messagesReceived, _dataReceived;

    private final Map<String, LinkRegistry> _linkRegistry = new HashMap<String, LinkRegistry>();
    private AtomicBoolean _blocked = new AtomicBoolean();

    private final Map<String, MessageDestination> _systemNodeDestinations =
            Collections.synchronizedMap(new HashMap<String,MessageDestination>());

    private final Map<String, MessageSource> _systemNodeSources =
            Collections.synchronizedMap(new HashMap<String,MessageSource>());

    private final EventLogger _eventLogger;

    private final List<VirtualHostAlias> _aliases = new ArrayList<VirtualHostAlias>();
    private final VirtualHostNode<?> _virtualHostNode;

    private final AtomicLong _targetSize = new AtomicLong(100 * 1024 * 1024);

    private MessageStoreLogSubject _messageStoreLogSubject;

    private final Set<BlockingType> _blockingReasons = Collections.synchronizedSet(EnumSet.noneOf(BlockingType.class));

    private NetworkConnectionScheduler _networkConnectionScheduler;

    private final VirtualHostPrincipal _principal;

    @ManagedAttributeField
    private boolean _queue_deadLetterQueueEnabled;

    @ManagedAttributeField
    private long _housekeepingCheckPeriod;

    @ManagedAttributeField
    private long _storeTransactionIdleTimeoutClose;

    @ManagedAttributeField
    private long _storeTransactionIdleTimeoutWarn;

    @ManagedAttributeField
    private long _storeTransactionOpenTimeoutClose;

    @ManagedAttributeField
    private long _storeTransactionOpenTimeoutWarn;

    @ManagedAttributeField
    private int _housekeepingThreadCount;

    @ManagedAttributeField
    private int _connectionThreadPoolSize;

    @ManagedAttributeField
    private int _numberOfSelectors;

    @ManagedAttributeField
    private List<String> _enabledConnectionValidators;

    @ManagedAttributeField
    private List<String> _disabledConnectionValidators;

    @ManagedAttributeField
    private List<String> _globalAddressDomains;

    private boolean _useAsyncRecoverer;

    private MessageDestination _defaultDestination;

    private MessageStore _messageStore;
    private MessageStoreRecoverer _messageStoreRecoverer;
    private final FileSystemSpaceChecker _fileSystemSpaceChecker;
    private int _fileSystemMaxUsagePercent;
    private Collection<VirtualHostLogger> _virtualHostLoggersToClose;

    public AbstractVirtualHost(final Map<String, Object> attributes, VirtualHostNode<?> virtualHostNode)
    {
        super(parentsMap(virtualHostNode), attributes);
        _broker = virtualHostNode.getParent(Broker.class);
        _virtualHostNode = virtualHostNode;

        _dtxRegistry = new DtxRegistry();

        _eventLogger = _broker.getParent(SystemConfig.class).getEventLogger();

        _eventLogger.message(VirtualHostMessages.CREATED(getName()));

        _defaultDestination = new DefaultDestination(this);

        _messagesDelivered = new StatisticsCounter("messages-delivered-" + getName());
        _dataDelivered = new StatisticsCounter("bytes-delivered-" + getName());
        _messagesReceived = new StatisticsCounter("messages-received-" + getName());
        _dataReceived = new StatisticsCounter("bytes-received-" + getName());
        _principal = new VirtualHostPrincipal(this);

        _housekeepingJobContext = SecurityManager.getSystemTaskControllerContext("Housekeeping["+getName()+"]", _principal);
        _fileSystemSpaceCheckerJobContext = SecurityManager.getSystemTaskControllerContext("FileSystemSpaceChecker["+getName()+"]", _principal);

        _fileSystemSpaceChecker = new FileSystemSpaceChecker();

        addChangeListener(new TargetSizeAssigningListener());
    }

    public void onValidate()
    {
        super.onValidate();
        String name = getName();
        if (name == null || "".equals(name.trim()))
        {
            throw new IllegalConfigurationException("Virtual host name must be specified");
        }
        String type = getType();
        if (type == null || "".equals(type.trim()))
        {
            throw new IllegalConfigurationException("Virtual host type must be specified");
        }
        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
        if(getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                validateGlobalAddressDomain(domain);
            }
        }

        validateConnectionThreadPoolSettings(this);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        VirtualHost<?, ?, ?> virtualHost = (VirtualHost<?, ?, ?>) proxyForValidation;

        if(changedAttributes.contains(GLOBAL_ADDRESS_DOMAINS))
        {

            if(virtualHost.getGlobalAddressDomains() != null)
            {
                for(String name : virtualHost.getGlobalAddressDomains())
                {
                    validateGlobalAddressDomain(name);
                }
            }
        }

        if (changedAttributes.contains(CONNECTION_THREAD_POOL_SIZE) || changedAttributes.contains(NUMBER_OF_SELECTORS))
        {
            validateConnectionThreadPoolSettings(virtualHost);
        }
    }

    private void validateGlobalAddressDomain(final String name)
    {
        String regex = "/(/?)([\\w_\\-:.\\$]+/)*[\\w_\\-:.\\$]+";
        if(!name.matches(regex))
        {
            throw new IllegalArgumentException("'"+name+"' is not a valid global address domain");
        }
    }

    @Override
    public MessageStore getMessageStore()
    {
        return _messageStore;
    }

    @Override
    public void validateOnCreate()
    {
        super.validateOnCreate();
        validateMessageStoreCreation();
    }

    private void validateConnectionThreadPoolSettings(VirtualHost<?,?,?> virtualHost)
    {
        if (virtualHost.getConnectionThreadPoolSize() < 1)
        {
            throw new IllegalConfigurationException(String.format("Thread pool size %d on VirtualHost %s must be greater than zero.", virtualHost.getConnectionThreadPoolSize(), getName()));
        }
        if (virtualHost.getNumberOfSelectors() < 1)
        {
            throw new IllegalConfigurationException(String.format("Number of Selectors %d on VirtualHost %s must be greater than zero.", virtualHost.getNumberOfSelectors(), getName()));
        }
        if (virtualHost.getConnectionThreadPoolSize() <= virtualHost.getNumberOfSelectors())
        {
            throw new IllegalConfigurationException(String.format("Number of Selectors %d on VirtualHost %s must be greater than the connection pool size %d.", virtualHost.getNumberOfSelectors(), getName(), virtualHost.getConnectionThreadPoolSize()));
        }
    }

    protected void validateMessageStoreCreation()
    {
        MessageStore store = createMessageStore();
        if (store != null)
        {
            try
            {
                store.openMessageStore(this);
            }
            catch (Exception e)
            {
                throw new IllegalConfigurationException("Cannot open virtual host message store:" + e.getMessage(), e);
            }
            finally
            {
                try
                {
                    store.closeMessageStore();
                }
                catch(Exception e)
                {
                    _logger.warn("Failed to close database", e);
                }
            }
        }
    }

    @Override
    protected void onExceptionInOpen(RuntimeException e)
    {
        super.onExceptionInOpen(e);
        shutdownHouseKeeping();
        closeNetworkConnectionScheduler();
        closeMessageStore();
        stopLogging(new ArrayList<>(getChildren(VirtualHostLogger.class)));
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        registerSystemNodes();

        _messageStore = createMessageStore();

        _messageStoreLogSubject = new MessageStoreLogSubject(getName(), _messageStore.getClass().getSimpleName());

        _messageStore.addEventListener(this, Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
        _messageStore.addEventListener(this, Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);

        _fileSystemMaxUsagePercent = getContextValue(Integer.class, Broker.STORE_FILESYSTEM_MAX_USAGE_PERCENT);


        QpidServiceLoader serviceLoader = new QpidServiceLoader();
        for(ConnectionValidator validator : serviceLoader.instancesOf(ConnectionValidator.class))
        {
            if((_enabledConnectionValidators.isEmpty()
                && (_disabledConnectionValidators.isEmpty()) || !_disabledConnectionValidators.contains(validator.getType()))
               || _enabledConnectionValidators.contains(validator.getType()))
            {
                _connectionValidators.add(validator);
            }

        }
    }

    private void checkVHostStateIsActive()
    {
        if (getState() != State.ACTIVE)
        {
            throw new IllegalStateException("The virtual host state of " + getState()
                                            + " does not permit this operation.");
        }
    }

    private void registerSystemNodes()
    {
        QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
        Iterable<SystemNodeCreator> factories = qpidServiceLoader.instancesOf(SystemNodeCreator.class);
        for(SystemNodeCreator creator : factories)
        {
            creator.register(_systemNodeRegistry);
        }
    }

    protected abstract MessageStore createMessageStore();

    protected boolean isStoreEmpty()
    {
        final IsStoreEmptyHandler isStoreEmptyHandler = new IsStoreEmptyHandler();

        getDurableConfigurationStore().visitConfiguredObjectRecords(isStoreEmptyHandler);

        return isStoreEmptyHandler.isEmpty();
    }

    private ListenableFuture<List<Void>> createDefaultExchanges()
    {
        return Subject.doAs(getSecurityManager().getSubjectWithAddedSystemRights(), new PrivilegedAction<ListenableFuture<List<Void>>>()
        {

            @Override
            public ListenableFuture<List<Void>> run()
            {
                List<ListenableFuture<Void>> standardExchangeFutures = new ArrayList<>();
                standardExchangeFutures.add(addStandardExchange(ExchangeDefaults.DIRECT_EXCHANGE_NAME, ExchangeDefaults.DIRECT_EXCHANGE_CLASS));
                standardExchangeFutures.add(addStandardExchange(ExchangeDefaults.TOPIC_EXCHANGE_NAME, ExchangeDefaults.TOPIC_EXCHANGE_CLASS));
                standardExchangeFutures.add(addStandardExchange(ExchangeDefaults.HEADERS_EXCHANGE_NAME, ExchangeDefaults.HEADERS_EXCHANGE_CLASS));
                standardExchangeFutures.add(addStandardExchange(ExchangeDefaults.FANOUT_EXCHANGE_NAME, ExchangeDefaults.FANOUT_EXCHANGE_CLASS));
                return Futures.allAsList(standardExchangeFutures);
            }

            ListenableFuture<Void> addStandardExchange(String name, String type)
            {

                Map<String, Object> attributes = new HashMap<String, Object>();
                attributes.put(Exchange.NAME, name);
                attributes.put(Exchange.TYPE, type);
                attributes.put(Exchange.ID, UUIDGenerator.generateExchangeUUID(name, getName()));
                final ListenableFuture<ExchangeImpl> future = addExchangeAsync(attributes);
                final SettableFuture<Void> returnVal = SettableFuture.create();
                Futures.addCallback(future, new FutureCallback<ExchangeImpl>()
                {
                    @Override
                    public void onSuccess(final ExchangeImpl result)
                    {
                        try
                        {
                            childAdded(result);
                            returnVal.set(null);
                        }
                        catch (Throwable t)
                        {
                            returnVal.setException(t);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t)
                    {
                        returnVal.setException(t);
                    }
                }, getTaskExecutor());

                return returnVal;
            }
        });
    }

    protected MessageStoreLogSubject getMessageStoreLogSubject()
    {
        return _messageStoreLogSubject;
    }

    @Override
    public Collection<AMQPConnection<?>> getConnections()
    {
        return _connections;
    }

    @Override
    public Collection<Map<String, Object>> listConnections()
    {
        Collection<Map<String, Object>> connections = new ArrayList<>();
        for (AMQPConnection<?> connection : _connections)
        {
            Map<String, Object> connectionData = new HashMap<>();
            connections.add(connectionData);
            for (String name : connection.getAttributeNames())
            {
                Object value = connection.getAttribute(name);
                if (value instanceof ConfiguredObject)
                {
                    connectionData.put(name, ((ConfiguredObject) value).getName());
                }
                else if (ConfiguredObject.CONTEXT.equals(name))
                {
                    Map<String, String> context =
                            (Map<String, String>) connection.getActualAttributes().get(ConfiguredObject.CONTEXT);
                    if (context != null && !context.isEmpty())
                    {
                        connectionData.put(name, context);
                    }
                }
                else if (value instanceof Collection)
                {
                    List<Object> converted = new ArrayList<>();
                    for (Object member : (Collection) value)
                    {
                        if (member instanceof ConfiguredObject)
                        {
                            converted.add(((ConfiguredObject) member).getName());
                        }
                        else
                        {
                            converted.add(member);
                        }
                    }
                    connectionData.put(name, converted);
                }
                else if (value != null)
                {
                    connectionData.put(name, value);
                }
            }
            Map<String, Number> statMap = connection.getStatistics();
            if(!statMap.isEmpty())
            {
                connectionData.put("statistics", new TreeMap(statMap));
            }
        }
        return connections;
    }


    @Override
    public Connection<?> getConnection(String name)
    {
        for (Connection<?> connection : _connections)
        {
            if (connection.getName().equals(name))
            {
                return connection;
            }
        }
        return null;
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(Class<C> clazz)
    {
        if(clazz == VirtualHostAlias.class)
        {
            return (Collection<C>) getAliases();
        }
        else
        {
            return super.getChildren(clazz);
        }
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass, Map<String, Object> attributes, ConfiguredObject... otherParents)
    {
        checkVHostStateIsActive();
        if(childClass == Exchange.class)
        {
            return (ListenableFuture<C>) addExchangeAsync(attributes);

        }
        else if(childClass == Queue.class)
        {
            return (ListenableFuture<C>) addQueueAsync(attributes);

        }
        else if(childClass == VirtualHostAlias.class)
        {
            throw new UnsupportedOperationException();
        }
        else if(childClass == VirtualHostLogger.class)
        {
            return getObjectFactory().createAsync(childClass, attributes, this);
        }
        throw new IllegalArgumentException("Cannot create a child of class " + childClass.getSimpleName());
    }

    public Collection<String> getExchangeTypeNames()
    {
        return getObjectFactory().getSupportedTypes(Exchange.class);
    }


    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    public boolean authoriseCreateConnection(final AMQPConnection<?> connection)
    {
        getSecurityManager().authoriseCreateConnection(connection);
        for(ConnectionValidator validator : _connectionValidators)
        {
            if(!validator.validateConnectionCreation(connection, this))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Initialise a housekeeping task to iterate over queues cleaning expired messages with no consumers
     * and checking for idle or open transactions that have exceeded the permitted thresholds.
     *
     * @param period
     */
    private void initialiseHouseKeeping(long period)
    {
        if (period != 0L)
        {
            scheduleHouseKeepingTask(period, new VirtualHostHouseKeepingTask());
        }
    }

    protected void shutdownHouseKeeping()
    {
        if(_houseKeepingTaskExecutor != null)
        {
            _houseKeepingTaskExecutor.shutdown();

            try
            {
                if (!_houseKeepingTaskExecutor.awaitTermination(HOUSEKEEPING_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS))
                {
                    _houseKeepingTaskExecutor.shutdownNow();
                }
            }
            catch (InterruptedException e)
            {
                _logger.warn("Interrupted during Housekeeping shutdown:", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void removeHouseKeepingTasks()
    {
        BlockingQueue<Runnable> taskQueue = _houseKeepingTaskExecutor.getQueue();
        for (final Runnable runnable : taskQueue)
        {
            _houseKeepingTaskExecutor.remove(runnable);
        }
    }

    private void closeNetworkConnectionScheduler()
    {
        if(_networkConnectionScheduler != null)
        {
            _networkConnectionScheduler.close();
            _networkConnectionScheduler = null;
        }
    }

    /**
     * Allow other broker components to register a HouseKeepingTask
     *
     * @param period How often this task should run, in ms.
     * @param task The task to run.
     */
    public void scheduleHouseKeepingTask(long period, HouseKeepingTask task)
    {
        _houseKeepingTaskExecutor.scheduleAtFixedRate(task, period / 2, period, TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> scheduleTask(long delay, Runnable task)
    {
        return _houseKeepingTaskExecutor.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void executeTask(final String name, final Runnable task, AccessControlContext context)
    {
        _houseKeepingTaskExecutor.execute(new HouseKeepingTask(name, this, context)
        {
            @Override
            public void execute()
            {
                task.run();
            }
        });
    }

    public long getHouseKeepingTaskCount()
    {
        return _houseKeepingTaskExecutor.getTaskCount();
    }

    public long getHouseKeepingCompletedTaskCount()
    {
        return _houseKeepingTaskExecutor.getCompletedTaskCount();
    }

    public int getHouseKeepingPoolSize()
    {
        return _houseKeepingTaskExecutor.getCorePoolSize();
    }

    public void setHouseKeepingPoolSize(int newSize)
    {
        _houseKeepingTaskExecutor.setCorePoolSize(newSize);
    }


    public int getHouseKeepingActiveCount()
    {
        return _houseKeepingTaskExecutor.getActiveCount();
    }

    @Override
    public List<String> getEnabledConnectionValidators()
    {
        return _enabledConnectionValidators;
    }

    @Override
    public List<String> getDisabledConnectionValidators()
    {
        return _disabledConnectionValidators;
    }

    @Override
    public List<String> getGlobalAddressDomains()
    {
        return _globalAddressDomains;
    }

    @Override
    public AMQQueue<?> getAttainedQueue(String name)
    {
        Queue child = awaitChildClassToAttainState(Queue.class, name);
        if(child == null && getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                if(name.startsWith(domain + "/"))
                {
                    child = awaitChildClassToAttainState(Queue.class, name.substring(domain.length()));
                    if(child != null)
                    {
                        break;
                    }
                }
            }
        }
        return (AMQQueue<?>) child;
    }

    @Override
    public MessageSource getAttainedMessageSource(final String name)
    {
        MessageSource systemSource = _systemNodeSources.get(name);
        return systemSource == null ? (MessageSource) awaitChildClassToAttainState(Queue.class, name) : systemSource;
    }

    @Override
    public AMQQueue<?> getAttainedQueue(UUID id)
    {
        return (AMQQueue<?>) awaitChildClassToAttainState(Queue.class, id);
    }

    @Override
    public Broker<?> getBroker()
    {
        return _broker;
    }

    @Override
    public Collection<AMQQueue<?>> getQueues()
    {
        Collection children = getChildren(Queue.class);
        return children;
    }

    @Override
    public int removeQueue(AMQQueue<?> queue)
    {
        return doSync(removeQueueAsync(queue));
    }

    @Override
    public ListenableFuture<Integer> removeQueueAsync(final AMQQueue<?> queue)
    {
        return queue.deleteAndReturnCount();
    }

    public AMQQueue<?> createQueue(Map<String, Object> attributes) throws QueueExistsException
    {
        return (AMQQueue<?> )createChild(Queue.class, attributes);
    }

    private ListenableFuture<? extends AMQQueue<?>> addQueueAsync(Map<String, Object> attributes) throws QueueExistsException
    {
        if (shouldCreateDLQ(attributes))
        {
            // TODO - this isn't really correct - what if the name has ${foo} in it?
            String queueName = String.valueOf(attributes.get(Queue.NAME));
            validateDLNames(queueName);
            String altExchangeName = createDLQ(queueName);
            attributes = new LinkedHashMap<String, Object>(attributes);
            attributes.put(Queue.ALTERNATE_EXCHANGE, altExchangeName);
        }
        return Futures.immediateFuture(addQueueWithoutDLQ(attributes));
    }

    private AMQQueue<?> addQueueWithoutDLQ(Map<String, Object> attributes) throws QueueExistsException
    {
        try
        {
            return (AMQQueue) getObjectFactory().create(Queue.class, attributes, this);
        }
        catch (DuplicateNameException e)
        {
            throw new QueueExistsException(String.format("Queue with name '%s' already exists", e.getName()),
                                           (AMQQueue) e.getExisting());
        }

    }


    @Override
    public MessageDestination getAttainedMessageDestination(final String name)
    {
        MessageDestination destination = _systemNodeDestinations.get(name);
        return destination == null ? getAttainedExchange(name) : destination;
    }

    @Override
    public ExchangeImpl getAttainedExchange(String name)
    {
        Exchange child = awaitChildClassToAttainState(Exchange.class, name);
        if(child == null && getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                if(name.startsWith(domain + "/"))
                {
                    child = awaitChildClassToAttainState(Exchange.class, name.substring(domain.length()));
                    if(child != null)
                    {
                        break;
                    }
                }
            }
        }
        return (ExchangeImpl) child;
    }

    private <C extends ConfiguredObject> C awaitChildClassToAttainState(final Class<C> childClass, final String name)
    {
        ListenableFuture<C> attainedChildByName = getAttainedChildByName(childClass, name);
        try
        {
            return (C) doSync(attainedChildByName, DEFAULT_AWAIT_ATTAINMENT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            _logger.warn("Gave up waiting for {} '{}' to attain state. Check object's state via Management.", childClass.getSimpleName(), name);
            return null;
        }
    }

    private <C extends ConfiguredObject> C awaitChildClassToAttainState(final Class<C> childClass, final UUID id)
    {
        ListenableFuture<C> attainedChildByName = getAttainedChildById(childClass, id);
        try
        {
            return (C) doSync(attainedChildByName, DEFAULT_AWAIT_ATTAINMENT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            _logger.warn("Gave up waiting for {} with ID {} to attain state. Check object's state via Management.", childClass.getSimpleName(), id);
            return null;
        }
    }

    @Override
    public MessageDestination getDefaultDestination()
    {
        return _defaultDestination;
    }

    @Override
    public Collection<ExchangeImpl<?>> getExchanges()
    {
        Collection children = getChildren(Exchange.class);
        return children;
    }


    @Override
    public ExchangeImpl createExchange(Map<String,Object> attributes)
            throws ExchangeExistsException, ReservedExchangeNameException,
                   NoFactoryForTypeException
    {
        return (ExchangeImpl)createChild(Exchange.class, attributes);
    }


    private ListenableFuture<ExchangeImpl> addExchangeAsync(Map<String,Object> attributes)
            throws ExchangeExistsException, ReservedExchangeNameException,
                   NoFactoryForTypeException
    {
        final SettableFuture<ExchangeImpl> returnVal = SettableFuture.create();
        Futures.addCallback(getObjectFactory().createAsync(Exchange.class, attributes, this),
                            new FutureCallback<Exchange>()
                            {
                                @Override
                                public void onSuccess(final Exchange result)
                                {
                                    returnVal.set((ExchangeImpl) result);
                                }

                                @Override
                                public void onFailure(final Throwable t)
                                {
                                    if(t instanceof DuplicateNameException)
                                    {
                                        DuplicateNameException dne = (DuplicateNameException) t;
                                        returnVal.setException(new ExchangeExistsException((ExchangeImpl) dne.getExisting()));
                                    }
                                    else
                                    {
                                        returnVal.setException(t);
                                    }
                                }
                            });
        return returnVal;

    }

    @Override
    public String getLocalAddress(final String routingAddress)
    {
        String localAddress = routingAddress;
        if(getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                if(localAddress.length() > routingAddress.length() - domain.length() && routingAddress.startsWith(domain + "/"))
                {
                    localAddress = routingAddress.substring(domain.length());
                }
            }
        }
        return localAddress;
    }

    public SecurityManager getSecurityManager()
    {
        return _broker.getSecurityManager();
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        setState(State.UNAVAILABLE);
        _virtualHostLoggersToClose = new ArrayList(getChildren(VirtualHostLogger.class));
        //Stop Connections
        return closeConnections();
    }

    @Override
    protected void onClose()
    {
        _dtxRegistry.close();
        shutdownHouseKeeping();
        closeMessageStore();
        closeNetworkConnectionScheduler();
        _eventLogger.message(VirtualHostMessages.CLOSED(getName()));

        stopLogging(_virtualHostLoggersToClose);
    }


    public ListenableFuture<Void> closeConnections()
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Closing connection registry :" + _connections.size() + " connections.");
        }
        _acceptsConnections.set(false);
        for(Connection conn : _connections)
        {
            conn.getUnderlyingConnection().stopConnection();
        }

        List<ListenableFuture<Void>> connectionCloseFutures = new ArrayList<>();
        while (!_connections.isEmpty())
        {
            Iterator<AMQPConnection<?>> itr = _connections.iterator();
            while(itr.hasNext())
            {
                Connection<?> connection = itr.next();
                try
                {
                    connectionCloseFutures.add(connection.closeAsync());
                }
                catch (Exception e)
                {
                    _logger.warn("Exception closing connection " + connection.getName() + " from " + connection.getRemoteAddress(), e);
                }
                finally
                {
                    itr.remove();
                }
            }
        }
        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(connectionCloseFutures);
        return Futures.transform(combinedFuture, new Function<List<Void>, Void>()
               {
                   @Override
                   public Void apply(List<Void> voids)
                   {
                       return null;
                   }
               });
    }

    private void closeMessageStore()
    {
        if (getMessageStore() != null)
        {
            try
            {
                if (_messageStoreRecoverer != null)
                {
                    _messageStoreRecoverer.cancel();
                }

                getMessageStore().closeMessageStore();

            }
            catch (StoreException e)
            {
                _logger.error("Failed to close message store", e);
            }

            if (!(_virtualHostNode.getConfigurationStore() instanceof MessageStoreProvider))
            {
                getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.CLOSED());
            }
        }
    }

    public void registerMessageDelivered(long messageSize)
    {
        _messagesDelivered.registerEvent(1L);
        _dataDelivered.registerEvent(messageSize);
        _broker.registerMessageDelivered(messageSize);
    }

    public void registerMessageReceived(long messageSize, long timestamp)
    {
        _messagesReceived.registerEvent(1L, timestamp);
        _dataReceived.registerEvent(messageSize, timestamp);
        _broker.registerMessageReceived(messageSize, timestamp);
    }

    public StatisticsCounter getMessageReceiptStatistics()
    {
        return _messagesReceived;
    }

    public StatisticsCounter getDataReceiptStatistics()
    {
        return _dataReceived;
    }

    public StatisticsCounter getMessageDeliveryStatistics()
    {
        return _messagesDelivered;
    }

    public StatisticsCounter getDataDeliveryStatistics()
    {
        return _dataDelivered;
    }

    public void resetStatistics()
    {
        _messagesDelivered.reset();
        _dataDelivered.reset();
        _messagesReceived.reset();
        _dataReceived.reset();

        for (Connection<?> connection : _connections)
        {
            connection.getUnderlyingConnection().resetStatistics();
        }
    }

    public synchronized LinkRegistry getLinkRegistry(String remoteContainerId)
    {
        LinkRegistry linkRegistry = _linkRegistry.get(remoteContainerId);
        if(linkRegistry == null)
        {
            linkRegistry = new LinkRegistry();
            _linkRegistry.put(remoteContainerId, linkRegistry);
        }
        return linkRegistry;
    }

    public DtxRegistry getDtxRegistry()
    {
        return _dtxRegistry;
    }

    private void block(BlockingType blockingType)
    {
        synchronized (_connections)
        {
            _blockingReasons.add(blockingType);
            if(_blocked.compareAndSet(false,true))
            {
                for(Connection<?> conn : _connections)
                {
                    conn.getUnderlyingConnection().block();
                }
            }
        }
    }


    private void unblock(BlockingType blockingType)
    {

        synchronized (_connections)
        {
            _blockingReasons.remove(blockingType);
            if(_blockingReasons.isEmpty() && _blocked.compareAndSet(true,false))
            {
                for(Connection<?> conn : _connections)
                {
                    conn.getUnderlyingConnection().unblock();
                }
            }
        }
    }

    public void event(final Event event)
    {
        switch(event)
        {
            case PERSISTENT_MESSAGE_SIZE_OVERFULL:
                block(BlockingType.STORE);
                _eventLogger.message(getMessageStoreLogSubject(), MessageStoreMessages.OVERFULL());
                break;
            case PERSISTENT_MESSAGE_SIZE_UNDERFULL:
                unblock(BlockingType.STORE);
                _eventLogger.message(getMessageStoreLogSubject(), MessageStoreMessages.UNDERFULL());
                break;
        }
    }

    protected void reportIfError(State state)
    {
        if (state == State.ERRORED)
        {
            _eventLogger.message(VirtualHostMessages.ERRORED(getName()));
        }
    }

    private static class IsStoreEmptyHandler implements ConfiguredObjectRecordHandler
    {
        private boolean _empty = true;

        @Override
        public void begin()
        {
        }

        @Override
        public boolean handle(final ConfiguredObjectRecord record)
        {
            // if there is a non vhost record then the store is not empty and we can stop looking at the records
            _empty = record.getType().equals(VirtualHost.class.getSimpleName());
            return _empty;
        }

        @Override
        public void end()
        {

        }

        public boolean isEmpty()
        {
            return _empty;
        }
    }

    @Override
    public String getRedirectHost(final AmqpPort<?> port)
    {
        return null;
    }

    private class TargetSizeAssigningListener implements ConfigurationChangeListener
    {
        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof Queue)
            {
                allocateTargetSizeToQueues();
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object,
                                 final ConfiguredObject<?> child)
        {
            if (child instanceof Queue)
            {
                allocateTargetSizeToQueues();
            }
        }

        @Override
        public void stateChanged(final ConfiguredObject<?> object,
                                 final State oldState,
                                 final State newState)
        {
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
        }
    }

    private class VirtualHostHouseKeepingTask extends HouseKeepingTask
    {
        public VirtualHostHouseKeepingTask()
        {
            super("Housekeeping["+AbstractVirtualHost.this.getName()+"]",AbstractVirtualHost.this,_housekeepingJobContext);
        }

        public void execute()
        {
            VirtualHostNode<?> virtualHostNode = getParent(VirtualHostNode.class);
            Broker<?> broker = virtualHostNode.getParent(Broker.class);
            broker.assignTargetSizes();

            for (AMQQueue<?> q : getQueues())
            {
                if (q.getState() == State.ACTIVE)
                {
                    _logger.debug("Checking message status for queue: {}", q.getName());
                    q.checkMessageStatus();
                }
            }
            for (Connection<?> connection : _connections)
            {
                _logger.debug("Checking for long running open transactions on connection {}", connection);
                for (AMQSessionModel<?> session : connection.getUnderlyingConnection().getSessionModels())
                {
                    _logger.debug("Checking for long running open transactions on session {}", session);
                    session.checkTransactionStatus(getStoreTransactionOpenTimeoutWarn(),
                                                   getStoreTransactionOpenTimeoutClose(),
                                                   getStoreTransactionIdleTimeoutWarn(),
                                                   getStoreTransactionIdleTimeoutClose());
                }
            }
        }
    }

    private class SystemNodeRegistry implements SystemNodeCreator.SystemNodeRegistry
    {
        @Override
        public void registerSystemNode(final MessageNode node)
        {
            if(node instanceof MessageDestination)
            {
                _systemNodeDestinations.put(node.getName(), (MessageDestination) node);
            }
            if(node instanceof MessageSource)
            {
                _systemNodeSources.put(node.getName(), (MessageSource)node);
            }
        }

        @Override
        public void removeSystemNode(final MessageNode node)
        {
            if(node instanceof MessageDestination)
            {
                _systemNodeDestinations.remove(node.getName());
            }
            if(node instanceof MessageSource)
            {
                _systemNodeSources.remove(node.getName());
            }
        }

        @Override
        public void removeSystemNode(final String name)
        {
            _systemNodeDestinations.remove(name);
            _systemNodeSources.remove(name);
        }

        @Override
        public VirtualHostImpl getVirtualHost()
        {
            return AbstractVirtualHost.this;
        }

        @Override
        public boolean hasSystemNode(final String name)
        {
            return _systemNodeSources.containsKey(name) || _systemNodeDestinations.containsKey(name);
        }

    }


    @Override
    public boolean getDefaultDeadLetterQueueEnabled()
    {
        return isQueue_deadLetterQueueEnabled();
    }


    public void executeTransaction(TransactionalOperation op)
    {
        final MessageStore store = getMessageStore();
        final LocalTransaction txn = new LocalTransaction(store);

        op.withinTransaction(new Transaction()
        {
            public void dequeue(final QueueEntry messageInstance)
            {
                final ServerTransaction.Action deleteAction = new ServerTransaction.Action()
                {
                    public void postCommit()
                    {
                        messageInstance.delete();
                    }

                    public void onRollback()
                    {
                    }
                };

                boolean acquired = messageInstance.acquireOrSteal(new Runnable()
                                                                    {
                                                                        @Override
                                                                        public void run()
                                                                        {
                                                                            ServerTransaction txn = new AutoCommitTransaction(store);
                                                                            txn.dequeue(messageInstance.getEnqueueRecord(), deleteAction);
                                                                        }
                                                                    });
                if(acquired)
                {
                    txn.dequeue(messageInstance.getEnqueueRecord(), deleteAction);
                }
            }

            public void copy(QueueEntry entry, Queue queue)
            {
                final ServerMessage message = entry.getMessage();
                final AMQQueue toQueue = (AMQQueue)queue;

                txn.enqueue(toQueue, message, new ServerTransaction.EnqueueAction()
                {
                    public void postCommit(MessageEnqueueRecord... records)
                    {
                        toQueue.enqueue(message, null, records[0]);
                    }

                    public void onRollback()
                    {
                    }
                });

            }

            public void move(final QueueEntry entry, Queue queue)
            {
                final ServerMessage message = entry.getMessage();
                final AMQQueue toQueue = (AMQQueue)queue;
                if(entry.acquire())
                {
                    txn.enqueue(toQueue, message,
                                new ServerTransaction.EnqueueAction()
                                {

                                    public void postCommit(MessageEnqueueRecord... records)
                                    {
                                        toQueue.enqueue(message, null, records[0]);
                                    }

                                    public void onRollback()
                                    {
                                        entry.release();
                                    }
                                });
                    txn.dequeue(entry.getEnqueueRecord(),
                                new ServerTransaction.Action()
                                {

                                    public void postCommit()
                                    {
                                        entry.delete();
                                    }

                                    public void onRollback()
                                    {

                                    }
                                });

                }
            }

        });
        txn.commit();
    }

    @Override
    public boolean isQueue_deadLetterQueueEnabled()
    {
        return _queue_deadLetterQueueEnabled;
    }

    @Override
    public long getHousekeepingCheckPeriod()
    {
        return _housekeepingCheckPeriod;
    }

    @Override
    public long getStoreTransactionIdleTimeoutClose()
    {
        return _storeTransactionIdleTimeoutClose;
    }

    @Override
    public long getStoreTransactionIdleTimeoutWarn()
    {
        return _storeTransactionIdleTimeoutWarn;
    }

    @Override
    public long getStoreTransactionOpenTimeoutClose()
    {
        return _storeTransactionOpenTimeoutClose;
    }

    @Override
    public long getStoreTransactionOpenTimeoutWarn()
    {
        return _storeTransactionOpenTimeoutWarn;
    }

    @Override
    public long getQueueCount()
    {
        return getQueues().size();
    }

    @Override
    public long getExchangeCount()
    {
        return getExchanges().size();
    }

    @Override
    public long getConnectionCount()
    {
        return _connections.size();
    }

    @Override
    public long getBytesIn()
    {
        return getDataReceiptStatistics().getTotal();
    }

    @Override
    public long getBytesOut()
    {
        return getDataDeliveryStatistics().getTotal();
    }

    @Override
    public long getMessagesIn()
    {
        return getMessageReceiptStatistics().getTotal();
    }

    @Override
    public long getMessagesOut()
    {
        return getMessageDeliveryStatistics().getTotal();
    }

    @Override
    public int getHousekeepingThreadCount()
    {
        return _housekeepingThreadCount;
    }

    @Override
    public int getConnectionThreadPoolSize()
    {
        return _connectionThreadPoolSize;
    }

    @Override
    public int getNumberOfSelectors()
    {
        return _numberOfSelectors;
    }

    @StateTransition( currentState = { State.UNINITIALIZED, State.ACTIVE, State.ERRORED }, desiredState = State.STOPPED )
    protected ListenableFuture<Void> doStop()
    {
        final List<VirtualHostLogger> loggers = new ArrayList<>(getChildren(VirtualHostLogger.class));
        return doAfter(closeConnections(), new Callable<ListenableFuture<Void>>()
                                            {
                                                @Override
                                                public ListenableFuture<Void> call() throws Exception
                                                {
                                                    return closeChildren();
                                                }
                                            }).then(new Runnable()
        {
            @Override
            public void run()
            {
                shutdownHouseKeeping();
                closeNetworkConnectionScheduler();
                closeMessageStore();
                setState(State.STOPPED);

                stopLogging(loggers);
            }
        });
    }

    private void stopLogging(Collection<VirtualHostLogger> loggers)
    {
        for (VirtualHostLogger logger : loggers)
        {
            logger.stopLogging();
        }
    }

    @StateTransition( currentState = { State.ACTIVE, State.ERRORED }, desiredState = State.DELETED )
    private ListenableFuture<Void> doDelete()
    {
        return doAfterAlways(closeAsync(),
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        MessageStore ms = getMessageStore();
                        if (ms != null)
                        {
                            try
                            {
                                ms.onDelete(AbstractVirtualHost.this);
                            }
                            catch (Exception e)
                            {
                                _logger.warn("Exception occurred on message store deletion", e);
                            }
                        }
                        deleted();
                        setState(State.DELETED);
                    }
                });
    }

    public Collection<VirtualHostAlias> getAliases()
    {
        return Collections.unmodifiableCollection(_aliases);
    }

    private String createDLQ(final String queueName)
    {
        final String dlExchangeName = getDeadLetterExchangeName(queueName);
        final String dlQueueName = getDeadLetterQueueName(queueName);

        ExchangeImpl dlExchange = null;
        final UUID dlExchangeId = UUID.randomUUID();

        try
        {
            Map<String,Object> attributes = new HashMap<String, Object>();

            attributes.put(org.apache.qpid.server.model.Exchange.ID, dlExchangeId);
            attributes.put(org.apache.qpid.server.model.Exchange.NAME, dlExchangeName);
            attributes.put(org.apache.qpid.server.model.Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
            attributes.put(org.apache.qpid.server.model.Exchange.DURABLE, true);
            attributes.put(org.apache.qpid.server.model.Exchange.LIFETIME_POLICY,
                           false ? LifetimePolicy.DELETE_ON_NO_LINKS : LifetimePolicy.PERMANENT);
            attributes.put(org.apache.qpid.server.model.Exchange.ALTERNATE_EXCHANGE, null);
            dlExchange = createExchange(attributes);
        }
        catch(ExchangeExistsException e)
        {
            // We're ok if the exchange already exists
            dlExchange = e.getExistingExchange();
        }
        catch (ReservedExchangeNameException | NoFactoryForTypeException | UnknownConfiguredObjectException e)
        {
            throw new ConnectionScopedRuntimeException("Attempt to create an alternate exchange for a queue failed",e);
        }

        AMQQueue dlQueue = null;

        {
            dlQueue = (AMQQueue) getChildByName(Queue.class, dlQueueName);

            if(dlQueue == null)
            {
                //set args to disable DLQ-ing/MDC from the DLQ itself, preventing loops etc
                final Map<String, Object> args = new HashMap<String, Object>();
                args.put(CREATE_DLQ_ON_CREATION, false);
                args.put(Queue.MAXIMUM_DELIVERY_ATTEMPTS, 0);

                try
                {


                    args.put(Queue.ID, UUID.randomUUID());
                    args.put(Queue.NAME, dlQueueName);
                    args.put(Queue.DURABLE, true);
                    dlQueue = addQueueWithoutDLQ(args);
                    childAdded(dlQueue);
                }
                catch (QueueExistsException e)
                {
                    // TODO - currently theoretically for two threads to be creating a queue at the same time.
                    // All model changing operations should be moved to the task executor of the virtual host
                }
            }
        }

        //ensure the queue is bound to the exchange
        if(!dlExchange.isBound(AbstractVirtualHost.DLQ_ROUTING_KEY, dlQueue))
        {
            //actual routing key used does not matter due to use of fanout exchange,
            //but we will make the key 'dlq' as it can be logged at creation.
            dlExchange.addBinding(AbstractVirtualHost.DLQ_ROUTING_KEY, dlQueue, null);
        }
        return dlExchangeName;
    }

    private static void validateDLNames(String name)
    {
        // check if DLQ name and DLQ exchange name do not exceed 255
        String exchangeName = getDeadLetterExchangeName(name);
        if (exchangeName.length() > MAX_LENGTH)
        {
            throw new IllegalArgumentException("DL exchange name '" + exchangeName
                                               + "' length exceeds limit of " + MAX_LENGTH + " characters for queue " + name);
        }
        String queueName = getDeadLetterQueueName(name);
        if (queueName.length() > MAX_LENGTH)
        {
            throw new IllegalArgumentException("DLQ queue name '" + queueName + "' length exceeds limit of "
                                               + MAX_LENGTH + " characters for queue " + name);
        }
    }

    private boolean shouldCreateDLQ(Map<String, Object> arguments)
    {

        boolean autoDelete = MapValueConverter.getEnumAttribute(LifetimePolicy.class,
                                                                Queue.LIFETIME_POLICY,
                                                                arguments,
                                                                LifetimePolicy.PERMANENT) != LifetimePolicy.PERMANENT;

        //feature is not to be enabled for temporary queues or when explicitly disabled by argument
        if (!(autoDelete || (arguments != null && arguments.containsKey(Queue.ALTERNATE_EXCHANGE))))
        {
            boolean dlqArgumentPresent = arguments != null
                                         && arguments.containsKey(CREATE_DLQ_ON_CREATION);
            if (dlqArgumentPresent)
            {
                boolean dlqEnabled = true;
                if (dlqArgumentPresent)
                {
                    Object argument = arguments.get(CREATE_DLQ_ON_CREATION);
                    dlqEnabled = (argument instanceof Boolean && ((Boolean)argument).booleanValue())
                                 || (argument instanceof String && Boolean.parseBoolean(argument.toString()));
                }
                return dlqEnabled;
            }
            return isQueue_deadLetterQueueEnabled();
        }
        return false;
    }

    private static String getDeadLetterQueueName(String name)
    {
        return name + System.getProperty(BrokerProperties.PROPERTY_DEAD_LETTER_QUEUE_SUFFIX, AbstractVirtualHost.DEFAULT_DLQ_NAME_SUFFIX);
    }

    private static String getDeadLetterExchangeName(String name)
    {
        return name + System.getProperty(BrokerProperties.PROPERTY_DEAD_LETTER_EXCHANGE_SUFFIX, VirtualHostImpl.DEFAULT_DLE_NAME_SUFFIX);
    }

    @Override
    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public DurableConfigurationStore getDurableConfigurationStore()
    {
        return _virtualHostNode.getConfigurationStore();
    }

    @Override
    public void setTargetSize(final long targetSize)
    {
        _targetSize.set(targetSize);
        allocateTargetSizeToQueues();
    }

    public long getTargetSize()
    {
        return _targetSize.get();
    }

    private void allocateTargetSizeToQueues()
    {
        long targetSize = _targetSize.get();
        Collection<AMQQueue<?>> queues = getQueues();
        long totalSize = calculateTotalEnqueuedSize(queues);
        _logger.debug("Allocating target size to queues, total target: {} ; total enqueued size {}", targetSize, totalSize);
        if(targetSize > 0l)
        {
            for (AMQQueue<?> q : queues)
            {
                long size = (long) ((((double) q.getPotentialMemoryFootprint() / (double) totalSize))
                                             * (double) targetSize);

                q.setTargetSize(size);
            }
        }
    }

    @Override
    public long getTotalQueueDepthBytes()
    {
        return calculateTotalEnqueuedSize(getQueues());
    }


    @Override
    public Principal getPrincipal()
    {
        return _principal;
    }

    @Override
    public void registerConnection(final AMQPConnection<?> connection)
    {
        doSync(registerConnectionAsync(connection));
    }

    public ListenableFuture<Void> registerConnectionAsync(final AMQPConnection<?> connection)
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                if (_acceptsConnections.get())
                {
                    _connections.add(connection);

                    if (_blocked.get())
                    {
                        connection.block();
                    }

                    connection.pushScheduler(_networkConnectionScheduler);
                    for (VirtualHostConnectionListener listener : _connectionAssociationListeners)
                    {
                        listener.connectionAssociated(connection);
                    }
                    return Futures.immediateFuture(null);
                }
                else
                {
                    final VirtualHostUnavailableException exception =
                            new VirtualHostUnavailableException(String.format(
                                    "VirtualHost '%s' not accepting connections",
                                    getName()));
                    return Futures.immediateFailedFuture(exception);
                }
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "register connection";
            }

            @Override
            public String getArguments()
            {
                return String.valueOf(connection);
            }
        });

    }

    @Override
    public void deregisterConnection(final AMQPConnection<?> connection)
    {
        doSync(deregisterConnectionAsync(connection));
    }

    public ListenableFuture<Void> deregisterConnectionAsync(final AMQPConnection<?> connection)
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                connection.popScheduler();
                if (_connections.remove(connection))
                {
                    for (VirtualHostConnectionListener listener : _connectionAssociationListeners)
                    {
                        listener.connectionRemoved(connection);
                    }
                }
                return Futures.immediateFuture(null);
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "deregister connection";
            }

            @Override
            public String getArguments()
            {
                return String.valueOf(connection);
            }
        });
    }


    private long calculateTotalEnqueuedSize(final Collection<AMQQueue<?>> queues)
    {
        long total = 0;
        for(AMQQueue<?> queue : queues)
        {
            total += queue.getPotentialMemoryFootprint();
        }
        return total;
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> onActivate()
    {
        final SuppressingInheritedAccessControlContextThreadFactory housekeepingThreadFactory =
                new SuppressingInheritedAccessControlContextThreadFactory("virtualhost-" + getName() + "-pool",
                                                                          SecurityManager.getSystemTaskSubject("Housekeeping", getPrincipal()));
        _houseKeepingTaskExecutor = new ScheduledThreadPoolExecutor(getHousekeepingThreadCount(), housekeepingThreadFactory){
            @Override
            protected void afterExecute(Runnable r, Throwable t)
            {
                super.afterExecute(r, t);
                if (t == null && r instanceof Future<?>)
                {
                    Future future = (Future<?>) r;
                    try
                    {
                        if (future.isDone())
                        {
                            Object result = future.get();
                        }
                    }
                    catch (CancellationException ce)
                    {
                        _logger.debug("Housekeeping task got cancelled");
                        // Ignore cancellation of task
                    }
                    catch (ExecutionException | UncheckedExecutionException ee)
                    {
                        t = ee.getCause();
                    }
                    catch (InterruptedException ie)
                    {
                        Thread.currentThread().interrupt(); // ignore/reset
                    }
                    catch (Throwable t1)
                    {
                        t = t1;
                    }
                }
                if (t != null)
                {
                    _logger.error("Houskeeping task threw an exception:", t);

                    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
                    if (uncaughtExceptionHandler != null)
                    {
                        uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t);
                    }
                    else
                    {
                        Runtime.getRuntime().halt(1);
                    }
                }
            }
        };

        long threadPoolKeepAliveTimeout = getContextValue(Long.class, CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT);

        final SuppressingInheritedAccessControlContextThreadFactory connectionThreadFactory =
                new SuppressingInheritedAccessControlContextThreadFactory("virtualhost-" + getName() + "-iopool",
                                                                          SecurityManager.getSystemTaskSubject("IO Pool", getPrincipal()));

        _networkConnectionScheduler = new NetworkConnectionScheduler("virtualhost-" + getName() + "-iopool",
                                                                     getNumberOfSelectors(),
                                                                     getConnectionThreadPoolSize(),
                                                                     threadPoolKeepAliveTimeout,
                                                                     connectionThreadFactory);
        _networkConnectionScheduler.start();
        MessageStore messageStore = getMessageStore();
        messageStore.openMessageStore(this);

        startFileSystemSpaceChecking();


        if (!(_virtualHostNode.getConfigurationStore() instanceof MessageStoreProvider))
        {
            getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.CREATED());
            getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.STORE_LOCATION(messageStore.getStoreLocation()));
        }

        messageStore.upgradeStoreStructure();

        getBroker().assignTargetSizes();

        if (isStoreEmpty())
        {
            return doAfter(createDefaultExchanges(), new Runnable()
            {
                @Override
                public void run()
                {
                    postCreateDefaultExchangeTasks();
                }
            });
        }
        else
        {
            postCreateDefaultExchangeTasks();
            return Futures.immediateFuture(null);
        }
    }

    private void postCreateDefaultExchangeTasks()
    {
        if(getContextValue(Boolean.class, USE_ASYNC_RECOVERY))
        {
            _messageStoreRecoverer = new AsynchronousMessageStoreRecoverer();
        }
        else
        {
           _messageStoreRecoverer = new SynchronousMessageStoreRecoverer();
        }

        // propagate any exception thrown during recovery into HouseKeepingTaskExecutor to handle them accordingly
        final ListenableFuture<Void> recoveryResult = _messageStoreRecoverer.recover(this);
        recoveryResult.addListener(new Runnable()
        {
            @Override
            public void run()
            {
                Futures.getUnchecked(recoveryResult);
            }
        }, _houseKeepingTaskExecutor);

        State finalState = State.ERRORED;
        try
        {
            initialiseHouseKeeping(getHousekeepingCheckPeriod());
            finalState = State.ACTIVE;
            _acceptsConnections.set(true);
        }
        finally
        {
            setState(finalState);
            reportIfError(getState());
        }
    }

    protected void startFileSystemSpaceChecking()
    {
        File storeLocationAsFile = _messageStore.getStoreLocationAsFile();
        if (storeLocationAsFile != null && _fileSystemMaxUsagePercent > 0)
        {
            _fileSystemSpaceChecker.setFileSystem(storeLocationAsFile);

            scheduleHouseKeepingTask(getHousekeepingCheckPeriod(), _fileSystemSpaceChecker);
        }
    }

    @Override
    public void addConnectionAssociationListener(VirtualHostConnectionListener listener)
    {
        _connectionAssociationListeners.add(listener);
    }

    @Override
    public void removeConnectionAssociationListener(VirtualHostConnectionListener listener)
    {
        _connectionAssociationListeners.remove(listener);
    }

    @StateTransition( currentState = { State.STOPPED, State.ERRORED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> onRestart()
    {
        resetStatistics();

        final List<ConfiguredObjectRecord> records = new ArrayList<>();

        // Transitioning to STOPPED will have closed all our children.  Now we are transition
        // back to ACTIVE, we need to recover and re-open them.

        getDurableConfigurationStore().visitConfiguredObjectRecords(new ConfiguredObjectRecordHandler()
        {
            @Override
            public void begin()
            {
            }

            @Override
            public boolean handle(final ConfiguredObjectRecord record)
            {
                records.add(record);
                return true;
            }

            @Override
            public void end()
            {
            }
        });

        new GenericRecoverer(this).recover(records);

        final List<ListenableFuture<Void>> childOpenFutures = new ArrayList<>();

        Subject.doAs(SecurityManager.getSubjectWithAddedSystemRights(), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                applyToChildren(new Action<ConfiguredObject<?>>()
                {
                    @Override
                    public void performAction(final ConfiguredObject<?> child)
                    {
                        final ListenableFuture<Void> childOpenFuture = child.openAsync();
                        childOpenFutures.add(childOpenFuture);

                        Futures.addCallback(childOpenFuture, new FutureCallback<Void>()
                        {
                            @Override
                            public void onSuccess(final Void result)
                            {
                            }

                            @Override
                            public void onFailure(final Throwable t)
                            {
                                _logger.error("Exception occurred while opening {} : {}",
                                              child.getClass().getSimpleName(), child.getName(), t);
                            }

                        });
                    }
                });
                return null;
            }
        });

        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(childOpenFutures);
        return Futures.transform(combinedFuture, new AsyncFunction<List<Void>, Void>()
        {
            @Override
            public ListenableFuture<Void> apply(final List<Void> input) throws Exception
            {
                return onActivate();
            }
        });
    }

    private class FileSystemSpaceChecker extends HouseKeepingTask
    {
        private boolean _fileSystemFull;
        private File _fileSystem;

        public FileSystemSpaceChecker()
        {
            super("FileSystemSpaceChecker["+AbstractVirtualHost.this.getName()+"]",AbstractVirtualHost.this,_fileSystemSpaceCheckerJobContext);
        }

        @Override
        public void execute()
        {
            long totalSpace = _fileSystem.getTotalSpace();
            long freeSpace = _fileSystem.getFreeSpace();

            if (totalSpace == 0)
            {
                _logger.warn("Cannot check file system for disk space because store path '{}' is not valid", _fileSystem.getPath());
                return;
            }

            long usagePercent = (100l * (totalSpace - freeSpace)) / totalSpace;

            if (_fileSystemFull && (usagePercent < _fileSystemMaxUsagePercent))
            {
                _fileSystemFull = false;
                getEventLogger().message(getMessageStoreLogSubject(), VirtualHostMessages.FILESYSTEM_NOTFULL(
                        _fileSystemMaxUsagePercent));
                unblock(BlockingType.FILESYSTEM);
            }
            else if(!_fileSystemFull && usagePercent > _fileSystemMaxUsagePercent)
            {
                _fileSystemFull = true;
                getEventLogger().message(getMessageStoreLogSubject(), VirtualHostMessages.FILESYSTEM_FULL(
                        _fileSystemMaxUsagePercent));
                block(BlockingType.FILESYSTEM);
            }

        }

        public void setFileSystem(final File fileSystem)
        {
            _fileSystem = fileSystem;
        }
    }

}
