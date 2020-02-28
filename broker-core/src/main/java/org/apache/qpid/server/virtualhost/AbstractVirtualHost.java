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

import static com.google.common.collect.Iterators.cycle;
import static java.util.Collections.newSetFromMap;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.security.auth.Subject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.exchange.DefaultDestination;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.MessageStoreMessages;
import org.apache.qpid.server.logging.messages.VirtualHostMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageNode;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.plugin.ConnectionValidator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemNodeCreator;
import org.apache.qpid.server.pool.SuppressingInheritedAccessControlContextThreadFactory;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryIterator;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.CompoundAccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SubjectFixedResultAccessControl;
import org.apache.qpid.server.security.SubjectFixedResultAccessControl.ResultCalculator;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;
import org.apache.qpid.server.stats.StatisticsReportingTask;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreProvider;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.VirtualHostStoreUpgraderAndRecoverer;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdaterImpl;
import org.apache.qpid.server.store.preferences.PreferencesRecoverer;
import org.apache.qpid.server.store.preferences.PreferencesRoot;
import org.apache.qpid.server.store.serializer.MessageStoreSerializer;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.NetworkConnectionScheduler;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.HousekeepingExecutor;
import org.apache.qpid.server.util.Strings;
import org.apache.qpid.server.virtualhost.connection.ConnectionPrincipalStatisticsRegistryImpl;

public abstract class AbstractVirtualHost<X extends AbstractVirtualHost<X>> extends AbstractConfiguredObject<X>
        implements QueueManagingVirtualHost<X>
{
    private final Collection<ConnectionValidator> _connectionValidators = new ArrayList<>();


    private final Set<AMQPConnection<?>> _connections = newSetFromMap(new ConcurrentHashMap<AMQPConnection<?>, Boolean>());
    private final AccessControlContext _housekeepingJobContext;
    private final AccessControlContext _fileSystemSpaceCheckerJobContext;
    private final AtomicBoolean _acceptsConnections = new AtomicBoolean(false);
    private volatile TaskExecutor _preferenceTaskExecutor;
    private volatile boolean _deleteRequested;
    private final ConcurrentMap<String, Cache> _caches = new ConcurrentHashMap<>();

    private enum BlockingType { STORE, FILESYSTEM };

    private static final String USE_ASYNC_RECOVERY = "use_async_message_store_recovery";


    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractVirtualHost.class);

    private static final int HOUSEKEEPING_SHUTDOWN_TIMEOUT = 5;

    private volatile ScheduledThreadPoolExecutor _houseKeepingTaskExecutor;
    private volatile ScheduledFuture<?> _statisticsReportingFuture;

    private final Broker<?> _broker;

    private final DtxRegistry _dtxRegistry;

    private final SystemNodeRegistry _systemNodeRegistry = new SystemNodeRegistry();

    private final AtomicLong _messagesIn = new AtomicLong();
    private final AtomicLong _messagesOut = new AtomicLong();
    private final AtomicLong _transactedMessagesIn = new AtomicLong();
    private final AtomicLong _transactedMessagesOut = new AtomicLong();
    private final AtomicLong _bytesIn = new AtomicLong();
    private final AtomicLong _bytesOut = new AtomicLong();
    private final AtomicLong _totalConnectionCount = new AtomicLong();
    private final AtomicLong _maximumMessageSize = new AtomicLong();

    private volatile LinkRegistryModel _linkRegistry;
    private AtomicBoolean _blocked = new AtomicBoolean();

    private final Map<String, MessageDestination> _systemNodeDestinations =
            Collections.synchronizedMap(new HashMap<String,MessageDestination>());

    private final Map<String, MessageSource> _systemNodeSources =
            Collections.synchronizedMap(new HashMap<String,MessageSource>());

    private final EventLogger _eventLogger;

    private final VirtualHostNode<?> _virtualHostNode;

    private final AtomicLong _targetSize = new AtomicLong(100 * 1024 * 1024);

    private MessageStoreLogSubject _messageStoreLogSubject;

    private final Set<BlockingType> _blockingReasons = Collections.synchronizedSet(EnumSet.noneOf(BlockingType.class));

    private NetworkConnectionScheduler _networkConnectionScheduler;

    private final VirtualHostPrincipal _principal;

    private ConfigurationChangeListener _accessControlProviderListener = new AccessControlProviderListener();

    private final AccessControl _accessControl;

    private volatile boolean _createDefaultExchanges;

    private final AccessControl _systemUserAllowed = new SubjectFixedResultAccessControl(new ResultCalculator()
    {
        @Override
        public Result getResult(final Subject subject)
        {
            return isSystemSubject(subject) ? Result.ALLOWED : Result.DEFER;
        }
    }, Result.DEFER);


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

    @ManagedAttributeField
    private List<NodeAutoCreationPolicy> _nodeAutoCreationPolicies;

    @ManagedAttributeField
    private volatile int _statisticsReportingPeriod;
    
    private boolean _useAsyncRecoverer;

    private MessageDestination _defaultDestination;

    private MessageStore _messageStore;
    private MessageStoreRecoverer _messageStoreRecoverer;
    private final FileSystemSpaceChecker _fileSystemSpaceChecker;
    private int _fileSystemMaxUsagePercent;
    private Collection<VirtualHostLogger> _virtualHostLoggersToClose;
    private PreferenceStore _preferenceStore;
    private long _flowToDiskCheckPeriod;
    private volatile boolean _isDiscardGlobalSharedSubscriptionLinksOnDetach;
    private volatile ConnectionPrincipalStatisticsRegistry _connectionPrincipalStatisticsRegistry;
    private volatile HouseKeepingTask _statisticsCheckTask;

    public AbstractVirtualHost(final Map<String, Object> attributes, VirtualHostNode<?> virtualHostNode)
    {
        super(virtualHostNode, attributes);
        _broker = (Broker<?>) virtualHostNode.getParent();
        _virtualHostNode = virtualHostNode;

        _dtxRegistry = new DtxRegistry(this);

        final SystemConfig systemConfig = (SystemConfig) _broker.getParent();
        _eventLogger = systemConfig.getEventLogger();

        _eventLogger.message(VirtualHostMessages.CREATED(getName()));

        _principal = new VirtualHostPrincipal(this);

        if (systemConfig.isManagementMode())
        {
            _accessControl = AccessControl.ALWAYS_ALLOWED;
        }
        else
        {
            _accessControl =  new CompoundAccessControl(
                    Collections.<AccessControl<?>>emptyList(), Result.DEFER
            );
        }

        _defaultDestination = new DefaultDestination(this, _accessControl);


        _housekeepingJobContext = getSystemTaskControllerContext("Housekeeping["+getName()+"]", _principal);
        _fileSystemSpaceCheckerJobContext = getSystemTaskControllerContext("FileSystemSpaceChecker["+getName()+"]", _principal);

        _fileSystemSpaceChecker = new FileSystemSpaceChecker();
    }

    private void updateAccessControl()
    {
        if(!((SystemConfig)_broker.getParent()).isManagementMode())
        {
            List<VirtualHostAccessControlProvider> children = new ArrayList<>(getChildren(VirtualHostAccessControlProvider.class));
            LOGGER.debug("Updating access control list with {} provider children", children.size());
            Collections.sort(children, VirtualHostAccessControlProvider.ACCESS_CONTROL_PROVIDER_COMPARATOR);

            List<AccessControl<?>> accessControls = new ArrayList<>(children.size()+2);
            accessControls.add(_systemUserAllowed);
            for(VirtualHostAccessControlProvider prov : children)
            {
                if(prov.getState() == State.ERRORED)
                {
                    accessControls.clear();
                    accessControls.add(AccessControl.ALWAYS_DENIED);
                    break;
                }
                else if(prov.getState() == State.ACTIVE)
                {
                    accessControls.add(prov.getController());
                }

            }
            accessControls.add(getParentAccessControl());
            ((CompoundAccessControl)_accessControl).setAccessControls(accessControls);

        }
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
        _createDefaultExchanges = true;
    }

    @Override
    public void setFirstOpening(boolean firstOpening)
    {
        _createDefaultExchanges = firstOpening;
    }

    @Override
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
        if(getNodeAutoCreationPolicies() != null)
        {
            for(NodeAutoCreationPolicy policy : getNodeAutoCreationPolicies())
            {
                validateNodeAutoCreationPolicy(policy);
            }
        }

        validateConnectionThreadPoolSettings(this);
        validateMessageStoreCreation();
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        QueueManagingVirtualHost<?> virtualHost = (QueueManagingVirtualHost<?>) proxyForValidation;

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
        if(changedAttributes.contains(NODE_AUTO_CREATION_POLICIES))
        {
            if(getNodeAutoCreationPolicies() != null)
            {
                for(NodeAutoCreationPolicy policy : virtualHost.getNodeAutoCreationPolicies())
                {
                    validateNodeAutoCreationPolicy(policy);
                }
            }

        }

        if (changedAttributes.contains(CONNECTION_THREAD_POOL_SIZE) || changedAttributes.contains(NUMBER_OF_SELECTORS))
        {
            validateConnectionThreadPoolSettings(virtualHost);
        }
    }

    @Override
    protected void changeAttributes(final Map<String, Object> attributes)
    {
        super.changeAttributes(attributes);
        if (attributes.containsKey(STATISTICS_REPORTING_PERIOD))
        {
            initialiseStatisticsReporting();
        }
    }

    @Override
    protected AccessControl getAccessControl()
    {
        return _accessControl;
    }

    private AccessControl getParentAccessControl()
    {
        return super.getAccessControl();
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();
        addChangeListener(_accessControlProviderListener);
        Collection<VirtualHostAccessControlProvider> accessControlProviders = getChildren(VirtualHostAccessControlProvider.class);
        if (!accessControlProviders.isEmpty())
        {
            accessControlProviders.forEach(child -> child.addChangeListener(_accessControlProviderListener));
        }
    }

    private void validateNodeAutoCreationPolicy(final NodeAutoCreationPolicy policy)
    {
        String pattern = policy.getPattern();
        if(pattern == null)
        {
            throw new IllegalArgumentException("The 'pattern' attribute of a NodeAutoCreationPolicy MUST be supplied: " + policy);
        }

        try
        {
            Pattern.compile(pattern);
        }
        catch (PatternSyntaxException e)
        {
            throw new IllegalArgumentException("The 'pattern' attribute of a NodeAutoCreationPolicy MUST be a valid "
                                               + "Java Regular Expression Pattern, the value '" + pattern + "' is not: " + policy);

        }

        String nodeType = policy.getNodeType();
        Class<? extends ConfiguredObject> sourceClass = null;
        for (Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(getCategoryClass()))
        {
            if (childClass.getSimpleName().equalsIgnoreCase(nodeType.trim()))
            {
                sourceClass = childClass;
                break;
            }
        }
        if(sourceClass == null)
        {
            throw new IllegalArgumentException("The node type of a NodeAutoCreationPolicy must be a valid child type "
                                               + "of a VirtualHost, '" + nodeType + "' is not.");
        }
        if(policy.isCreatedOnConsume() && !MessageSource.class.isAssignableFrom(sourceClass))
        {
            throw new IllegalArgumentException("A NodeAutoCreationPolicy which creates nodes on consume must have a "
                                               + "nodeType which implements MessageSource, '" + nodeType + "' does not.");
        }

        if(policy.isCreatedOnPublish() && !MessageDestination.class.isAssignableFrom(sourceClass))
        {
            throw new IllegalArgumentException("A NodeAutoCreationPolicy which creates nodes on publish must have a "
                                               + "nodeType which implements MessageDestination, '" + nodeType + "' does not.");
        }
        if(!(policy.isCreatedOnConsume() || policy.isCreatedOnPublish()))
        {
            throw new IllegalArgumentException("A NodeAutoCreationPolicy must create on consume, create on publish or both.");
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

    private void validateConnectionThreadPoolSettings(QueueManagingVirtualHost<?> virtualHost)
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
            throw new IllegalConfigurationException(String.format("Number of Selectors %d on VirtualHost %s must be less than the connection pool size %d.", virtualHost.getNumberOfSelectors(), getName(), virtualHost.getConnectionThreadPoolSize()));
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
                    LOGGER.warn("Failed to close database", e);
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
        stopPreferenceTaskExecutor();
        closePreferenceStore();
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
        _flowToDiskCheckPeriod = getContextValue(Long.class, FLOW_TO_DISK_CHECK_PERIOD);
        _isDiscardGlobalSharedSubscriptionLinksOnDetach = getContextValue(Boolean.class, DISCARD_GLOBAL_SHARED_SUBSCRIPTION_LINKS_ON_DETACH);

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

        PreferencesRoot preferencesRoot = (VirtualHostNode) getParent();
        _preferenceStore = preferencesRoot.createPreferenceStore();

        _linkRegistry = createLinkRegistry();

        createHousekeepingExecutor();
    }

    LinkRegistryModel createLinkRegistry()
    {
        LinkRegistryModel linkRegistry;
        Iterator<LinkRegistryFactory>
                linkRegistryFactories = (new QpidServiceLoader()).instancesOf(LinkRegistryFactory.class).iterator();
        if (linkRegistryFactories.hasNext())
        {
            final LinkRegistryFactory linkRegistryFactory = linkRegistryFactories.next();
            if (linkRegistryFactories.hasNext())
            {
                throw new RuntimeException("Found multiple implementations of LinkRegistry");
            }
            linkRegistry = linkRegistryFactory.create(this);
        }
        else
        {
            linkRegistry = null;
        }
        return linkRegistry;
    }

    private void createHousekeepingExecutor()
    {
        if(_houseKeepingTaskExecutor == null || _houseKeepingTaskExecutor.isTerminated())
        {
            _houseKeepingTaskExecutor = new HousekeepingExecutor("virtualhost-" + getName() + "-pool",
                                                                 getHousekeepingThreadCount(),
                                                                 getSystemTaskSubject("Housekeeping", getPrincipal()));
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

    @Override
    public boolean isActive()
    {
        return getState() == State.ACTIVE;
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

    private ListenableFuture<List<Void>> createDefaultExchanges()
    {
        return Subject.doAs(getSubjectWithAddedSystemRights(), new PrivilegedAction<ListenableFuture<List<Void>>>()
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
                final ListenableFuture<Exchange<?>> future = addExchangeAsync(attributes);
                final SettableFuture<Void> returnVal = SettableFuture.create();
                addFutureCallback(future, new FutureCallback<Exchange<?>>()
                {
                    @Override
                    public void onSuccess(final Exchange<?> result)
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
    public Collection<? extends Connection<?>> getConnections()
    {
        return _connections;
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
    public int publishMessage(@Param(name = "message") final ManageableMessage message)
    {
        final String address = message.getAddress();
        MessageDestination destination = address == null ? getDefaultDestination() : getAttainedMessageDestination(address, true);
        if(destination == null)
        {
            destination = getDefaultDestination();
        }

        final AMQMessageHeader header = new MessageHeaderImpl(message);

        Serializable body = null;
        Object messageContent = message.getContent();
        if(messageContent != null)
        {
            if(messageContent instanceof Map || messageContent instanceof List)
            {
                if(message.getMimeType() != null || message.getEncoding() != null)
                {
                    throw new IllegalArgumentException("If the message content is provided as map or list, the mime type and encoding must be left unset");
                }
                body = (Serializable)messageContent;
            }
            else if(messageContent instanceof String)
            {
                String contentTransferEncoding = message.getContentTransferEncoding();
                if("base64".equalsIgnoreCase(contentTransferEncoding))
                {
                    body = Strings.decodeBase64((String) messageContent);
                }
                else if(contentTransferEncoding == null || contentTransferEncoding.trim().equals("") || contentTransferEncoding.trim().equalsIgnoreCase("identity"))
                {
                    String mimeType = message.getMimeType();
                    if(mimeType != null && !(mimeType = mimeType.trim().toLowerCase()).equals(""))
                    {
                        if (!(mimeType.startsWith("text/") || Arrays.asList("application/json", "application/xml")
                                                                    .contains(mimeType)))
                        {
                            throw new IllegalArgumentException(message.getMimeType()
                                                               + " is invalid as a MIME type for this message. "
                                                               + "Only MIME types of the text type can be used if a string is supplied as the content");
                        }
                        else if (mimeType.matches(".*;\\s*charset\\s*=.*"))
                        {
                            throw new IllegalArgumentException(message.getMimeType()
                                                               + " is invalid as a MIME type for this message. "
                                                               + "If a string is supplied as the content, the MIME type must not include a charset parameter");
                        }
                    }
                    body = (String) messageContent;
                }
                else
                {
                    throw new IllegalArgumentException("contentTransferEncoding value '" + contentTransferEncoding + "' is invalid.  The only valid values are base64 and identity");
                }
            }
            else
            {
                throw new IllegalArgumentException("The message content (if present) can only be a string, map or list");
            }
        }

        InternalMessage internalMessage = InternalMessage.createMessage(getMessageStore(), header, body, message.isPersistent(), address);
        AutoCommitTransaction txn = new AutoCommitTransaction(getMessageStore());
        final InstanceProperties instanceProperties =
                new InstanceProperties()
                {
                    @Override
                    public Object getProperty(final Property prop)
                    {
                        switch (prop)
                        {
                            case EXPIRATION:
                                Date expiration = message.getExpiration();
                                return expiration == null ? 0 : expiration.getTime();
                            case IMMEDIATE:
                                return false;
                            case PERSISTENT:
                                return message.isPersistent();
                            case MANDATORY:
                                return false;
                            case REDELIVERED:
                                return false;
                            default:
                                return null;
                        }
                    }
                };
        final RoutingResult<InternalMessage> result =
                destination.route(internalMessage, address, instanceProperties);
        return result.send(txn, null);

    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                             Map<String, Object> attributes)
    {
        checkVHostStateIsActive();
        return super.addChildAsync(childClass, attributes);
    }


    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    public Map<String, Object> extractConfig(final boolean includeSecureAttributes)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<Map<String,Object>>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Map<String, Object>> execute() throws RuntimeException
            {
                ConfigurationExtractor configExtractor = new ConfigurationExtractor();
                Map<String, Object> config = configExtractor.extractConfig(AbstractVirtualHost.this,
                                                                           includeSecureAttributes);
                return Futures.immediateFuture(config);
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "extractConfig";
            }

            @Override
            public String getArguments()
            {
                return "includeSecureAttributes=" + String.valueOf(includeSecureAttributes);
            }
        }));
    }

    @Override
    public Content exportMessageStore()
    {
        return new MessageStoreContent();
    }

    private class MessageStoreContent implements Content, CustomRestHeaders
    {

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            doSync(doOnConfigThread(new Task<ListenableFuture<Void>, IOException>()
            {
                @Override
                public ListenableFuture<Void> execute() throws IOException
                {
                    if (getState() != State.STOPPED)
                    {
                        throw new IllegalArgumentException(
                                "The exportMessageStore operation can only be called when the virtual host is stopped");
                    }

                    _messageStore.openMessageStore(AbstractVirtualHost.this);
                    try
                    {
                        final Map<UUID, String> queueMap = new HashMap<>();
                        getDurableConfigurationStore().reload(new ConfiguredObjectRecordHandler()
                        {
                            @Override
                            public void handle(final ConfiguredObjectRecord record)
                            {
                                if(record.getType().equals(Queue.class.getSimpleName()))
                                {
                                    queueMap.put(record.getId(), (String) record.getAttributes().get(ConfiguredObject.NAME));
                                }
                            }
                        });
                        MessageStoreSerializer serializer = new QpidServiceLoader().getInstancesByType(MessageStoreSerializer.class).get(MessageStoreSerializer.LATEST);
                        MessageStore.MessageStoreReader reader = _messageStore.newMessageStoreReader();
                        serializer.serialize(queueMap, reader, outputStream);
                    }
                    finally
                    {
                        _messageStore.closeMessageStore();
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
                    return "exportMessageStore";
                }

                @Override
                public String getArguments()
                {
                    return null;
                }
            }));
        }

        @Override
        public void release()
        {
        }

        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "application/octet-stream";
        }

        @SuppressWarnings("unused")
        @RestContentHeader("Content-Disposition")
        public String getContentDisposition()
        {
            try
            {

                String vhostName = getName();
                // replace all non-ascii and non-printable characters and all backslashes and percent encoded characters
                // as suggested by rfc6266 Appendix D
                String asciiName = vhostName.replaceAll("[^\\x20-\\x7E]", "?")
                                                 .replace('\\', '?')
                                                 .replaceAll("%[0-9a-fA-F]{2}", "?");
                String disposition = String.format("attachment; filename=\"%s_messages.bin\"; filename*=\"UTF-8''%s_messages.bin\"",
                                                   asciiName,
                                                   URLEncoder.encode(vhostName, StandardCharsets.UTF_8.name())
                                                   );
                return disposition;
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("JVM does not support UTF8", e);
            }
        }

    }

    @Override
    public void importMessageStore(final String source)
    {
        try
        {
            final URL url = convertStringToURL(source);

            try (InputStream input = url.openStream();
                 BufferedInputStream bufferedInputStream = new BufferedInputStream(input);
                 DataInputStream data = new DataInputStream(bufferedInputStream))
            {

                final MessageStoreSerializer serializer = MessageStoreSerializer.FACTORY.newInstance(data);

                doSync(doOnConfigThread(new Task<ListenableFuture<Void>, IOException>()
                {
                    @Override
                    public ListenableFuture<Void> execute() throws IOException
                    {
                        if (getState() != State.STOPPED)
                        {
                            throw new IllegalArgumentException(
                                    "The importMessageStore operation can only be called when the virtual host is stopped");
                        }

                        try
                        {
                            _messageStore.openMessageStore(AbstractVirtualHost.this);
                            checkMessageStoreEmpty();
                            final Map<String, UUID> queueMap = new HashMap<>();
                            getDurableConfigurationStore().reload(new ConfiguredObjectRecordHandler()
                            {
                                @Override
                                public void handle(final ConfiguredObjectRecord record)
                                {
                                    if (record.getType().equals(Queue.class.getSimpleName()))
                                    {
                                        queueMap.put((String) record.getAttributes().get(ConfiguredObject.NAME),
                                                     record.getId());
                                    }
                                }
                            });

                            serializer.deserialize(queueMap, _messageStore, data);
                        }
                        finally
                        {
                            _messageStore.closeMessageStore();
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
                        return "importMessageStore";
                    }

                    @Override
                    public String getArguments()
                    {
                        if (url.getProtocol().equalsIgnoreCase("http") || url.getProtocol().equalsIgnoreCase("https") || url.getProtocol().equalsIgnoreCase("file"))
                        {
                            return "source=" + source;
                        }
                        else if (url.getProtocol().equalsIgnoreCase("data"))
                        {
                            return "source=<data stream>";
                        }
                        else
                        {
                            return "source=<unknown source type>";
                        }
                    }
                }));
            }
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Cannot convert '" + source + "' to a readable resource", e);
        }
    }

    private void checkMessageStoreEmpty()
    {
        final MessageStore.MessageStoreReader reader = _messageStore.newMessageStoreReader();
        final StoreEmptyCheckingHandler handler = new StoreEmptyCheckingHandler();
        reader.visitMessages(handler);
        if(handler.isEmpty())
        {
            reader.visitMessageInstances(handler);

            if(handler.isEmpty())
            {
                reader.visitDistributedTransactions(handler);
            }
        }

        if(!handler.isEmpty())
        {
            throw new IllegalArgumentException("The message store is not empty");
        }
    }

    private static URL convertStringToURL(final String source)
    {
        URL url;
        try
        {
            url = new URL(source);
        }
        catch (MalformedURLException e)
        {
            File file = new File(source);
            try
            {
                url = file.toURI().toURL();
            }
            catch (MalformedURLException notAFile)
            {
                throw new IllegalConfigurationException("Cannot convert " + source + " to a readable resource",
                                                        notAFile);
            }
        }
        return url;
    }

    @Override
    public boolean authoriseCreateConnection(final AMQPConnection<?> connection)
    {
        authorise(Operation.PERFORM_ACTION("connect"));
        for(ConnectionValidator validator : _connectionValidators)
        {
            if(!validator.validateConnectionCreation(connection, this))
            {
                return false;
            }
        }
        return true;
    }

    private void initialiseStatisticsReporting()
    {
        long report = getStatisticsReportingPeriod() * 1000L;

        ScheduledFuture<?> previousStatisticsReportingFuture = _statisticsReportingFuture;
        if (previousStatisticsReportingFuture != null)
        {
            previousStatisticsReportingFuture.cancel(false);
        }
        if (report > 0L)
        {
            _statisticsReportingFuture = _houseKeepingTaskExecutor.scheduleAtFixedRate(new StatisticsReportingTask(this,
                                                                                                                   getSystemTaskSubject(
                                                                                                                           "Statistics", _principal)),
                                                                                       report,
                                                                                       report,
                                                                                       TimeUnit.MILLISECONDS);
        }
    }

    private void initialiseHouseKeeping()
    {
        final long period = getHousekeepingCheckPeriod();
        if (period > 0L)
        {
            scheduleHouseKeepingTask(period, new VirtualHostHouseKeepingTask());
        }
    }

    private void initialiseFlowToDiskChecking()
    {
        final long period = getFlowToDiskCheckPeriod();
        if (period > 0L)
        {
            scheduleHouseKeepingTask(period, new FlowToDiskCheckingTask());
        }
    }

    private void shutdownHouseKeeping()
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
                LOGGER.warn("Interrupted during Housekeeping shutdown:", e);
                Thread.currentThread().interrupt();
            }
            finally
            {
                _houseKeepingTaskExecutor = null;
            }
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
    @Override
    public void scheduleHouseKeepingTask(long period, HouseKeepingTask task)
    {
        task.setFuture(_houseKeepingTaskExecutor.scheduleAtFixedRate(task, period / 2, period, TimeUnit.MILLISECONDS));
    }


    @Override
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
    public List<NodeAutoCreationPolicy> getNodeAutoCreationPolicies()
    {
        return _nodeAutoCreationPolicies;
    }

    @Override
    public MessageSource getAttainedMessageSource(final String name)
    {
        MessageSource messageSource = _systemNodeSources.get(name);
        if(messageSource == null)
        {
            messageSource = getAttainedChildFromAddress(Queue.class, name);
        }
        if(messageSource == null)
        {
            messageSource = autoCreateNode(name, MessageSource.class, false);
        }
        return messageSource;
    }


    private <T> T autoCreateNode(final String name, final Class<T> clazz, boolean publish)
    {
        for (NodeAutoCreationPolicy policy : getNodeAutoCreationPolicies())
        {
            String pattern = policy.getPattern();
            if (name.matches(pattern) &&
                ((publish && policy.isCreatedOnPublish()) || (!publish && policy.isCreatedOnConsume())))
            {
                String nodeType = policy.getNodeType();
                Class<? extends ConfiguredObject> sourceClass = null;
                for (Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(getCategoryClass()))
                {
                    if (childClass.getSimpleName().equalsIgnoreCase(nodeType.trim())
                        && clazz.isAssignableFrom(childClass))
                    {
                        sourceClass = childClass;
                    }
                }
                if (sourceClass != null)
                {
                    final Map<String, Object> attributes = new HashMap<>(policy.getAttributes());
                    attributes.remove(ConfiguredObject.ID);
                    attributes.put(ConfiguredObject.NAME, name);
                    final Class<? extends ConfiguredObject> childClass = sourceClass;
                    try
                    {

                        final T node =  Subject.doAs(getSubjectWithAddedSystemRights(),
                                                     new PrivilegedAction<T>()
                                                     {
                                                         @Override
                                                         public T run()
                                                         {
                                                             return (T) doSync(createChildAsync(childClass,
                                                                                                attributes));

                                                         }
                                                     }
                                                    );

                        if (node != null)
                        {
                            return node;
                        }

                    }
                    catch (AbstractConfiguredObject.DuplicateNameException e)
                    {
                        return (T)e.getExisting();
                    }
                    catch (RuntimeException e)
                    {
                        LOGGER.info("Unable to auto create a node named '{}' due to exception", name, e);
                    }
                }
            }

        }
        return null;

    }


    @Override
    public Queue<?> getAttainedQueue(UUID id)
    {
        return (Queue<?>) awaitChildClassToAttainState(Queue.class, id);
    }

    @Override
    public Queue<?> getAttainedQueue(final String name)
    {
        return (Queue<?>) awaitChildClassToAttainState(Queue.class, name);
    }

    @Override
    public Broker<?> getBroker()
    {
        return _broker;
    }

    @Override
    public MessageDestination getAttainedMessageDestination(final String name, final boolean mayCreate)
    {
        MessageDestination destination = _systemNodeDestinations.get(name);
        if(destination == null)
        {
            destination = getAttainedChildFromAddress(Exchange.class, name);
        }
        if(destination == null)
        {
            destination = getAttainedChildFromAddress(Queue.class, name);
        }
        if(destination == null && mayCreate)
        {
            destination = autoCreateNode(name, MessageDestination.class, true);
        }
        return destination;
    }

    @Override
    public MessageDestination getSystemDestination(final String name)
    {
        return _systemNodeDestinations.get(name);
    }

    @Override
    public ListenableFuture<Void> reallocateMessages()
    {
        final ScheduledThreadPoolExecutor houseKeepingTaskExecutor = _houseKeepingTaskExecutor;
        if (houseKeepingTaskExecutor != null)
        {
            try
            {
                final Future<Void> future = houseKeepingTaskExecutor.submit(() ->
                                                                            {
                                                                                final Collection<Queue> queues =
                                                                                        getChildren(Queue.class);
                                                                                for (Queue q : queues)
                                                                                {
                                                                                    if (q.getState() == State.ACTIVE)
                                                                                    {
                                                                                        q.reallocateMessages();
                                                                                    }
                                                                                }
                                                                                return null;
                                                                            });
                return JdkFutureAdapters.listenInPoolThread(future);
            }
            catch (RejectedExecutionException e)
            {
                if (!houseKeepingTaskExecutor.isShutdown())
                {
                    LOGGER.warn("Failed to schedule reallocation of messages", e);
                }
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public long getTotalDepthOfQueuesBytes()
    {
        long total = 0;
        final Collection<Queue> queues = getChildren(Queue.class);
        for(Queue q : queues)
        {
            total += q.getQueueDepthBytes();
        }
        return total;
    }

    @Override
    public long getTotalDepthOfQueuesMessages()
    {
        long total = 0;
        final Collection<Queue> queues = getChildren(Queue.class);
        for(Queue q : queues)
        {
            total += q.getQueueDepthMessages();
        }
        return total;
    }

    @Override
    public long getInMemoryMessageSize()
    {
        return _messageStore == null ? -1 : _messageStore.getInMemorySize();
    }

    @Override
    public long getBytesEvacuatedFromMemory()
    {
        return _messageStore == null ? -1 : _messageStore.getBytesEvacuatedFromMemory();
    }

    @Override
    public <T extends ConfiguredObject<?>> T getAttainedChildFromAddress(final Class<T> childClass,
                                                                         final String address)
    {
        T child = awaitChildClassToAttainState(childClass, address);
        if(child == null && getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                if(address.startsWith(domain + "/"))
                {
                    child = awaitChildClassToAttainState(childClass, address.substring(domain.length()+1));
                    if(child != null)
                    {
                        break;
                    }
                }
            }
        }
        return child;
    }

    @Override
    public long getInboundMessageSizeHighWatermark()
    {
        return _maximumMessageSize.get();
    }

    @Override
    public MessageDestination getDefaultDestination()
    {
        return _defaultDestination;
    }


    private ListenableFuture<Exchange<?>> addExchangeAsync(Map<String,Object> attributes)
            throws ReservedExchangeNameException,
                   NoFactoryForTypeException
    {
        final SettableFuture<Exchange<?>> returnVal = SettableFuture.create();
        addFutureCallback(getObjectFactory().createAsync(Exchange.class, attributes, this),
                            new FutureCallback<Exchange>()
                            {
                                @Override
                                public void onSuccess(final Exchange result)
                                {
                                    returnVal.set(result);
                                }

                                @Override
                                public void onFailure(final Throwable t)
                                {
                                    returnVal.setException(t);
                                }
                            }, getTaskExecutor());
        return returnVal;

    }

    @Override
    public String getLocalAddress(final String routingAddress)
    {
        if(getGlobalAddressDomains() != null)
        {
            for(String domain : getGlobalAddressDomains())
            {
                if(routingAddress.startsWith(domain + "/"))
                {
                    return routingAddress.substring(domain.length() + 1);
                }
            }
        }
        return routingAddress;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        return beforeDeleteOrClose();
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        return onCloseOrDelete();
    }

    @Override
    protected ListenableFuture<Void> beforeDelete()
    {
        return beforeDeleteOrClose();
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        _deleteRequested  = true;
        return onCloseOrDelete();
    }

    private ListenableFuture<Void> beforeDeleteOrClose()
    {
        setState(State.UNAVAILABLE);
        _virtualHostLoggersToClose = new ArrayList<>(getChildren(VirtualHostLogger.class));
        //Stop Connections
        return closeConnections();
    }

    private ListenableFuture<Void> onCloseOrDelete()
    {
        _dtxRegistry.close();
        shutdownHouseKeeping();

        if (_deleteRequested)
        {
            deleteLinkRegistry();
        }

        closeMessageStore();
        stopPreferenceTaskExecutor();
        closePreferenceStore();

        if (_deleteRequested)
        {
            deleteMessageStore();
            deletePreferenceStore();
        }

        closeNetworkConnectionScheduler();
        _eventLogger.message(VirtualHostMessages.CLOSED(getName()));

        stopLogging(_virtualHostLoggersToClose);
        _systemNodeRegistry.close();
        return Futures.immediateFuture(null);
    }


    private ListenableFuture<Void> closeConnections()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Closing connection registry : {} connection(s).", _connections.size());
        }
        _acceptsConnections.set(false);
        for(AMQPConnection<?> conn : _connections)
        {
            conn.stopConnection();
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
                    LOGGER.warn("Exception closing connection " + connection.getName() + " from " + connection.getRemoteAddress(), e);
                }
                finally
                {
                    itr.remove();
                }
            }
        }
        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(connectionCloseFutures);
        return Futures.transform(combinedFuture, voids -> null, MoreExecutors.directExecutor());
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
                LOGGER.error("Failed to close message store", e);
            }

            if (!(_virtualHostNode.getConfigurationStore() instanceof MessageStoreProvider))
            {
                getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.CLOSED());
            }
        }
    }

    @Override
    public void registerMessageDelivered(long messageSize)
    {
        _messagesOut.incrementAndGet();
        _bytesOut.addAndGet(messageSize);
        _broker.registerMessageDelivered(messageSize);
    }

    @Override
    public void registerMessageReceived(long messageSize)
    {
        _messagesIn.incrementAndGet();
        _bytesIn.addAndGet(messageSize);
        _broker.registerMessageReceived(messageSize);
        long hwm;
        while((hwm = _maximumMessageSize.get()) < messageSize)
        {
            _maximumMessageSize.compareAndSet(hwm, messageSize);
        }
    }

    @Override
    public void registerTransactedMessageReceived()
    {
        _transactedMessagesIn.incrementAndGet();
        _broker.registerTransactedMessageReceived();
    }

    @Override
    public void registerTransactedMessageDelivered()
    {
        _transactedMessagesOut.incrementAndGet();
        _broker.registerTransactedMessageDelivered();
    }

    @Override
    public long getMessagesIn()
    {
        return _messagesIn.get();
    }

    @Override
    public long getBytesIn()
    {
        return _bytesIn.get();
    }

    @Override
    public long getMessagesOut()
    {
        return _messagesOut.get();
    }

    @Override
    public long getBytesOut()
    {
        return _bytesOut.get();
    }

    @Override
    public long getTransactedMessagesIn()
    {
        return _transactedMessagesIn.get();
    }

    @Override
    public long getTransactedMessagesOut()
    {
        return _transactedMessagesOut.get();
    }

    @Override
    public <T extends LinkModel> T getSendingLink( String remoteContainerId, String linkName)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<T>, RuntimeException>()
        {
            @Override
            public ListenableFuture<T> execute()
            {
                return Futures.immediateFuture((T)_linkRegistry.getSendingLink(remoteContainerId, linkName));
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "getSendingLink";
            }

            @Override
            public String getArguments()
            {
                return String.format("remoteContainerId='%s', linkName='%s'", remoteContainerId, linkName);
            }
        }));
    }

    @Override
    public <T extends LinkModel> T getReceivingLink(String remoteContainerId, String linkName)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<T>, RuntimeException>()
        {
            @Override
            public ListenableFuture<T> execute()
            {
                return Futures.immediateFuture((T)_linkRegistry.getReceivingLink(remoteContainerId, linkName));
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "getReceivingLink";
            }

            @Override
            public String getArguments()
            {
                return String.format("remoteContainerId='%s', linkName='%s'", remoteContainerId, linkName);
            }
        }));
    }

    @Override
    public <T extends LinkModel> Collection<T> findSendingLinks(final Pattern containerIdPattern,
                                                                final Pattern linkNamePattern)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<Collection<T>>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Collection<T>> execute()
            {
                return Futures.immediateFuture(_linkRegistry.findSendingLinks(containerIdPattern, linkNamePattern));
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "findSendingLinks";
            }

            @Override
            public String getArguments()
            {
                return String.format("containerIdPattern='%s', linkNamePattern='%s'", containerIdPattern, linkNamePattern);
            }
        }));
    }

    @Override
    public <T extends LinkModel> void visitSendingLinks(final LinkRegistryModel.LinkVisitor<T> visitor)
    {
        doSync(doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                _linkRegistry.visitSendingLinks(visitor);
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
                return "visitSendingLinks";
            }

            @Override
            public String getArguments()
            {
                return String.format("visitor='%s'", visitor);
            }
        }));
    }

    @Override
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
                for(AMQPConnection<?> conn : _connections)
                {
                    conn.block();
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
                for(AMQPConnection<?> conn : _connections)
                {
                    conn.unblock();
                }
            }
        }
    }

    @Override
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

    private void reportIfError(State state)
    {
        if (state == State.ERRORED)
        {
            _eventLogger.message(VirtualHostMessages.ERRORED(getName()));
        }
    }

    @Override
    public String getRedirectHost(final AmqpPort<?> port)
    {
        return null;
    }

    @Override
    public boolean isOverTargetSize()
    {
        return getInMemoryMessageSize() > _targetSize.get();
    }

    private static class MessageHeaderImpl implements AMQMessageHeader
    {
        private final String _userName;
        private final long _timestamp;
        private final ManageableMessage _message;

        public MessageHeaderImpl(final ManageableMessage message)
        {
            _message = message;
            final AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
            _userName = (currentUser == null ? null : currentUser.getName());
            _timestamp = System.currentTimeMillis();
        }

        @Override
        public String getCorrelationId()
        {
            return _message.getCorrelationId();
        }

        @Override
        public long getExpiration()
        {
            Date expiration = _message.getExpiration();
            return expiration == null ? 0 : expiration.getTime();
        }

        @Override
        public String getUserId()
        {
            return _userName;
        }

        @Override
        public String getAppId()
        {
            return null;
        }

        @Override
        public String getGroupId()
        {
            Object jmsXGroupId = getHeader("JMSXGroupID");
            return jmsXGroupId == null ? null : String.valueOf(jmsXGroupId);
        }

        @Override
        public String getMessageId()
        {
            return _message.getMessageId();
        }

        @Override
        public String getMimeType()
        {
            return _message.getMimeType();
        }

        @Override
        public String getEncoding()
        {
            return _message.getEncoding();
        }

        @Override
        public byte getPriority()
        {
            return (byte) _message.getPriority();
        }

        @Override
        public long getTimestamp()
        {
            return _timestamp;
        }

        @Override
        public long getNotValidBefore()
        {
            final Date notValidBefore = _message.getNotValidBefore();
            return notValidBefore == null ? 0 : notValidBefore.getTime();
        }

        @Override
        public String getType()
        {
            return null;
        }

        @Override
        public String getReplyTo()
        {
            return _message.getReplyTo();
        }

        @Override
        public Object getHeader(final String name)
        {
            return getHeaders().get(name);
        }

        @Override
        public boolean containsHeaders(final Set<String> names)
        {
            return getHeaders().keySet().containsAll(names);
        }

        @Override
        public boolean containsHeader(final String name)
        {
            return getHeaders().keySet().contains(name);
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            return Collections.unmodifiableCollection(getHeaders().keySet());
        }

        private Map<String, Object> getHeaders()
        {
            return _message.getHeaders() == null ? Collections.<String, Object>emptyMap() : _message.getHeaders();
        }
    }

    private class VirtualHostHouseKeepingTask extends HouseKeepingTask
    {
        public VirtualHostHouseKeepingTask()
        {
            super("Housekeeping["+AbstractVirtualHost.this.getName()+"]",AbstractVirtualHost.this,_housekeepingJobContext);
        }

        @Override
        public void execute()
        {
            for (Queue<?> q : getChildren(Queue.class))
            {
                if (q.getState() == State.ACTIVE)
                {
                    LOGGER.debug("Checking message status for queue: {}", q.getName());
                    q.checkMessageStatus();
                }
            }
        }
    }

    class FlowToDiskCheckingTask extends HouseKeepingTask
    {
        public FlowToDiskCheckingTask()
        {
            super("FlowToDiskChecking["+AbstractVirtualHost.this.getName()+"]", AbstractVirtualHost.this, _housekeepingJobContext);
        }

        @Override
        public void execute()
        {
            if (isOverTargetSize())
            {
                long currentTargetSize = _targetSize.get();
                List<QueueEntryIterator> queueIterators = new ArrayList<>();
                for (Queue<?> q : getChildren(Queue.class))
                {
                    queueIterators.add(q.queueEntryIterator());
                }
                Collections.shuffle(queueIterators);

                long cumulativeSize = 0;
                final Iterator<QueueEntryIterator> cyclicIterators = cycle(queueIterators);
                while (cyclicIterators.hasNext())
                {
                    final QueueEntryIterator queueIterator = cyclicIterators.next();
                    if (queueIterator.advance())
                    {
                        QueueEntry node = queueIterator.getNode();
                        if (node != null && !node.isDeleted())
                        {
                            try (MessageReference messageReference = node.getMessage().newReference())
                            {
                                final StoredMessage storedMessage = messageReference.getMessage().getStoredMessage();
                                final long inMemorySize = storedMessage.getInMemorySize();
                                if (inMemorySize > 0)
                                {
                                    if (cumulativeSize <= currentTargetSize)
                                    {
                                        cumulativeSize += inMemorySize;
                                    }

                                    if (cumulativeSize > currentTargetSize && node.getQueue().checkValid(node))
                                    {
                                        storedMessage.flowToDisk();
                                    }
                                }
                            }
                            catch (MessageDeletedException e)
                            {
                                // pass
                            }
                        }
                    }
                    else
                    {
                        cyclicIterators.remove();
                    }
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
                removeMessageSource(node.getName());
            }
        }

        private void removeMessageSource(final String name)
        {
            MessageSource messageSource = _systemNodeSources.remove(name);
            if (messageSource != null)
            {
                messageSource.close();
            }
        }

        @Override
        public void removeSystemNode(final String name)
        {
            _systemNodeDestinations.remove(name);
            removeMessageSource(name);
        }

        @Override
        public VirtualHostNode<?> getVirtualHostNode()
        {
            return (VirtualHostNode) getParent();
        }

        @Override
        public VirtualHost<?> getVirtualHost()
        {
            return AbstractVirtualHost.this;
        }

        @Override
        public boolean hasSystemNode(final String name)
        {
            return _systemNodeSources.containsKey(name) || _systemNodeDestinations.containsKey(name);
        }

        public void close()
        {
            _systemNodeSources.values().forEach(MessageSource::close);
        }
    }


    @Override
    public void executeTransaction(TransactionalOperation op)
    {
        final MessageStore store = getMessageStore();
        final LocalTransaction txn = new LocalTransaction(store);

        op.withinTransaction(new Transaction()
        {
            @Override
            public void dequeue(final QueueEntry messageInstance)
            {
                final ServerTransaction.Action deleteAction = new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {
                        messageInstance.delete();
                    }

                    @Override
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

            @Override
            public void copy(QueueEntry entry, final Queue<?> queue)
            {
                final ServerMessage message = entry.getMessage();

                txn.enqueue(queue, message, new ServerTransaction.EnqueueAction()
                {
                    @Override
                    public void postCommit(MessageEnqueueRecord... records)
                    {
                        queue.enqueue(message, null, records[0]);
                    }

                    @Override
                    public void onRollback()
                    {
                    }
                });

            }

            @Override
            public void move(final QueueEntry entry, final Queue<?> queue)
            {
                final ServerMessage message = entry.getMessage();
                if(entry.acquire())
                {
                    txn.enqueue(queue, message,
                                new ServerTransaction.EnqueueAction()
                                {

                                    @Override
                                    public void postCommit(MessageEnqueueRecord... records)
                                    {
                                        queue.enqueue(message, null, records[0]);
                                    }

                                    @Override
                                    public void onRollback()
                                    {
                                        entry.release();
                                    }
                                });
                    txn.dequeue(entry.getEnqueueRecord(),
                                new ServerTransaction.Action()
                                {

                                    @Override
                                    public void postCommit()
                                    {
                                        entry.delete();
                                    }

                                    @Override
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
    public long getHousekeepingCheckPeriod()
    {
        return _housekeepingCheckPeriod;
    }

    @Override
    public long getFlowToDiskCheckPeriod()
    {
        return _flowToDiskCheckPeriod;
    }

    @Override
    public boolean isDiscardGlobalSharedSubscriptionLinksOnDetach()
    {
        return _isDiscardGlobalSharedSubscriptionLinksOnDetach;
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
        return getChildren(Queue.class).size();
    }

    @Override
    public long getExchangeCount()
    {
        return getChildren(Exchange.class).size();
    }

    @Override
    public long getConnectionCount()
    {
        return _connections.size();
    }

    @Override
    public long getTotalConnectionCount()
    {
        return _totalConnectionCount.get();
    }

    @Override
    public int getHousekeepingThreadCount()
    {
        return _housekeepingThreadCount;
    }

    @Override
    public int getStatisticsReportingPeriod()
    {
        return _statisticsReportingPeriod;
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
                resetConnectionPrincipalStatisticsRegistry();
                shutdownHouseKeeping();
                closeNetworkConnectionScheduler();
                if (_linkRegistry != null)
                {
                    _linkRegistry.close();
                }
                closeMessageStore();
                stopPreferenceTaskExecutor();
                closePreferenceStore();
                setState(State.STOPPED);
                stopLogging(loggers);
            }
        });
    }

    @Override
    public UserPreferences createUserPreferences(ConfiguredObject<?> object)
    {
        if (_preferenceTaskExecutor == null || !_preferenceTaskExecutor.isRunning())
        {
            throw new IllegalStateException("Cannot create user preferences in not fully initialized virtual host");
        }
        return new UserPreferencesImpl(_preferenceTaskExecutor, object, _preferenceStore, Collections.<Preference>emptySet());
    }

    private void stopPreferenceTaskExecutor()
    {
        if (_preferenceTaskExecutor != null)
        {
            _preferenceTaskExecutor.stop();
            _preferenceTaskExecutor = null;
        }
    }

    private void closePreferenceStore()
    {
        if (_preferenceStore != null)
        {
            _preferenceStore.close();
        }
    }

    private void stopLogging(Collection<VirtualHostLogger> loggers)
    {
        for (VirtualHostLogger logger : loggers)
        {
            logger.stopLogging();
        }
    }

    private void deleteLinkRegistry()
    {
        if (_linkRegistry != null)
        {
            _linkRegistry.delete();
            _linkRegistry = null;
        }
    }

    private void deletePreferenceStore()
    {
        final PreferenceStore ps = _preferenceStore;
        if (ps != null)
        {
            try
            {
                ps.onDelete();
            }
            catch (Exception e)
            {
                LOGGER.warn("Exception occurred on preference store deletion", e);
            }
            finally
            {
                _preferenceStore = null;

            }
        }
    }

    private void deleteMessageStore()
    {
        MessageStore ms = _messageStore;
        if (ms != null)
        {
            try
            {
                ms.onDelete(AbstractVirtualHost.this);
            }
            catch (Exception e)
            {
                LOGGER.warn( "Exception occurred on message store deletion", e);
            }
            finally
            {
                _messageStore = null;
            }
        }
    }

    @Override
    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public String getProductVersion()
    {
        return _broker.getProductVersion();
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
    }

    @Override
    public long getTargetSize()
    {
        return _targetSize.get();
    }

    @Override
    public Principal getPrincipal()
    {
        return _principal;
    }

    @Override
    public boolean registerConnection(final AMQPConnection<?> connection,
                                      final ConnectionEstablishmentPolicy connectionEstablishmentPolicy)
    {
        return doSync(registerConnectionAsync(connection, connectionEstablishmentPolicy));
    }

    public ListenableFuture<Boolean> registerConnectionAsync(final AMQPConnection<?> connection,
                                                          final ConnectionEstablishmentPolicy connectionEstablishmentPolicy)
    {
        return doOnConfigThread(new Task<ListenableFuture<Boolean>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Boolean> execute()
            {
                if (_acceptsConnections.get())
                {
                    if (connectionEstablishmentPolicy.mayEstablishNewConnection(_connections, connection))
                    {
                        final ConnectionPrincipalStatistics cps =
                                _connectionPrincipalStatisticsRegistry.connectionOpened(connection);
                        connection.registered(cps);
                        _connections.add(connection);
                        _totalConnectionCount.incrementAndGet();

                        if (_blocked.get())
                        {
                            connection.block();
                        }

                        connection.pushScheduler(_networkConnectionScheduler);
                        return Futures.immediateFuture(true);
                    }
                    else
                    {
                        return Futures.immediateFuture(false);
                    }
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
                _connections.remove(connection);
                _connectionPrincipalStatisticsRegistry.connectionClosed(connection);

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

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> onActivate()
    {

        long threadPoolKeepAliveTimeout = getContextValue(Long.class, CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT);

        final SuppressingInheritedAccessControlContextThreadFactory connectionThreadFactory =
                new SuppressingInheritedAccessControlContextThreadFactory("virtualhost-" + getName() + "-iopool",
                                                                          getSystemTaskSubject("IO Pool", getPrincipal()));

        _networkConnectionScheduler = new NetworkConnectionScheduler("virtualhost-" + getName() + "-iopool",
                                                                     getNumberOfSelectors(),
                                                                     getConnectionThreadPoolSize(),
                                                                     threadPoolKeepAliveTimeout,
                                                                     connectionThreadFactory);
        _networkConnectionScheduler.start();

        updateAccessControl();
        initialiseStatisticsReporting();
        initialiseConnectionPrincipalStatisticsRegistry();

        MessageStore messageStore = getMessageStore();
        messageStore.openMessageStore(this);

        startFileSystemSpaceChecking();


        if (!(_virtualHostNode.getConfigurationStore() instanceof MessageStoreProvider))
        {
            getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.CREATED());
            getEventLogger().message(getMessageStoreLogSubject(), MessageStoreMessages.STORE_LOCATION(messageStore.getStoreLocation()));
        }

        messageStore.upgradeStoreStructure();

        if (_linkRegistry != null)
        {
            _linkRegistry.open();
        }

        getBroker().assignTargetSizes();

        final PreferenceStoreUpdater updater = new PreferenceStoreUpdaterImpl();
        Collection<PreferenceRecord> records = _preferenceStore.openAndLoad(updater);
        _preferenceTaskExecutor = new TaskExecutorImpl("virtualhost-" + getName() + "-preferences", null);
        _preferenceTaskExecutor.start();
        PreferencesRecoverer preferencesRecoverer = new PreferencesRecoverer(_preferenceTaskExecutor);
        preferencesRecoverer.recoverPreferences(this, records, _preferenceStore);

        if (_createDefaultExchanges)
        {
            return doAfter(createDefaultExchanges(), new Runnable()
            {
                @Override
                public void run()
                {
                    _createDefaultExchanges = false;
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

    private void initialiseConnectionPrincipalStatisticsRegistry()
    {
        final long connectionFrequencyPeriodMillis = getContextValue(Long.class, CONNECTION_FREQUENCY_PERIOD);
        final Duration connectionFrequencyPeriod = Duration.ofMillis(connectionFrequencyPeriodMillis);
        final ConnectionPrincipalStatisticsRegistryImpl connectionStatisticsRegistry =
                new ConnectionPrincipalStatisticsRegistryImpl(() -> connectionFrequencyPeriod);
        HouseKeepingTask task = null;
        long taskRunPeriod = connectionFrequencyPeriodMillis / 2;
        if (taskRunPeriod > 0)
        {
            final AccessControlContext context =
                    getSystemTaskControllerContext("ConnectionPrincipalStatisticsCheck", _principal);
            task = new ConnectionPrincipalStatisticsCheckingTask(this, context, connectionStatisticsRegistry);
            scheduleHouseKeepingTask(taskRunPeriod, task);
        }
        _statisticsCheckTask = task;
        _connectionPrincipalStatisticsRegistry = connectionStatisticsRegistry;
    }

    private void resetConnectionPrincipalStatisticsRegistry()
    {
        final HouseKeepingTask previousStatisticsCheckTask = _statisticsCheckTask;
        if (previousStatisticsCheckTask != null)
        {
            previousStatisticsCheckTask.cancel();
        }
        _statisticsCheckTask = null;
        final ConnectionPrincipalStatisticsRegistry connectionPrincipalStatisticsRegistry =
                _connectionPrincipalStatisticsRegistry;
        if (connectionPrincipalStatisticsRegistry != null)
        {
            connectionPrincipalStatisticsRegistry.reset();
        }
        _connectionPrincipalStatisticsRegistry = null;
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
        // TODO if message recovery fails we ought to be transitioning the VH into ERROR and releasing the thread-pools etc.
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
            initialiseHouseKeeping();
            initialiseFlowToDiskChecking();
            finalState = State.ACTIVE;
            _acceptsConnections.set(true);
        }
        finally
        {
            setState(finalState);
            reportIfError(getState());
        }
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(VirtualHostMessages.OPERATION(operation));
    }

    protected void startFileSystemSpaceChecking()
    {
        long housekeepingCheckPeriod = getHousekeepingCheckPeriod();
        File storeLocationAsFile = _messageStore.getStoreLocationAsFile();
        if (storeLocationAsFile != null && _fileSystemMaxUsagePercent > 0 && housekeepingCheckPeriod > 0)
        {
            _fileSystemSpaceChecker.setFileSystem(storeLocationAsFile);

            scheduleHouseKeepingTask(housekeepingCheckPeriod, _fileSystemSpaceChecker);
        }
    }

    @Override
    public SocketConnectionMetaData getConnectionMetaData()
    {
        return getBroker().getConnectionMetaData();
    }

    @StateTransition( currentState = { State.STOPPED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> onRestart()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        try
        {
            addFutureCallback(doRestart(),new FutureCallback<Void>()
                              {
                                  @Override
                                  public void onSuccess(final Void result)
                                  {
                                      returnVal.set(null);
                                  }

                                  @Override
                                  public void onFailure(final Throwable t)
                                  {
                                      doAfterAlways(onRestartFailure(), ()-> returnVal.setException(t));
                                  }
                              }, getTaskExecutor()
                             );
        }
        catch (IllegalArgumentException | IllegalConfigurationException e)
        {
            doAfterAlways(onRestartFailure(), ()-> returnVal.setException(e));
        }
        return returnVal;
    }

    private ListenableFuture<Void> doRestart()
    {
        createHousekeepingExecutor();

        final VirtualHostStoreUpgraderAndRecoverer virtualHostStoreUpgraderAndRecoverer =
                new VirtualHostStoreUpgraderAndRecoverer((VirtualHostNode<?>) getParent());
        virtualHostStoreUpgraderAndRecoverer.reloadAndRecoverVirtualHost(getDurableConfigurationStore());

        final Collection<VirtualHostAccessControlProvider> accessControlProviders = getChildren(VirtualHostAccessControlProvider.class);
        if (!accessControlProviders.isEmpty())
        {
            accessControlProviders.forEach(child -> child.addChangeListener(_accessControlProviderListener));
        }

        final List<ListenableFuture<Void>> childOpenFutures = new ArrayList<>();

        Subject.doAs(getSubjectWithAddedSystemRights(), (PrivilegedAction<Object>) () ->
        {
            applyToChildren(child ->
                            {
                                final ListenableFuture<Void> childOpenFuture = child.openAsync();
                                childOpenFutures.add(childOpenFuture);

                                addFutureCallback(childOpenFuture, new FutureCallback<Void>()
                                {
                                    @Override
                                    public void onSuccess(final Void result)
                                    {
                                    }

                                    @Override
                                    public void onFailure(final Throwable t)
                                    {
                                        LOGGER.error("Exception occurred while opening {} : {}",
                                                      child.getClass().getSimpleName(), child.getName(), t);
                                        onRestartFailure();
                                    }

                                }, getTaskExecutor());
                            });
            return null;
        });

        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(childOpenFutures);
        return Futures.transformAsync(combinedFuture, input -> onActivate(), MoreExecutors.directExecutor());
    }

    private ChainedListenableFuture<Void> onRestartFailure()
    {
        final List<VirtualHostLogger> loggers = new ArrayList<>(getChildren(VirtualHostLogger.class));
        return doAfter(closeChildren(), () -> {
            shutdownHouseKeeping();
            closeNetworkConnectionScheduler();
            if (_linkRegistry != null)
            {
                _linkRegistry.close();
            }
            closeMessageStore();
            stopPreferenceTaskExecutor();
            closePreferenceStore();
            setState(State.ERRORED);
            stopLogging(loggers);
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
                LOGGER.warn("Cannot check file system for disk space because store path '{}' is not valid", _fileSystem.getPath());
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

    @Override
    public <T extends MessageSource> T createMessageSource(final Class<T> clazz, final Map<String, Object> attributes)
    {
        if(Queue.class.isAssignableFrom(clazz))
        {
            return (T) createChild((Class<? extends Queue>)clazz, attributes);
        }
        else if(clazz.isAssignableFrom(Queue.class))
        {
            return (T) createChild(Queue.class, attributes);
        }
        else
        {
            throw new IllegalArgumentException("Cannot create message source children of class " + clazz.getSimpleName());
        }
    }

    @Override
    public <T extends MessageDestination> T createMessageDestination(final Class<T> clazz,
                                                                     final Map<String, Object> attributes)
    {
        if(Exchange.class.isAssignableFrom(clazz))
        {
            return (T) createChild((Class<? extends Exchange>)clazz, attributes);
        }
        else if(Queue.class.isAssignableFrom(clazz))
        {
            return (T) createChild((Class<? extends Queue>)clazz, attributes);
        }
        else if(clazz.isAssignableFrom(Queue.class))
        {
            return (T) createChild(Queue.class, attributes);
        }
        else
        {
            throw new IllegalArgumentException("Cannot create message destination children of class " + clazz.getSimpleName());
        }
    }

    @Override
    public boolean hasMessageSources()
    {
        return !(_systemNodeSources.isEmpty() && getChildren(Queue.class).isEmpty());
    }

    @Override
    @DoOnConfigThread
    public Queue<?> getSubscriptionQueue(@Param(name = "exchangeName", mandatory = true) final String exchangeName,
                                         @Param(name = "attributes", mandatory = true) final Map<String, Object> attributes,
                                         @Param(name = "bindings", mandatory = true) final Map<String, Map<String, Object>> bindings)
    {
        Queue queue;
        Object exclusivityPolicy = attributes.get(Queue.EXCLUSIVE);
        if (exclusivityPolicy == null)
        {
            exclusivityPolicy = getContextValue(ExclusivityPolicy.class, Queue.QUEUE_DEFAULT_EXCLUSIVITY_POLICY);
        }
        if (!(exclusivityPolicy instanceof ExclusivityPolicy))
        {
            throw new IllegalArgumentException("Exclusivity policy is required");
        }
        Exchange<?> exchange = findConfiguredObject(Exchange.class, exchangeName);
        if (exchange == null)
        {
            throw new NotFoundException(String.format("Exchange '%s' was not found", exchangeName));
        }
        try
        {
            queue = createMessageDestination(Queue.class, attributes);

            for (String binding : bindings.keySet())
            {
                exchange.addBinding(binding, queue, bindings.get(binding));
            }
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            Queue<?> existingQueue = (Queue) e.getExisting();

            if (existingQueue.getExclusive() == exclusivityPolicy)
            {
                if (hasDifferentBindings(exchange, existingQueue, bindings))
                {
                    if (existingQueue.getConsumers().isEmpty())
                    {
                        existingQueue.delete();

                        queue = createMessageDestination(Queue.class, attributes);

                        for (String binding : bindings.keySet())
                        {
                            try
                            {
                                exchange.addBinding(binding, queue, bindings.get(binding));
                            }
                            catch (AMQInvalidArgumentException ia)
                            {
                                throw new IllegalArgumentException("Unexpected bind argument : " + ia.getMessage(), ia);
                            }
                        }
                    }
                    else
                    {
                        throw new IllegalStateException("subscription already in use");
                    }
                }
                else
                {
                    queue = existingQueue;
                }
            }
            else
            {
                throw new IllegalStateException("subscription already in use");
            }
        }
        catch (AMQInvalidArgumentException e)
        {
            throw new IllegalArgumentException("Unexpected bind argument : " + e.getMessage(), e);
        }
        return queue;
    }

    @Override
    @DoOnConfigThread
    public void removeSubscriptionQueue(@Param(name = "queueName", mandatory = true) final String queueName) throws NotFoundException
    {
        Queue<?> queue = findConfiguredObject(Queue.class, queueName);
        if (queue == null)
        {
            throw new NotFoundException(String.format("Queue '%s' was not found", queueName));
        }

        if (queue.getConsumers().isEmpty())
        {
            queue.delete();
        }
        else
        {
            throw new IllegalStateException("There are consumers on Queue '" + queueName + "'");
        }
    }

    @Override
    public Object dumpLinkRegistry()
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<Object>, IOException>()
        {
            @Override
            public ListenableFuture<Object> execute() throws IOException
            {
                Object dump;
                if (getState() == State.STOPPED)
                {
                    _messageStore.openMessageStore(AbstractVirtualHost.this);
                    try
                    {
                        _linkRegistry.open();
                        try
                        {
                            dump = _linkRegistry.dump();
                        }
                        finally
                        {
                            _linkRegistry.close();
                        }
                    }
                    finally
                    {
                        _messageStore.closeMessageStore();
                    }
                }
                else if (getState() == State.ACTIVE)
                {
                    dump = _linkRegistry.dump();
                }
                else
                {
                    throw new IllegalStateException("The dumpLinkRegistry operation can only be called when the virtual host is active or stopped.");
                }
                return Futures.immediateFuture(dump);
            }

            @Override
            public String getObject()
            {
                return AbstractVirtualHost.this.toString();
            }

            @Override
            public String getAction()
            {
                return "dumpLinkRegistry";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        }));
    }

    @Override
    public void purgeLinkRegistry(final String containerIdPatternString, final String role, final String linkNamePatternString)
    {
        doSync(doOnConfigThread(new Task<ListenableFuture<Void>, IOException>()
        {
            @Override
            public ListenableFuture<Void> execute() throws IOException
            {
                if (getState() != State.STOPPED)
                {
                    throw new IllegalArgumentException(
                            "The purgeLinkRegistry operation can only be called when the virtual host is stopped.");
                }
                Pattern containerIdPattern = Pattern.compile(containerIdPatternString);
                Pattern linkNamePattern = Pattern.compile(linkNamePatternString);

                _messageStore.openMessageStore(AbstractVirtualHost.this);
                try
                {
                    _linkRegistry.open();
                    try
                    {
                        if ("SENDER".equals(role) || "BOTH".equals(role))
                        {
                            _linkRegistry.purgeSendingLinks(containerIdPattern, linkNamePattern);
                        }
                        if ("RECEIVER".equals(role) || "BOTH".equals(role))
                        {
                            _linkRegistry.purgeReceivingLinks(containerIdPattern, linkNamePattern);
                        }
                        return Futures.immediateFuture(null);
                    }
                    finally
                    {
                        _linkRegistry.close();
                    }
                }
                finally
                {
                    _messageStore.closeMessageStore();
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
                return "purgeLinkRegistry";
            }

            @Override
            public String getArguments()
            {
                return String.format("containerIdPattern='%s',role='%s',linkNamePattern='%s'",
                                     containerIdPatternString,
                                     role,
                                     linkNamePatternString);
            }
        }));
    }

    @Override
    public <K, V> Cache<K, V> getNamedCache(final String cacheName)
    {
        final String maxSizeContextVarName = String.format(NAMED_CACHE_MAXIMUM_SIZE_FORMAT, cacheName);
        final String expirationContextVarName = String.format(NAMED_CACHE_EXPIRATION_FORMAT, cacheName);
        Set<String> contextKeys = getContextKeys(false);
        int maxSize = contextKeys.contains(maxSizeContextVarName) ? getContextValue(Integer.class, maxSizeContextVarName) : getContextValue(Integer.class, NAMED_CACHE_MAXIMUM_SIZE);
        long expiration = contextKeys.contains(expirationContextVarName) ? getContextValue(Long.class, expirationContextVarName) : getContextValue(Long.class, NAMED_CACHE_EXPIRATION);

        return _caches.computeIfAbsent(cacheName, (k) -> CacheBuilder.<K, V>newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expiration, TimeUnit.MILLISECONDS)
                .build());
    }

    private boolean hasDifferentBindings(final Exchange<?> exchange,
                                         final Queue queue,
                                         final Map<String, Map<String,Object>> bindings)
    {
        for(String binding: bindings.keySet())
        {
            boolean theSameBindingFound = false;
            for (Binding publishingLink : exchange.getPublishingLinks(queue))
            {
                if (publishingLink.getBindingKey().equals(binding))
                {
                    Map<String, Object> expectedArguments = bindings.get(binding);
                    Map<String, Object> actualArguments = publishingLink.getArguments();


                    if (new HashMap<>(expectedArguments == null ? Collections.emptyMap() : expectedArguments).equals(new HashMap<>(actualArguments == null? Collections.emptyMap() : actualArguments)))
                    {
                        theSameBindingFound = true;
                    }

                }
            }
            if (!theSameBindingFound)
            {
                return true;
            }
        }
        return false;
    }

    private final class AccessControlProviderListener extends AbstractConfigurationChangeListener
    {
        private final Set<ConfiguredObject<?>> _bulkChanges = new HashSet<>();

        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if(object.getCategoryClass() == VirtualHost.class && child.getCategoryClass() == VirtualHostAccessControlProvider.class)
            {
                child.addChangeListener(this);
                AbstractVirtualHost.this.updateAccessControl();
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if(object.getCategoryClass() == VirtualHost.class && child.getCategoryClass() == VirtualHostAccessControlProvider.class)
            {
                AbstractVirtualHost.this.updateAccessControl();
            }
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            if(object.getCategoryClass() == VirtualHostAccessControlProvider.class && !_bulkChanges.contains(object))
            {
                AbstractVirtualHost.this.updateAccessControl();
            }
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
            if(object.getCategoryClass() == VirtualHostAccessControlProvider.class)
            {
                _bulkChanges.add(object);
            }
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            if(object.getCategoryClass() == VirtualHostAccessControlProvider.class)
            {
                _bulkChanges.remove(object);
                AbstractVirtualHost.this.updateAccessControl();
            }
        }
    }

    private class StoreEmptyCheckingHandler
            implements MessageHandler, MessageInstanceHandler, DistributedTransactionHandler
    {
        private boolean _empty = true;

        @Override
        public boolean handle(final StoredMessage<?> storedMessage)
        {
            _empty = false;
            return false;
        }

        @Override
        public boolean handle(final MessageEnqueueRecord record)
        {
            _empty = false;
            return false;
        }

        @Override
        public boolean handle(final org.apache.qpid.server.store.Transaction.StoredXidRecord storedXid,
                              final org.apache.qpid.server.store.Transaction.EnqueueRecord[] enqueues,
                              final org.apache.qpid.server.store.Transaction.DequeueRecord[] dequeues)
        {
            _empty = false;
            return false;
        }


        public boolean isEmpty()
        {
            return _empty;
        }

    }
}
