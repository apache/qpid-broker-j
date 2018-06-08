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
package org.apache.qpid.server.model;

import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.login.AccountNotFoundException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.BrokerPrincipal;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemAddressSpaceCreator;
import org.apache.qpid.server.plugin.SystemNodeCreator;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.CompoundAccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SubjectFixedResultAccessControl;
import org.apache.qpid.server.security.SubjectFixedResultAccessControl.ResultCalculator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.SimpleAuthenticationManager;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.stats.StatisticsReportingTask;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdaterImpl;
import org.apache.qpid.server.store.preferences.PreferencesRecoverer;
import org.apache.qpid.server.store.preferences.PreferencesRoot;
import org.apache.qpid.server.util.HousekeepingExecutor;
import org.apache.qpid.server.util.SystemUtils;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNodeCreator;

@ManagedObject( category = false, type = "Broker" )
public class BrokerImpl extends AbstractContainer<BrokerImpl> implements Broker<BrokerImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerImpl.class);

    private static final Pattern MODEL_VERSION_PATTERN = Pattern.compile("^\\d+\\.\\d+$");

    private static final int HOUSEKEEPING_SHUTDOWN_TIMEOUT = 5;

    public static final String MANAGEMENT_MODE_AUTHENTICATION = "MANAGEMENT_MODE_AUTHENTICATION";

    private final AccessControl _systemUserAllowed = new SubjectFixedResultAccessControl(new ResultCalculator()
    {
        @Override
        public Result getResult(final Subject subject)
        {
            return isSystemSubject(subject) ? Result.ALLOWED : Result.DEFER;
        }
    }, Result.DEFER);

    private final BrokerPrincipal _principal;

    private AuthenticationProvider<?> _managementModeAuthenticationProvider;

    private final AtomicLong _messagesIn = new AtomicLong();
    private final AtomicLong _messagesOut = new AtomicLong();
    private final AtomicLong _transactedMessagesIn = new AtomicLong();
    private final AtomicLong _transactedMessagesOut = new AtomicLong();
    private final AtomicLong _bytesIn = new AtomicLong();
    private final AtomicLong _bytesOut = new AtomicLong();
    private final AtomicLong _maximumMessageSize = new AtomicLong();

    @ManagedAttributeField
    private int _statisticsReportingPeriod;
    @ManagedAttributeField
    private boolean _messageCompressionEnabled;

    private PreferenceStore _preferenceStore;

    private final boolean _virtualHostPropertiesNodeEnabled;
    private Collection<BrokerLogger> _brokerLoggersToClose;
    private int _networkBufferSize = DEFAULT_NETWORK_BUFFER_SIZE;
    private final AddressSpaceRegistry _addressSpaceRegistry = new AddressSpaceRegistry();
    private ConfigurationChangeListener _accessControlProviderListener = new AccessControlProviderListener();
    private final AccessControl _accessControl;
    private TaskExecutor _preferenceTaskExecutor;
    private String _documentationUrl;
    private long _compactMemoryThreshold;
    private long _compactMemoryInterval;
    private long _flowToDiskThreshold;
    private double _sparsityFraction;
    private long _lastDisposalCounter;
    private ScheduledFuture<?> _assignTargetSizeSchedulingFuture;
    private volatile ScheduledFuture<?> _statisticsReportingFuture;
    private long _housekeepingCheckPeriod;

    @ManagedObjectFactoryConstructor
    public BrokerImpl(Map<String, Object> attributes,
                      SystemConfig parent)
    {
        super(attributes, parent);
        _principal = new BrokerPrincipal(this);

        if (parent.isManagementMode())
        {
            Map<String,Object> authManagerAttrs = new HashMap<String, Object>();
            authManagerAttrs.put(NAME,"MANAGEMENT_MODE_AUTHENTICATION");
            authManagerAttrs.put(ID, UUID.randomUUID());
            SimpleAuthenticationManager authManager = new SimpleAuthenticationManager(authManagerAttrs, this);
            authManager.addUser(SystemConfig.MANAGEMENT_MODE_USER_NAME, _parent.getManagementModePassword());
            _managementModeAuthenticationProvider = authManager;
            _accessControl = AccessControl.ALWAYS_ALLOWED;
        }
        else
        {
            _accessControl =  new CompoundAccessControl(Collections.<AccessControl<?>>emptyList(), Result.ALLOWED);
        }

        QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
        final Set<String> systemNodeCreatorTypes = qpidServiceLoader.getInstancesByType(SystemNodeCreator.class).keySet();
        _virtualHostPropertiesNodeEnabled = systemNodeCreatorTypes.contains(VirtualHostPropertiesNodeCreator.TYPE);
    }

    private void registerSystemAddressSpaces()
    {
        QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
        Iterable<SystemAddressSpaceCreator> factories = qpidServiceLoader.instancesOf(SystemAddressSpaceCreator.class);
        for(SystemAddressSpaceCreator creator : factories)
        {
            creator.register(_addressSpaceRegistry);
        }
    }


    @Override
    protected void postResolve()
    {
        super.postResolve();
        Integer networkBufferSize = getContextValue(Integer.class, NETWORK_BUFFER_SIZE);
        if (networkBufferSize == null || networkBufferSize < MINIMUM_NETWORK_BUFFER_SIZE)
        {
            throw new IllegalConfigurationException(NETWORK_BUFFER_SIZE + " is set to unacceptable value '" +
                    networkBufferSize + "'. Must be larger than " + MINIMUM_NETWORK_BUFFER_SIZE + ".");
        }
        _networkBufferSize = networkBufferSize;

        _sparsityFraction = getContextValue(Double.class, BROKER_DIRECT_BYTE_BUFFER_POOL_SPARSITY_REALLOCATION_FRACTION);
        int poolSize = getContextValue(Integer.class, BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE);

        QpidByteBuffer.initialisePool(_networkBufferSize, poolSize, _sparsityFraction);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        final SystemConfig parent = (SystemConfig) getParent();
        Runnable task =  parent.getOnContainerResolveTask();
        if(task != null)
        {
            task.run();
        }
        addChangeListener(_accessControlProviderListener);
        for(AccessControlProvider aclProvider : getChildren(AccessControlProvider.class))
        {
            aclProvider.addChangeListener(_accessControlProviderListener);
        }
        _eventLogger.message(BrokerMessages.CONFIG(parent instanceof FileBasedSettings
                                                           ? ((FileBasedSettings) parent).getStorePath()
                                                           : "N/A"));

    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        String modelVersion = (String) getActualAttributes().get(Broker.MODEL_VERSION);
        if (modelVersion == null)
        {
            deleteNoChecks();
            throw new IllegalConfigurationException(String.format("Broker %s must be specified", Broker.MODEL_VERSION));
        }

        if (!MODEL_VERSION_PATTERN.matcher(modelVersion).matches())
        {
            deleteNoChecks();
            throw new IllegalConfigurationException(String.format("Broker %s is specified in incorrect format: %s",
                                                                  Broker.MODEL_VERSION,
                                                                  modelVersion));
        }

        int versionSeparatorPosition = modelVersion.indexOf(".");
        String majorVersionPart = modelVersion.substring(0, versionSeparatorPosition);
        int majorModelVersion = Integer.parseInt(majorVersionPart);
        int minorModelVersion = Integer.parseInt(modelVersion.substring(versionSeparatorPosition + 1));

        if (majorModelVersion != BrokerModel.MODEL_MAJOR_VERSION || minorModelVersion > BrokerModel.MODEL_MINOR_VERSION)
        {
            deleteNoChecks();
            throw new IllegalConfigurationException(String.format(
                    "The model version '%s' in configuration is incompatible with the broker model version '%s'",
                    modelVersion,
                    BrokerModel.MODEL_VERSION));
        }

        if(!isDurable())
        {
            deleteNoChecks();
            throw new IllegalArgumentException(String.format("%s must be durable", getClass().getSimpleName()));
        }

    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        Broker updated = (Broker) proxyForValidation;
        if (changedAttributes.contains(MODEL_VERSION) && !BrokerModel.MODEL_VERSION.equals(updated.getModelVersion()))
        {
            throw new IllegalConfigurationException("Cannot change the model version");
        }

        if (changedAttributes.contains(CONTEXT))
        {
            @SuppressWarnings("unchecked")
            Map<String, String> context = (Map<String, String>) proxyForValidation.getAttribute(CONTEXT);
            if (context.containsKey(BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE))
            {
                String value = context.get(BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE);
                try
                {
                    DescendantScope.valueOf(value);
                }
                catch (Exception e)
                {
                    throw new IllegalConfigurationException(String.format(
                            "Unsupported value '%s' is specified for context variable '%s'. Please, change it to any of supported : %s",
                            value,
                            BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE,
                            EnumSet.allOf(DescendantScope.class)));
                }
            }
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
    protected void validateChildDelete(final ConfiguredObject<?> child)
    {
        super.validateChildDelete(child);
        if(child instanceof AccessControlProvider && getChildren(AccessControlProvider.class).size() == 1)
        {
            String categoryName = child.getCategoryClass().getSimpleName();
            throw new IllegalConfigurationException("The " + categoryName + " named '" + child.getName()
                                                    + "' cannot be deleted as at least one " + categoryName
                                                    + " must be present");
        }
    }

    @StateTransition( currentState = State.UNINITIALIZED, desiredState = State.ACTIVE )
    private ListenableFuture<Void> activate()
    {
        if(_parent.isManagementMode())
        {
            return doAfter(_managementModeAuthenticationProvider.openAsync(),
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            performActivation();
                        }
                    });
        }
        else
        {
            performActivation();
            return Futures.immediateFuture(null);
        }
    }

    @SuppressWarnings("unused")
    @StateTransition( currentState = {State.ACTIVE, State.ERRORED}, desiredState = State.STOPPED )
    private ListenableFuture<Void> doStop()
    {
        stopPreferenceTaskExecutor();
        closePreferenceStore();
        return Futures.immediateFuture(null);
    }

    private void closePreferenceStore()
    {
        PreferenceStore ps = _preferenceStore;
        if (ps != null)
        {
            ps.close();
        }
    }

    private void stopPreferenceTaskExecutor()
    {
        TaskExecutor preferenceTaskExecutor = _preferenceTaskExecutor;
        if (preferenceTaskExecutor != null)
        {
            preferenceTaskExecutor.stop();
        }
    }

    @Override
    public void initiateShutdown()
    {
        getEventLogger().message(BrokerMessages.OPERATION("initiateShutdown"));
        _parent.closeAsync();
    }

    @Override
    public Map<String, Object> extractConfig(final boolean includeSecureAttributes)
    {
        return (new ConfigurationExtractor()).extractConfig(this, includeSecureAttributes);
    }

    private void performActivation()
    {
        final DescendantScope descendantScope = getContextValue(DescendantScope.class,
                                                                BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE);
        List<ConfiguredObject<?>> failedChildren = getChildrenInState(this, State.ERRORED, descendantScope);

        if (!failedChildren.isEmpty())
        {
            for (ConfiguredObject<?> o : failedChildren)
            {
                LOGGER.warn("{} child object '{}' of type '{}' is {}",
                            o.getParent().getCategoryClass().getSimpleName(),
                            o.getName(),
                            o.getClass().getSimpleName(),
                            State.ERRORED);
            }
            getEventLogger().message(BrokerMessages.FAILED_CHILDREN(failedChildren.toString()));
        }

        _documentationUrl = getContextValue(String.class, QPID_DOCUMENTATION_URL);
        final boolean brokerShutdownOnErroredChild = getContextValue(Boolean.class,
                                                                     BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD);
        if (!_parent.isManagementMode() && brokerShutdownOnErroredChild && !failedChildren.isEmpty())
        {
            throw new IllegalStateException(String.format(
                    "Broker context variable %s is set and the broker has %s children",
                    BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD,
                    State.ERRORED));
        }
        updateAccessControl();

        _houseKeepingTaskExecutor = new HousekeepingExecutor("broker-" + getName() + "-pool",
                                                             getHousekeepingThreadCount(),
                                                             getSystemTaskSubject("Housekeeping", _principal));
        initialiseStatisticsReporting();

        scheduleDirectMemoryCheck();
        _assignTargetSizeSchedulingFuture = scheduleHouseKeepingTask(getHousekeepingCheckPeriod(),
                                                                     TimeUnit.MILLISECONDS,
                                                                     this::assignTargetSizes);

        final PreferenceStoreUpdaterImpl updater = new PreferenceStoreUpdaterImpl();
        final Collection<PreferenceRecord> preferenceRecords = _preferenceStore.openAndLoad(updater);
        _preferenceTaskExecutor = new TaskExecutorImpl("broker-" + getName() + "-preferences", null);
        _preferenceTaskExecutor.start();
        PreferencesRecoverer preferencesRecoverer = new PreferencesRecoverer(_preferenceTaskExecutor);
        preferencesRecoverer.recoverPreferences(this, preferenceRecords, _preferenceStore);

        if (isManagementMode())
        {
            _eventLogger.message(BrokerMessages.MANAGEMENT_MODE(SystemConfig.MANAGEMENT_MODE_USER_NAME,
                                                                _parent.getManagementModePassword()));
        }
        setState(State.ACTIVE);
    }

    private List<ConfiguredObject<?>> getChildrenInState(final ConfiguredObject<?> configuredObject,
                                                         final State state,
                                                         final DescendantScope descendantScope)
    {
        List<ConfiguredObject<?>> foundChildren = new ArrayList<>();
        Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
        for (final Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(categoryClass))
        {
            final Collection<? extends ConfiguredObject> children = configuredObject.getChildren(childClass);
            for (final ConfiguredObject<?> child : children)
            {
                if (child.getState() == state)
                {
                    foundChildren.add(child);
                }
                if (descendantScope == DescendantScope.ALL)
                {
                    foundChildren.addAll(getChildrenInState(child, state, descendantScope));
                }
            }
        }
        return foundChildren;
    }

    private void checkDirectMemoryUsage()
    {
        if (_compactMemoryThreshold >= 0
            && QpidByteBuffer.getAllocatedDirectMemorySize() > _compactMemoryThreshold
            && _lastDisposalCounter != QpidByteBuffer.getPooledBufferDisposalCounter())
        {

            _lastDisposalCounter = QpidByteBuffer.getPooledBufferDisposalCounter();

            ListenableFuture<Void> result = compactMemoryInternal();
            addFutureCallback(result, new FutureCallback<Void>()
            {
                @Override
                public void onSuccess(final Void result)
                {
                    scheduleDirectMemoryCheck();
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    scheduleDirectMemoryCheck();
                }
            }, MoreExecutors.directExecutor());
        }
        else
        {
            scheduleDirectMemoryCheck();
        }
    }

    private void scheduleDirectMemoryCheck()
    {
        if (_compactMemoryInterval > 0)
        {
            try
            {
                _houseKeepingTaskExecutor.schedule(this::checkDirectMemoryUsage,
                                                   _compactMemoryInterval,
                                                   TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException e)
            {
                if (!_houseKeepingTaskExecutor.isShutdown())
                {
                    LOGGER.warn("Failed to schedule direct memory check", e);
                }
            }
        }
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
            _statisticsReportingFuture = _houseKeepingTaskExecutor.scheduleAtFixedRate(new StatisticsReportingTask(this, getSystemTaskSubject("Statistics")), report, report, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public int getStatisticsReportingPeriod()
    {
        return _statisticsReportingPeriod;
    }

    @Override
    public boolean isMessageCompressionEnabled()
    {
        return _messageCompressionEnabled;
    }

    @Override
    public Collection<VirtualHostNode<?>> getVirtualHostNodes()
    {
        Collection children = getChildren(VirtualHostNode.class);
        return children;
    }

    @Override
    public Collection<Port<?>> getPorts()
    {
        Collection children = getChildren(Port.class);
        return children;
    }

    @Override
    public Collection<AuthenticationProvider<?>> getAuthenticationProviders()
    {
        Collection children = getChildren(AuthenticationProvider.class);
        return children;
    }

    @Override
    public synchronized void assignTargetSizes()
    {
        LOGGER.debug("Assigning target sizes based on total target {}", _flowToDiskThreshold);
        long totalSize = 0l;
        Collection<VirtualHostNode<?>> vhns = getVirtualHostNodes();
        Map<QueueManagingVirtualHost<?>, Long> vhs = new HashMap<>();
        for (VirtualHostNode<?> vhn : vhns)
        {
            VirtualHost<?> vh = vhn.getVirtualHost();
            if (vh instanceof QueueManagingVirtualHost)
            {
                QueueManagingVirtualHost<?> host = (QueueManagingVirtualHost<?>)vh;
                long totalQueueDepthBytes = host.getTotalDepthOfQueuesBytes();
                vhs.put(host, totalQueueDepthBytes);
                totalSize += totalQueueDepthBytes;
            }
        }

        final long proportionalShare = (long) ((double) _flowToDiskThreshold / (double) vhs.size());
        for (Map.Entry<QueueManagingVirtualHost<?>, Long> entry : vhs.entrySet())
        {
            long virtualHostTotalQueueSize = entry.getValue();
            final long size;
            if (totalSize == 0)
            {
                size = proportionalShare;
            }
            else
            {
                double fraction = ((double)virtualHostTotalQueueSize)/((double)totalSize);
                double queueSizeBasedShare = ((double)_flowToDiskThreshold)/ 2.0 * fraction;
                size = (long)(queueSizeBasedShare + ((double)proportionalShare) / 2.0);
            }

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Assigning target size {} to vhost {}", size, entry.getKey());
            }
            entry.getKey().setTargetSize(size);
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        PreferencesRoot preferencesRoot = (SystemConfig) getParent();
        _preferenceStore = preferencesRoot.createPreferenceStore();

        getEventLogger().message(BrokerMessages.STARTUP(CommonProperties.getReleaseVersion(),
                                                        CommonProperties.getBuildVersion()));

        getEventLogger().message(BrokerMessages.PLATFORM(System.getProperty("java.vendor"),
                                                         System.getProperty("java.runtime.version",
                                                                            System.getProperty("java.version")),
                                                         SystemUtils.getOSName(),
                                                         SystemUtils.getOSVersion(),
                                                         SystemUtils.getOSArch(),
                                                         String.valueOf(getNumberOfCores())));

        long directMemory = getMaxDirectMemorySize();
        long heapMemory = Runtime.getRuntime().maxMemory();
        getEventLogger().message(BrokerMessages.MAX_MEMORY(heapMemory, directMemory));

        _flowToDiskThreshold = getContextValue(Long.class, BROKER_FLOW_TO_DISK_THRESHOLD);
        _compactMemoryThreshold = getContextValue(Long.class, Broker.COMPACT_MEMORY_THRESHOLD);
        _compactMemoryInterval = getContextValue(Long.class, Broker.COMPACT_MEMORY_INTERVAL);
        _housekeepingCheckPeriod = getContextValue(Long.class, Broker.QPID_BROKER_HOUSEKEEPING_CHECK_PERIOD);

        if (SystemUtils.getProcessPid() != null)
        {
            getEventLogger().message(BrokerMessages.PROCESS(SystemUtils.getProcessPid()));
        }

        registerSystemAddressSpaces();

        assignTargetSizes();
    }

    @Override
    public NamedAddressSpace getSystemAddressSpace(String name)
    {
        return _addressSpaceRegistry.getAddressSpace(name);
    }

    @Override
    public Collection<GroupProvider<?>> getGroupProviders()
    {
        Collection children = getChildren(GroupProvider.class);
        return children;
    }

    private ListenableFuture<VirtualHostNode> createVirtualHostNodeAsync(Map<String, Object> attributes)
            throws AccessControlException, IllegalArgumentException
    {

        return doAfter(getObjectFactory().createAsync(VirtualHostNode.class, attributes, this),
                       new CallableWithArgument<ListenableFuture<VirtualHostNode>, VirtualHostNode>()
                       {
                           @Override
                           public ListenableFuture<VirtualHostNode> call(final VirtualHostNode virtualHostNode)
                                   throws Exception
                           {
                               // permission has already been granted to create the virtual host
                               // disable further access check on other operations, e.g. create exchange
                               Subject.doAs(getSubjectWithAddedSystemRights(),
                                            new PrivilegedAction<Object>()
                                            {
                                                @Override
                                                public Object run()
                                                {
                                                    virtualHostNode.start();
                                                    return null;
                                                }
                                            });
                               return Futures.immediateFuture(virtualHostNode);
                           }
                       });
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                          final Map<String, Object> attributes)
    {
        if (childClass == VirtualHostNode.class)
        {
            return (ListenableFuture<C>) createVirtualHostNodeAsync(attributes);
        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _brokerLoggersToClose = new ArrayList(getChildren(BrokerLogger.class));
        return super.beforeClose();
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        if (_assignTargetSizeSchedulingFuture != null)
        {
            _assignTargetSizeSchedulingFuture.cancel(true);
        }

        shutdownHouseKeeping();

        stopPreferenceTaskExecutor();
        closePreferenceStore();

        _eventLogger.message(BrokerMessages.STOPPED());

        try
        {
            for (BrokerLogger<?> logger : _brokerLoggersToClose)
            {
                logger.stopLogging();
            }
        }
        finally
        {
            Runnable task = _parent.getOnContainerCloseTask();
            if(task != null)
            {
                task.run();
            }
        }
        return Futures.immediateFuture(null);

    }

    @Override
    public UserPreferences createUserPreferences(final ConfiguredObject<?> object)
    {
        return new UserPreferencesImpl(_preferenceTaskExecutor, object, _preferenceStore, Collections.<Preference>emptySet());
    }

    private void updateAccessControl()
    {
        if(!isManagementMode())
        {
            List<AccessControlProvider> children = new ArrayList<>(getChildren(AccessControlProvider.class));
            Collections.sort(children, CommonAccessControlProvider.ACCESS_CONTROL_PROVIDER_COMPARATOR);

            List<AccessControl<?>> accessControls = new ArrayList<>(children.size()+1);
            accessControls.add(_systemUserAllowed);
            for(AccessControlProvider prov : children)
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

            ((CompoundAccessControl)_accessControl).setAccessControls(accessControls);

        }
    }

    @Override
    public AccessControl getAccessControl()
    {
        return _accessControl;
    }

    @Override
    public VirtualHost<?> findVirtualHostByName(String name)
    {
        for (VirtualHostNode<?> virtualHostNode : getChildren(VirtualHostNode.class))
        {
            VirtualHost<?> virtualHost = virtualHostNode.getVirtualHost();
            if (virtualHost != null && virtualHost.getName().equals(name))
            {
                return virtualHost;
            }
        }
        return null;
    }

    @Override
    public VirtualHostNode findDefautVirtualHostNode()
    {
        VirtualHostNode existingDefault = null;
        Collection<VirtualHostNode<?>> virtualHostNodes = new ArrayList<>(getVirtualHostNodes());
        for(VirtualHostNode node : virtualHostNodes)
        {
            if (node.isDefaultVirtualHostNode())
            {
                existingDefault = node;
                break;
            }
        }
        return existingDefault;
    }

    @Override
    public Collection<KeyStore<?>> getKeyStores()
    {
        Collection children = getChildren(KeyStore.class);
        return children;
    }

    @Override
    public Collection<TrustStore<?>> getTrustStores()
    {
        Collection children = getChildren(TrustStore.class);
        return children;
    }

    @Override
    public boolean isManagementMode()
    {
        return _parent.isManagementMode();
    }

    @Override
    public Collection<AccessControlProvider<?>> getAccessControlProviders()
    {
        Collection children = getChildren(AccessControlProvider.class);
        return children;
    }

    @Override
    protected void onExceptionInOpen(RuntimeException e)
    {
        _eventLogger.message(BrokerMessages.FATAL_ERROR(e.getMessage()));
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(BrokerMessages.OPERATION(operation));
    }

    @Override
    public void registerMessageDelivered(long messageSize)
    {
        _messagesOut.incrementAndGet();
        _bytesOut.addAndGet(messageSize);
    }

    @Override
    public void registerTransactedMessageReceived()
    {
        _transactedMessagesIn.incrementAndGet();
    }

    @Override
    public void registerTransactedMessageDelivered()
    {
        _transactedMessagesOut.incrementAndGet();
    }

    @Override
    public void registerMessageReceived(long messageSize)
    {
        _messagesIn.incrementAndGet();
        _bytesIn.addAndGet(messageSize);
        long hwm;
        while((hwm = _maximumMessageSize.get()) < messageSize)
        {
            _maximumMessageSize.compareAndSet(hwm, messageSize);
        }
    }



    @Override
    public long getFlowToDiskThreshold()
    {
        return _flowToDiskThreshold;
    }

    @Override
    public long getNumberOfBuffersInUse()
    {
        return QpidByteBuffer.getNumberOfBuffersInUse();
    }

    @Override
    public long getNumberOfBuffersInPool()
    {
        return QpidByteBuffer.getNumberOfBuffersInPool();
    }

    @Override
    public long getInboundMessageSizeHighWatermark()
    {
        return _maximumMessageSize.get();
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
    public boolean isVirtualHostPropertiesNodeEnabled()
    {
        return _virtualHostPropertiesNodeEnabled;
    }

    @Override
    public AuthenticationProvider<?> getManagementModeAuthenticationProvider()
    {
        return _managementModeAuthenticationProvider;
    }

    @Override
    public int getNetworkBufferSize()
    {
        return _networkBufferSize;
    }

    @Override
    public String getDocumentationUrl()
    {
        return _documentationUrl;
    }

    @Override
    public void restart()
    {
        Subject.doAs(getSystemTaskSubject("Broker"), new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                final SystemConfig<?> systemConfig = (SystemConfig) getParent();
                // This is deliberately asynchronous as the HTTP thread will be interrupted by restarting
                doAfter(systemConfig.setAttributesAsync(Collections.<String,Object>singletonMap(ConfiguredObject.DESIRED_STATE,
                                                                                                State.STOPPED)),
                        new Callable<ListenableFuture<Void>>()
                        {
                            @Override
                            public ListenableFuture<Void> call() throws Exception
                            {
                                return systemConfig.setAttributesAsync(Collections.<String,Object>singletonMap(ConfiguredObject.DESIRED_STATE, State.ACTIVE));
                            }
                        });

                return null;
            }
        });

    }

    @Override
    public Principal getUser()
    {
        return AuthenticatedPrincipal.getCurrentUser();
    }

    @Override
    public SocketConnectionMetaData getConnectionMetaData()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        final SocketConnectionPrincipal principal;
        if(subject != null)
        {
            Set<SocketConnectionPrincipal> principals = subject.getPrincipals(SocketConnectionPrincipal.class);
            if(!principals.isEmpty())
            {
                principal = principals.iterator().next();
            }
            else
            {
                principal = null;
            }
        }
        else
        {
            principal = null;
        }
        return principal == null ? null : principal.getConnectionMetaData();
    }

    @Override
    public Set<Principal> getGroups()
    {
        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        if (currentSubject == null)
        {
            return Collections.emptySet();
        }

        final Set<Principal> currentPrincipals = Collections.<Principal>unmodifiableSet(currentSubject.getPrincipals(GroupPrincipal.class));
        return currentPrincipals;
    }

    @Override
    public void purgeUser(final AuthenticationProvider<?> origin, final String username)
    {
        doPurgeUser(origin, username);
    }

    private void doPurgeUser(final AuthenticationProvider<?> origin, final String username)
    {
        // remove from AuthenticationProvider
        if (origin instanceof PasswordCredentialManagingAuthenticationProvider)
        {
            try
            {
                ((PasswordCredentialManagingAuthenticationProvider) origin).deleteUser(username);
            }
            catch (AccountNotFoundException e)
            {
                // pass
            }
        }

        // remove from Groups
        final Collection<GroupProvider> groupProviders = getChildren(GroupProvider.class);
        for (GroupProvider<?> groupProvider : groupProviders)
        {
            final Collection<Group> groups = groupProvider.getChildren(Group.class);
            for (Group<?> group : groups)
            {
                final Collection<GroupMember> members = group.getChildren(GroupMember.class);
                for (GroupMember<?> member : members)
                {
                    if (username.equals(member.getName()))
                    {
                        member.delete();
                    }
                }
            }
        }

        // remove Preferences from all ConfiguredObjects
        Subject userSubject = new Subject(true,
                                          Collections.singleton(new AuthenticatedPrincipal(new UsernamePrincipal(username, origin))),
                                          Collections.EMPTY_SET,
                                          Collections.EMPTY_SET);
        java.util.Queue<ConfiguredObject<?>> configuredObjects = new LinkedList<>();
        configuredObjects.add(BrokerImpl.this);
        while (!configuredObjects.isEmpty())
        {
            final ConfiguredObject<?> currentObject = configuredObjects.poll();
            final Collection<Class<? extends ConfiguredObject>> childClasses = getModel().getChildTypes(currentObject.getClass());
            for (Class<? extends ConfiguredObject> childClass : childClasses)
            {
                final Collection<? extends ConfiguredObject> children = currentObject.getChildren(childClass);
                for (ConfiguredObject child : children)
                {
                    configuredObjects.add(child);
                }
            }

            Subject.doAs(userSubject, new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    currentObject.getUserPreferences().delete(null, null, null);
                    return null;
                }
            });
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
        }
    }

    @Override
    public long getCompactMemoryThreshold()
    {
        return _compactMemoryThreshold;
    }

    @Override
    public long getCompactMemoryInterval()
    {
        return _compactMemoryInterval;
    }

    @Override
    public double getSparsityFraction()
    {
        return _sparsityFraction;
    }

    @Override
    public long getHousekeepingCheckPeriod()
    {
        return _housekeepingCheckPeriod;
    }

    @Override
    public void compactMemory()
    {
        compactMemoryInternal();
    }

    private ListenableFuture<Void> compactMemoryInternal()
    {
        LOGGER.debug("Compacting direct memory buffers: numberOfActivePooledBuffers: {}",
                     QpidByteBuffer.getNumberOfBuffersInUse());

        final Collection<VirtualHostNode<?>> vhns = getVirtualHostNodes();
        List<ListenableFuture<Void>> futures = new ArrayList<>(vhns.size());
        for (VirtualHostNode<?> vhn : vhns)
        {
            VirtualHost<?> vh = vhn.getVirtualHost();
            if (vh instanceof QueueManagingVirtualHost)
            {
                ListenableFuture<Void> future = ((QueueManagingVirtualHost) vh).reallocateMessages();
                futures.add(future);
            }
        }

        SettableFuture<Void> resultFuture = SettableFuture.create();
        final ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(futures);
        addFutureCallback(combinedFuture, new FutureCallback<List<Void>>()
        {
            @Override
            public void onSuccess(final List<Void> result)
            {
                if (LOGGER.isDebugEnabled())
                {
                   LOGGER.debug("After compact direct memory buffers: numberOfActivePooledBuffers: {}",
                                QpidByteBuffer.getNumberOfBuffersInUse());
                }
                resultFuture.set(null);
            }

            @Override
            public void onFailure(final Throwable t)
            {
                LOGGER.warn("Unexpected error during direct memory compaction.", t);
                resultFuture.setException(t);
            }
        }, _houseKeepingTaskExecutor);
        return resultFuture;
    }

    private class AddressSpaceRegistry implements SystemAddressSpaceCreator.AddressSpaceRegistry
    {
        private final ConcurrentMap<String, NamedAddressSpace> _systemAddressSpaces = new ConcurrentHashMap<>();

        @Override
        public void registerAddressSpace(final NamedAddressSpace addressSpace)
        {
            _systemAddressSpaces.put(addressSpace.getName(), addressSpace);
        }

        @Override
        public void removeAddressSpace(final NamedAddressSpace addressSpace)
        {
            _systemAddressSpaces.remove(addressSpace.getName(), addressSpace);
        }

        @Override
        public void removeAddressSpace(final String name)
        {
            _systemAddressSpaces.remove(name);
        }

        @Override
        public NamedAddressSpace getAddressSpace(final String name)
        {
            return name == null ? null : _systemAddressSpaces.get(name);
        }

        @Override
        public Broker<?> getBroker()
        {
            return BrokerImpl.this;
        }
    }


    private final class AccessControlProviderListener extends AbstractConfigurationChangeListener
    {
        private final Set<ConfiguredObject<?>> _bulkChanges = new HashSet<>();


        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if(object.getCategoryClass() == Broker.class && child.getCategoryClass() == AccessControlProvider.class)
            {
                child.addChangeListener(this);
                BrokerImpl.this.updateAccessControl();
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if(object.getCategoryClass() == Broker.class && child.getCategoryClass() == AccessControlProvider.class)
            {
                BrokerImpl.this.updateAccessControl();
            }
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            if(object.getCategoryClass() == AccessControlProvider.class && !_bulkChanges.contains(object))
            {
                BrokerImpl.this.updateAccessControl();
            }
        }

        @Override
        public void bulkChangeStart(final ConfiguredObject<?> object)
        {
            if(object.getCategoryClass() == AccessControlProvider.class)
            {
                _bulkChanges.add(object);
            }
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            if(object.getCategoryClass() == AccessControlProvider.class)
            {
                _bulkChanges.remove(object);
                BrokerImpl.this.updateAccessControl();
            }
        }
    }

}
