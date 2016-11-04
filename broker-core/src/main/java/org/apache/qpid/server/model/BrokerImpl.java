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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.login.AccountNotFoundException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.BrokerPrincipal;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.logging.messages.VirtualHostMessages;
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
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.SimpleAuthenticationManager;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.stats.StatisticsCounter;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdaterImpl;
import org.apache.qpid.server.store.preferences.PreferencesRecoverer;
import org.apache.qpid.server.store.preferences.PreferencesRoot;
import org.apache.qpid.server.util.HousekeepingExecutor;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNodeCreator;
import org.apache.qpid.util.SystemUtils;

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

    private String[] POSITIVE_NUMERIC_ATTRIBUTES = { CONNECTION_SESSION_COUNT_LIMIT,
            CONNECTION_HEART_BEAT_DELAY, STATISTICS_REPORTING_PERIOD };


    private AuthenticationProvider<?> _managementModeAuthenticationProvider;

    private Timer _reportingTimer;
    private final StatisticsCounter _messagesDelivered, _dataDelivered, _messagesReceived, _dataReceived;

    /** Flags used to control the reporting of flow to disk. Protected by this */
    private boolean _totalMessageSizeExceedThresholdReported = false,  _totalMessageSizeWithinThresholdReported = true;

    @ManagedAttributeField
    private int _connection_sessionCountLimit;
    @ManagedAttributeField
    private int _connection_heartBeatDelay;
    @ManagedAttributeField
    private boolean _connection_closeWhenNoRoute;
    @ManagedAttributeField
    private int _statisticsReportingPeriod;
    @ManagedAttributeField
    private boolean _statisticsReportingResetEnabled;
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

    @ManagedObjectFactoryConstructor
    public BrokerImpl(Map<String, Object> attributes,
                      SystemConfig parent)
    {
        super(parentsMap(parent), attributes, parent);
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
        _messagesDelivered = new StatisticsCounter("messages-delivered");
        _dataDelivered = new StatisticsCounter("bytes-delivered");
        _messagesReceived = new StatisticsCounter("messages-received");
        _dataReceived = new StatisticsCounter("bytes-received");


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

        int poolSize = getContextValue(Integer.class, BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE);

        QpidByteBuffer.initialisePool(_networkBufferSize, poolSize);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        final SystemConfig parent = getParent(SystemConfig.class);
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
            deleted();
            throw new IllegalConfigurationException("Broker " + Broker.MODEL_VERSION + " must be specified");
        }

        if (!MODEL_VERSION_PATTERN.matcher(modelVersion).matches())
        {
            deleted();
            throw new IllegalConfigurationException("Broker " + Broker.MODEL_VERSION + " is specified in incorrect format: "
                                                    + modelVersion);
        }

        int versionSeparatorPosition = modelVersion.indexOf(".");
        String majorVersionPart = modelVersion.substring(0, versionSeparatorPosition);
        int majorModelVersion = Integer.parseInt(majorVersionPart);
        int minorModelVersion = Integer.parseInt(modelVersion.substring(versionSeparatorPosition + 1));

        if (majorModelVersion != BrokerModel.MODEL_MAJOR_VERSION || minorModelVersion > BrokerModel.MODEL_MINOR_VERSION)
        {
            deleted();
            throw new IllegalConfigurationException("The model version '" + modelVersion
                                                    + "' in configuration is incompatible with the broker model version '" + BrokerModel.MODEL_VERSION + "'");
        }

        if(!isDurable())
        {
            deleted();
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
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

        for (String attributeName : POSITIVE_NUMERIC_ATTRIBUTES)
        {
            if(changedAttributes.contains(attributeName))
            {
                Number value = (Number) updated.getAttribute(attributeName);

                if (value != null && value.longValue() < 0)
                {
                    throw new IllegalConfigurationException(
                            "Only positive integer value can be specified for the attribute "
                            + attributeName);
                }
            }
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
        boolean hasBrokerAnyErroredChildren = false;

        List<ConfiguredObject<?>> failedChildren = new ArrayList<>();
        for (final Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(getCategoryClass()))
        {
            final Collection<? extends ConfiguredObject> children = getChildren(childClass);
            if (children != null) {
                for (final ConfiguredObject<?> child : children)
                {
                    if (child.getState() == State.ERRORED )
                    {
                        hasBrokerAnyErroredChildren = true;
                        LOGGER.warn("Broker child object '{}' of type '{}' is {}",
                                child.getName(), childClass.getSimpleName(), State.ERRORED);
                        failedChildren.add(child);
                    }
                }
            }
        }

        if(!failedChildren.isEmpty())
        {
            getEventLogger().message(BrokerMessages.FAILED_CHILDREN(failedChildren.toString()));
        }

        _documentationUrl = getContextValue(String.class, QPID_DOCUMENTATION_URL);
        final boolean brokerShutdownOnErroredChild = getContextValue(Boolean.class,
                                                                     BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD);
        if (!_parent.isManagementMode() && brokerShutdownOnErroredChild && hasBrokerAnyErroredChildren)
        {
            throw new IllegalStateException(String.format("Broker context variable %s is set and the broker has %s children",
                    BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD, State.ERRORED));
        }
        updateAccessControl();

        initialiseStatisticsReporting();

        _houseKeepingTaskExecutor = new HousekeepingExecutor("broker-" + getName() + "-pool",
                                                             getHousekeepingThreadCount(),
                                                             getSystemTaskSubject("Housekeeping", _principal));

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

    private void initialiseStatisticsReporting()
    {
        long report = ((Number)getAttribute(Broker.STATISTICS_REPORTING_PERIOD)).intValue() * 1000; // convert to ms
        final boolean reset = (Boolean)getAttribute(Broker.STATISTICS_REPORTING_RESET_ENABLED);

        /* add a timer task to report statistics if generation is enabled for broker or virtualhosts */
        if (report > 0L)
        {
            _reportingTimer = new Timer("Statistics-Reporting", true);
            StatisticsReportingTask task = new StatisticsReportingTask(reset, _eventLogger);
            _reportingTimer.scheduleAtFixedRate(task, report / 2, report);
        }
    }

    @Override
    public int getConnection_sessionCountLimit()
    {
        return _connection_sessionCountLimit;
    }

    @Override
    public int getConnection_heartBeatDelay()
    {
        return _connection_heartBeatDelay;
    }

    @Override
    public boolean getConnection_closeWhenNoRoute()
    {
        return _connection_closeWhenNoRoute;
    }

    @Override
    public int getStatisticsReportingPeriod()
    {
        return _statisticsReportingPeriod;
    }

    @Override
    public boolean getStatisticsReportingResetEnabled()
    {
        return _statisticsReportingResetEnabled;
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

    public Collection<Port<?>> getPorts()
    {
        Collection children = getChildren(Port.class);
        return children;
    }

    public Collection<AuthenticationProvider<?>> getAuthenticationProviders()
    {
        Collection children = getChildren(AuthenticationProvider.class);
        return children;
    }

    @Override
    public synchronized void assignTargetSizes()
    {
        long totalTarget = getContextValue(Long.class, BROKER_FLOW_TO_DISK_THRESHOLD);
        LOGGER.debug("Assigning target sizes based on total target {}", totalTarget);
        long totalSize = 0l;
        Collection<VirtualHostNode<?>> vhns = getVirtualHostNodes();
        Map<VirtualHost<?>, Long> vhs = new HashMap<>();
        for (VirtualHostNode<?> vhn : vhns)
        {
            VirtualHost<?> vh = vhn.getVirtualHost();
            if (vh != null)
            {
                long totalQueueDepthBytes = vh.getTotalQueueDepthBytes();
                vhs.put(vh, totalQueueDepthBytes);
                totalSize += totalQueueDepthBytes;
            }
        }

        if (totalSize > totalTarget && !_totalMessageSizeExceedThresholdReported)
        {
            _eventLogger.message(BrokerMessages.FLOW_TO_DISK_ACTIVE(totalSize / 1024, totalTarget / 1024));
            _totalMessageSizeExceedThresholdReported = true;
            _totalMessageSizeWithinThresholdReported = false;
        }
        else if (totalSize <= totalTarget && !_totalMessageSizeWithinThresholdReported)
        {
            _eventLogger.message(BrokerMessages.FLOW_TO_DISK_INACTIVE(totalSize / 1024, totalTarget / 1024));
            _totalMessageSizeWithinThresholdReported = true;
            _totalMessageSizeExceedThresholdReported = false;
        }

        final long proportionalShare = (long) ((double) totalTarget / (double) vhs.size());
        for (Map.Entry<VirtualHost<?>, Long> entry : vhs.entrySet())
        {
            long virtualHostTotalQueueSize = entry.getValue();
            final long size;
            if (totalSize == 0)
            {
                size = proportionalShare;
            }
            else
            {
                long queueSizeBasedShare = (totalTarget * virtualHostTotalQueueSize) / (2 * totalSize);
                size = queueSizeBasedShare + (proportionalShare / 2);
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

        PreferencesRoot preferencesRoot = getParent(SystemConfig.class);
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

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass, final Map<String, Object> attributes, final ConfiguredObject... otherParents)
    {
        if (childClass == VirtualHostNode.class)
        {
            return (ListenableFuture<C>) createVirtualHostNodeAsync(attributes);
        }
        else
        {
            return getObjectFactory().createAsync(childClass, attributes, this);
        }


    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _brokerLoggersToClose = new ArrayList(getChildren(BrokerLogger.class));
        return super.beforeClose();
    }

    @Override
    protected void onClose()
    {
        if (_reportingTimer != null)
        {
            _reportingTimer.cancel();
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
                    accessControls.add(prov.getAccessControl());
                }

            }

            ((CompoundAccessControl)_accessControl).setAccessControls(accessControls);

        }
    }

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

    public void registerMessageDelivered(long messageSize)
    {
        _messagesDelivered.registerEvent(1L);
        _dataDelivered.registerEvent(messageSize);
    }

    public void registerMessageReceived(long messageSize, long timestamp)
    {
        _messagesReceived.registerEvent(1L, timestamp);
        _dataReceived.registerEvent(messageSize, timestamp);
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

    @Override
    public void resetStatistics()
    {
        _messagesDelivered.reset();
        _dataDelivered.reset();
        _messagesReceived.reset();
        _dataReceived.reset();

        for (VirtualHostNode<?> virtualHostNode : getChildren(VirtualHostNode.class))
        {
            VirtualHost<?> virtualHost = virtualHostNode.getVirtualHost();
            if (virtualHost instanceof StatisticsGatherer)
            {
                ((StatisticsGatherer)virtualHost).resetStatistics();
            }
        }
    }

    private class StatisticsReportingTask extends TimerTask
    {
        private final int DELIVERED = 0;
        private final int RECEIVED = 1;

        private final boolean _reset;
        private final EventLogger _logger;
        private final Subject _subject;

        public StatisticsReportingTask(boolean reset, EventLogger logger)
        {
            _reset = reset;
            _logger = logger;
            _subject = getSystemTaskSubject("Statistics");
        }

        public void run()
        {
            Subject.doAs(_subject, new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    reportStatistics();
                    return null;
                }
            });
        }

        protected void reportStatistics()
        {
            try
            {
                _eventLogger.message(BrokerMessages.STATS_DATA(DELIVERED, _dataDelivered.getPeak() / 1024.0, _dataDelivered.getTotal()));
                _eventLogger.message(BrokerMessages.STATS_MSGS(DELIVERED, _messagesDelivered.getPeak(), _messagesDelivered.getTotal()));
                _eventLogger.message(BrokerMessages.STATS_DATA(RECEIVED, _dataReceived.getPeak() / 1024.0, _dataReceived.getTotal()));
                _eventLogger.message(BrokerMessages.STATS_MSGS(RECEIVED,
                                                               _messagesReceived.getPeak(),
                                                               _messagesReceived.getTotal()));

                for (VirtualHostNode<?> virtualHostNode : getChildren(VirtualHostNode.class))
                {
                    VirtualHost<?> virtualHost = virtualHostNode.getVirtualHost();
                    if (virtualHost instanceof StatisticsGatherer)
                    {
                        StatisticsGatherer statGatherer = (StatisticsGatherer) virtualHost;
                        String name = virtualHost.getName();
                        StatisticsCounter dataDelivered = statGatherer.getDataDeliveryStatistics();
                        StatisticsCounter messagesDelivered = statGatherer.getMessageDeliveryStatistics();
                        StatisticsCounter dataReceived = statGatherer.getDataReceiptStatistics();
                        StatisticsCounter messagesReceived = statGatherer.getMessageReceiptStatistics();
                        EventLogger logger = virtualHost.getEventLogger();
                        logger.message(VirtualHostMessages.STATS_DATA(name,
                                                                      DELIVERED,
                                                                      dataDelivered.getPeak() / 1024.0,
                                                                      dataDelivered.getTotal()));
                        logger.message(VirtualHostMessages.STATS_MSGS(name,
                                                                      DELIVERED,
                                                                      messagesDelivered.getPeak(),
                                                                      messagesDelivered.getTotal()));
                        logger.message(VirtualHostMessages.STATS_DATA(name,
                                                                      RECEIVED,
                                                                      dataReceived.getPeak() / 1024.0,
                                                                      dataReceived.getTotal()));
                        logger.message(VirtualHostMessages.STATS_MSGS(name,
                                                                      RECEIVED,
                                                                      messagesReceived.getPeak(),
                                                                      messagesReceived.getTotal()));

                    }
                }

                if (_reset)
                {
                    resetStatistics();
                }
            }
            catch(Exception e)
            {
                LOGGER.warn("Unexpected exception occurred while reporting the statistics", e);
            }
        }
    }

    @Override
    public boolean isVirtualHostPropertiesNodeEnabled()
    {
        return _virtualHostPropertiesNodeEnabled;
    }

    public AuthenticationProvider<?> getManagementModeAuthenticationProvider()
    {
        return _managementModeAuthenticationProvider;
    }

    @Override
    public int getNetworkBufferSize()
    {
        return _networkBufferSize;
    }

    public String getDocumentationUrl()
    {
        return _documentationUrl;
    }

    @Override
    public Principal getUser()
    {
        return AuthenticatedPrincipal.getCurrentUser();
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
                LOGGER.warn("Interrupted during Housekeeping shutdown:", e);
                Thread.currentThread().interrupt();
            }
        }
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
