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
package org.apache.qpid.server.model.adapter;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.QpidLoggerTurboFilter;
import org.apache.qpid.server.logging.StartupAppender;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.BrokerOptions;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogRecorder;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.logging.messages.VirtualHostMessages;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.plugin.ConfigurationSecretEncrypterFactory;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemNodeCreator;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.security.auth.manager.SimpleAuthenticationManager;
import org.apache.qpid.server.stats.StatisticsCounter;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.server.virtualhost.VirtualHostPropertiesNodeCreator;
import org.apache.qpid.util.SystemUtils;

public class BrokerAdapter extends AbstractConfiguredObject<BrokerAdapter> implements Broker<BrokerAdapter>, StatisticsGatherer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAdapter.class);

    private static final Pattern MODEL_VERSION_PATTERN = Pattern.compile("^\\d+\\.\\d+$");


    public static final String MANAGEMENT_MODE_AUTHENTICATION = "MANAGEMENT_MODE_AUTHENTICATION";

    private String[] POSITIVE_NUMERIC_ATTRIBUTES = { CONNECTION_SESSION_COUNT_LIMIT,
            CONNECTION_HEART_BEAT_DELAY, STATISTICS_REPORTING_PERIOD };


    private SystemConfig<?> _parent;
    private EventLogger _eventLogger;
    private LogRecorder _logRecorder;

    private final SecurityManager _securityManager;

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

    @ManagedAttributeField(afterSet = "postEncrypterProviderSet")
    private String _confidentialConfigurationEncryptionProvider;

    private final boolean _virtualHostPropertiesNodeEnabled;
    private Collection<BrokerLogger> _brokerLoggersToClose;
    private int _networkBufferSize = DEFAULT_NETWORK_BUFFER_SIZE;
    private final long _maximumHeapHize = Runtime.getRuntime().maxMemory();
    private final long _maximumDirectMemorySize = getMaxDirectMemorySize();
    private final BufferPoolMXBean _bufferPoolMXBean;
    private final List<String> _jvmArguments;
    private String _documentationUrl;

    @ManagedObjectFactoryConstructor
    public BrokerAdapter(Map<String, Object> attributes,
                         SystemConfig parent)
    {
        super(parentsMap(parent), attributes);
        _parent = parent;
        _eventLogger = parent.getEventLogger();
        _securityManager = new SecurityManager(this, parent.isManagementMode());
        if (parent.isManagementMode())
        {
            Map<String,Object> authManagerAttrs = new HashMap<String, Object>();
            authManagerAttrs.put(NAME,"MANAGEMENT_MODE_AUTHENTICATION");
            authManagerAttrs.put(ID, UUID.randomUUID());
            SimpleAuthenticationManager authManager = new SimpleAuthenticationManager(authManagerAttrs, this);
            authManager.addUser(BrokerOptions.MANAGEMENT_MODE_USER_NAME, _parent.getManagementModePassword());
            _managementModeAuthenticationProvider = authManager;
        }

        QpidServiceLoader qpidServiceLoader = new QpidServiceLoader();
        final Set<String> systemNodeCreatorTypes = qpidServiceLoader.getInstancesByType(SystemNodeCreator.class).keySet();
        _virtualHostPropertiesNodeEnabled = systemNodeCreatorTypes.contains(VirtualHostPropertiesNodeCreator.TYPE);
        if(attributes.get(CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER) != null )
        {

            final String encryptionProviderType = String.valueOf(attributes.get(CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER));
            updateEncrypter(encryptionProviderType);
        }
        _messagesDelivered = new StatisticsCounter("messages-delivered");
        _dataDelivered = new StatisticsCounter("bytes-delivered");
        _messagesReceived = new StatisticsCounter("messages-received");
        _dataReceived = new StatisticsCounter("bytes-received");

        BufferPoolMXBean bufferPoolMXBean = null;
        List<BufferPoolMXBean> bufferPoolMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for(BufferPoolMXBean mBean : bufferPoolMXBeans)
        {
            if (mBean.getName().equals("direct"))
            {
                bufferPoolMXBean = mBean;
                break;
            }
        }
        _bufferPoolMXBean = bufferPoolMXBean;
        _jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    }

    private void updateEncrypter(final String encryptionProviderType)
    {
        if(encryptionProviderType != null && !"".equals(encryptionProviderType.trim()))
        {
            PluggableFactoryLoader<ConfigurationSecretEncrypterFactory> factoryLoader =
                    new PluggableFactoryLoader<>(ConfigurationSecretEncrypterFactory.class);
            ConfigurationSecretEncrypterFactory factory = factoryLoader.get(encryptionProviderType);
            if (factory == null)
            {
                throw new IllegalConfigurationException("Unknown Configuration Secret Encryption method "
                                                        + encryptionProviderType);
            }
            setEncrypter(factory.createEncrypter(this));
        }
        else
        {
            setEncrypter(null);
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

        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        StartupAppender startupAppender = (StartupAppender) rootLogger.getAppender(StartupAppender.class.getName());
        if (startupAppender != null)
        {
            rootLogger.detachAppender(startupAppender);
            startupAppender.stop();
        }

        final SystemConfig parent = getParent(SystemConfig.class);
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

        Collection<AccessControlProvider<?>> accessControlProviders = getAccessControlProviders();

        if(accessControlProviders != null && accessControlProviders.size() > 1)
        {
            deleted();
            throw new IllegalArgumentException("At most one AccessControlProvider can be defined");
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

    @Override
    public void initiateShutdown()
    {
        _securityManager.authorise(Operation.SHUTDOWN, this);
        getEventLogger().message(BrokerMessages.OPERATION("initiateShutdown"));
        _parent.closeAsync();
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

        initialiseStatisticsReporting();

        if (isManagementMode())
        {
            _eventLogger.message(BrokerMessages.MANAGEMENT_MODE(BrokerOptions.MANAGEMENT_MODE_USER_NAME,
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
    public String getBuildVersion()
    {
        return CommonProperties.getBuildVersion();
    }

    @Override
    public String getOperatingSystem()
    {
        return SystemUtils.getOSString();
    }

    @Override
    public String getPlatform()
    {
        return System.getProperty("java.vendor") + " "
                      + System.getProperty("java.runtime.version", System.getProperty("java.version"));
    }

    @Override
    public String getProcessPid()
    {
        return SystemUtils.getProcessPid();
    }

    @Override
    public String getProductVersion()
    {
        return CommonProperties.getReleaseVersion();
    }

    @Override
    public int getNumberOfCores()
    {
        return Runtime.getRuntime().availableProcessors();
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
    public String getConfidentialConfigurationEncryptionProvider()
    {
        return _confidentialConfigurationEncryptionProvider;
    }

    @Override
    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
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
        Map<VirtualHost<?, ?, ?>, Long> vhs = new HashMap<>();
        for (VirtualHostNode<?> vhn : vhns)
        {
            VirtualHost<?, ?, ?> vh = vhn.getVirtualHost();
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
        for (Map.Entry<VirtualHost<?, ?, ?>, Long> entry : vhs.entrySet())
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

        assignTargetSizes();
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
                               Subject.doAs(SecurityManager.getSubjectWithAddedSystemRights(),
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
            // uninstall Qpid turbo filter
            QpidLoggerTurboFilter.uninstallFromRootContext();
        }
    }

    @Override
    public SecurityManager getSecurityManager()
    {
        return _securityManager;
    }

    @Override
    public VirtualHost<?,?,?> findVirtualHostByName(String name)
    {
        for (VirtualHostNode<?> virtualHostNode : getChildren(VirtualHostNode.class))
        {
            VirtualHost<?, ?, ?> virtualHost = virtualHostNode.getVirtualHost();
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
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    public void setEventLogger(final EventLogger eventLogger)
    {
        _eventLogger = eventLogger;
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

    public void resetStatistics()
    {
        _messagesDelivered.reset();
        _dataDelivered.reset();
        _messagesReceived.reset();
        _dataReceived.reset();

        for (VirtualHostNode<?> virtualHostNode : getChildren(VirtualHostNode.class))
        {
            VirtualHost<?, ?, ?> virtualHost = virtualHostNode.getVirtualHost();
            if (virtualHost instanceof StatisticsGatherer)
            {
                ((StatisticsGatherer) virtualHost).resetStatistics();
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
            _subject = SecurityManager.getSystemTaskSubject("Statistics");
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
                    VirtualHost<?, ?, ?> virtualHost = virtualHostNode.getVirtualHost();
                    if (virtualHost instanceof StatisticsGatherer && virtualHost instanceof  VirtualHostImpl)
                    {
                        StatisticsGatherer statisticsGatherer = (StatisticsGatherer) virtualHost;
                        String name = virtualHost.getName();
                        StatisticsCounter dataDelivered = statisticsGatherer.getDataDeliveryStatistics();
                        StatisticsCounter messagesDelivered = statisticsGatherer.getMessageDeliveryStatistics();
                        StatisticsCounter dataReceived = statisticsGatherer.getDataReceiptStatistics();
                        StatisticsCounter messagesReceived = statisticsGatherer.getMessageReceiptStatistics();
                        EventLogger logger = ((VirtualHostImpl)virtualHost).getEventLogger();
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

    @SuppressWarnings("unused")
    private void postEncrypterProviderSet()
    {
        updateEncrypter(_confidentialConfigurationEncryptionProvider);
        forceUpdateAllSecureAttributes();
    }

    @SuppressWarnings("unused")
    public static Collection<String> getAvailableConfigurationEncrypters()
    {
        return (new QpidServiceLoader()).getInstancesByType(ConfigurationSecretEncrypterFactory.class).keySet();
    }

    public static long getMaxDirectMemorySize()
    {
        long maxMemory = 0;
        try
        {
            ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
            Class<?> hotSpotDiagnosticMXBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean", true, systemClassLoader);
            Class<?> vmOptionClass = Class.forName("com.sun.management.VMOption", true, systemClassLoader);

            Object hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean((Class<? extends PlatformManagedObject>)hotSpotDiagnosticMXBeanClass);
            Method getVMOption = hotSpotDiagnosticMXBeanClass.getDeclaredMethod("getVMOption", String.class);
            Object vmOption = getVMOption.invoke(hotSpotDiagnosticMXBean, "MaxDirectMemorySize");
            Method getValue = vmOptionClass.getDeclaredMethod("getValue");
            String maxDirectMemoryAsString = (String)getValue.invoke(vmOption);
            maxMemory = Long.parseLong(maxDirectMemoryAsString);
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e)
        {
            LOGGER.debug("Cannot determine direct memory max size using com.sun.management.HotSpotDiagnosticMXBean: " + e.getMessage());
        }
        catch (InvocationTargetException e)
        {
            throw new ServerScopedRuntimeException("Unexpected exception in evaluation of MaxDirectMemorySize with HotSpotDiagnosticMXBean", e.getTargetException());
        }

        if (maxMemory == 0)
        {
            Pattern maxDirectMemorySizeArgumentPattern = Pattern.compile("^\\s*-XX:MaxDirectMemorySize\\s*=\\s*(\\d+)\\s*([KkMmGgTt]?)\\s*$");
            RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
            List<String> inputArguments = RuntimemxBean.getInputArguments();
            boolean argumentFound = false;
            for (String argument : inputArguments)
            {
                Matcher matcher = maxDirectMemorySizeArgumentPattern.matcher(argument);
                if (matcher.matches())
                {
                    argumentFound = true;
                    maxMemory = Long.parseLong(matcher.group(1));
                    String unit = matcher.group(2);
                    char unitChar = "".equals(unit) ? 0 : unit.charAt(0);
                    switch (unitChar)
                    {
                        case 'k':
                        case 'K':
                            maxMemory *= 1024l;
                            break;
                        case 'm':
                        case 'M':
                            maxMemory *= 1024l * 1024l;
                            break;
                        case 'g':
                        case 'G':
                            maxMemory *= 1024l * 1024l * 1024l;
                            break;
                        case 't':
                        case 'T':
                            maxMemory *= 1024l * 1024l * 1024l * 1024l;
                            break;
                        case 0:
                            // noop
                            break;
                        default:
                            throw new IllegalStateException("Unexpected unit character in MaxDirectMemorySize argument : " + argument);
                    }
                    // do not break; continue. Oracle and IBM JVMs use the last value when argument is specified multiple times
                }
            }

            if (maxMemory == 0)
            {
                if (argumentFound)
                {
                    throw new IllegalArgumentException("Qpid Broker cannot operate with 0 direct memory. Please, set JVM argument MaxDirectMemorySize to non-zero value");
                }
                else
                {
                    maxMemory = Runtime.getRuntime().maxMemory();
                }
            }
        }

        return maxMemory;
    }

    @Override
    public int getNetworkBufferSize()
    {
        return _networkBufferSize;
    }

    @Override
    public int getNumberOfLiveThreads()
    {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }

    @Override
    public long getMaximumHeapMemorySize()
    {
        return _maximumHeapHize;
    }

    @Override
    public long getUsedHeapMemorySize()
    {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    @Override
    public long getMaximumDirectMemorySize()
    {
        return _maximumDirectMemorySize;
    }

    @Override
    public long getUsedDirectMemorySize()
    {
        if (_bufferPoolMXBean == null)
        {
            return -1;
        }
        return _bufferPoolMXBean.getMemoryUsed();
    }

    @Override
    public long getDirectMemoryTotalCapacity()
    {
        if (_bufferPoolMXBean == null)
        {
            return -1;
        }
        return _bufferPoolMXBean.getTotalCapacity();
    }

    @Override
    public int getNumberOfObjectsPendingFinalization()
    {
        return ManagementFactory.getMemoryMXBean().getObjectPendingFinalizationCount();
    }

    @Override
    public List<String> getJvmArguments()
    {
        return _jvmArguments;
    }

    @Override
    public String getDocumentationUrl()
    {
        return _documentationUrl;
    }

    @Override
    public void performGC()
    {
        _securityManager.authorise(Operation.CONFIGURE, this);
        getEventLogger().message(BrokerMessages.OPERATION("performGC"));
        System.gc();
    }

    @Override
    public Content getThreadStackTraces(boolean appendToLog)
    {
        _securityManager.authorise(Operation.CONFIGURE, this);
        getEventLogger().message(BrokerMessages.OPERATION("getThreadStackTraces"));
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        StringBuilder threadDump = new StringBuilder();
        for (ThreadInfo threadInfo : threadInfos)
        {
            threadDump.append(getThreadStackTraces(threadInfo));
        }
        long[] deadLocks = threadMXBean.findDeadlockedThreads();
        if (deadLocks != null && deadLocks.length > 0)
        {
            ThreadInfo[] deadlockedThreads = threadMXBean.getThreadInfo(deadLocks);
            threadDump.append(System.lineSeparator()).append("Deadlock is detected!").append(System.lineSeparator());
            for (ThreadInfo threadInfo : deadlockedThreads)
            {
                threadDump.append(getThreadStackTraces(threadInfo));
            }
        }
        String threadStackTraces = threadDump.toString();
        if (appendToLog)
        {
            LOGGER.warn("Thread dump:{} {}", System.lineSeparator(), threadStackTraces);
        }
        return new ThreadStackContent(threadStackTraces);
    }

    @Override
    public Content findThreadStackTraces(String threadNameFindExpression)
    {
        _securityManager.authorise(Operation.CONFIGURE, this);
        getEventLogger().message(BrokerMessages.OPERATION("findThreadStackTraces"));
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        StringBuilder threadDump = new StringBuilder();
        Pattern pattern = threadNameFindExpression == null || threadNameFindExpression.equals("") ? null : Pattern.compile(
                threadNameFindExpression);
        for (ThreadInfo threadInfo : threadInfos)
        {
            if (pattern== null || pattern.matcher(threadInfo.getThreadName()).find())
            {
                threadDump.append(getThreadStackTraces(threadInfo));
            }
        }
        return new ThreadStackContent(threadDump.toString());
    }

    private String getThreadStackTraces(final ThreadInfo threadInfo)
    {
        String lineSeparator = System.lineSeparator();
        StringBuilder dump = new StringBuilder();
        dump.append("\"").append(threadInfo.getThreadName()).append("\"").append(" Id=")
            .append(threadInfo.getThreadId()).append( " ").append(threadInfo.getThreadState());
        if (threadInfo.getLockName() != null)
        {
            dump.append(" on ").append(threadInfo.getLockName());
        }
        if (threadInfo.getLockOwnerName() != null)
        {
            dump.append(" owned by \"").append(threadInfo.getLockOwnerName())
                .append("\" Id=").append(threadInfo.getLockOwnerId());
        }
        if (threadInfo.isSuspended())
        {
            dump.append(" (suspended)");
        }
        if (threadInfo.isInNative())
        {
            dump.append(" (in native)");
        }
        dump.append(lineSeparator);
        StackTraceElement[] stackTrace = threadInfo.getStackTrace();
        for (int i = 0; i < stackTrace.length; i++)
        {
            StackTraceElement stackTraceElement = stackTrace[i];
            dump.append("    at ").append(stackTraceElement.toString()).append(lineSeparator);

            LockInfo lockInfo = threadInfo.getLockInfo();
            if (i == 0 && lockInfo != null)
            {
                Thread.State threadState = threadInfo.getThreadState();
                switch (threadState)
                {
                    case BLOCKED:
                        dump.append("    -  blocked on ").append(lockInfo).append(lineSeparator);
                        break;
                    case WAITING:
                        dump.append("    -  waiting on ").append(lockInfo).append(lineSeparator);
                        break;
                    case TIMED_WAITING:
                        dump.append("    -  waiting on ").append(lockInfo).append(lineSeparator);
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : threadInfo.getLockedMonitors())
            {
                if (mi.getLockedStackDepth() == i)
                {
                    dump.append("    -  locked ").append(mi).append(lineSeparator);
                }
            }
        }

        LockInfo[] locks = threadInfo.getLockedSynchronizers();
        if (locks.length > 0)
        {
            dump.append(lineSeparator).append("    Number of locked synchronizers = ").append(locks.length);
            dump.append(lineSeparator);
            for (LockInfo li : locks)
            {
                dump.append("    - " + li);
                dump.append(lineSeparator);
            }
        }
        dump.append(lineSeparator);
        return dump.toString();
    }

    public static class ThreadStackContent implements Content, CustomRestHeaders
    {
        private final String _threadStackTraces;

        public ThreadStackContent(final String threadStackTraces)
        {
            _threadStackTraces = threadStackTraces;
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            if (_threadStackTraces != null)
            {
                outputStream.write(_threadStackTraces.getBytes(Charset.forName("UTF-8")));
            }
        }

        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "text/plain;charset=utf-8";
        }
    }
}
