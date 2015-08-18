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

import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.qpid.server.logging.QpidLoggerTurboFilter;
import org.apache.qpid.server.logging.StartupAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.common.QpidProperties;
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
        if(changedAttributes.contains(DURABLE) && !proxyForValidation.isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
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

    private void performActivation()
    {
        boolean hasBrokerAnyErroredChildren = false;

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
                                new Object[]{ child.getName(), childClass.getSimpleName(), State.ERRORED });
                    }
                }
            }
        }

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
        return QpidProperties.getBuildVersion();
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
        return QpidProperties.getReleaseVersion();
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
        long totalTarget  = getContextValue(Long.class, BROKER_FLOW_TO_DISK_THRESHOLD);
        long totalSize = 0l;
        Collection<VirtualHostNode<?>> vhns = getVirtualHostNodes();
        Map<VirtualHost<?,?,?>,Long> vhs = new HashMap<>();
        for(VirtualHostNode<?> vhn : vhns)
        {
            VirtualHost<?, ?, ?> vh = vhn.getVirtualHost();
            if(vh != null)
            {
                long totalQueueDepthBytes = vh.getTotalQueueDepthBytes();
                vhs.put(vh,totalQueueDepthBytes);
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

        for(Map.Entry<VirtualHost<?, ?, ?>,Long> entry : vhs.entrySet())
        {

            long size = (long) (entry.getValue().doubleValue() * ((double) totalTarget / (double) totalSize));
            entry.getKey().setTargetSize(size);
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        getEventLogger().message(BrokerMessages.STARTUP(QpidProperties.getReleaseVersion(),
                                                        QpidProperties.getBuildVersion()));

        getEventLogger().message(BrokerMessages.PLATFORM(System.getProperty("java.vendor"),
                                                         System.getProperty("java.runtime.version",
                                                                            System.getProperty("java.version")),
                                                         SystemUtils.getOSName(),
                                                         SystemUtils.getOSVersion(),
                                                         SystemUtils.getOSArch(),
                                                         String.valueOf(getNumberOfCores())));

        getEventLogger().message(BrokerMessages.MAX_MEMORY(Runtime.getRuntime().maxMemory()));

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
            if (virtualHost instanceof VirtualHostImpl)
            {
                ((VirtualHostImpl) virtualHost).resetStatistics();
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
                    if (virtualHost instanceof VirtualHostImpl)
                    {
                        VirtualHostImpl vhostImpl = (VirtualHostImpl) virtualHost;
                        String name = virtualHost.getName();
                        StatisticsCounter dataDelivered = vhostImpl.getDataDeliveryStatistics();
                        StatisticsCounter messagesDelivered = vhostImpl.getMessageDeliveryStatistics();
                        StatisticsCounter dataReceived = vhostImpl.getDataReceiptStatistics();
                        StatisticsCounter messagesReceived = vhostImpl.getMessageReceiptStatistics();
                        EventLogger logger = vhostImpl.getEventLogger();
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
}
