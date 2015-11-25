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

import java.util.Collection;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.adapter.BrokerAdapter;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.stats.StatisticsGatherer;

@ManagedObject( defaultType = "broker" )
public interface Broker<X extends Broker<X>> extends ConfiguredObject<X>, EventLoggerProvider, StatisticsGatherer
{

    String BUILD_VERSION = "buildVersion";
    String OPERATING_SYSTEM = "operatingSystem";
    String PLATFORM = "platform";
    String PROCESS_PID = "processPid";
    String PRODUCT_VERSION = "productVersion";
    String STATISTICS_REPORTING_PERIOD = "statisticsReportingPeriod";
    String STATISTICS_REPORTING_RESET_ENABLED = "statisticsReportingResetEnabled";
    String STORE_PATH = "storePath";
    String MODEL_VERSION = "modelVersion";
    String CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER = "confidentialConfigurationEncryptionProvider";

    String CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = "channel.flowControlEnforcementTimeout";

    String CONNECTION_SESSION_COUNT_LIMIT = "connection.sessionCountLimit";
    String CONNECTION_HEART_BEAT_DELAY = "connection.heartBeatDelay";
    String CONNECTION_CLOSE_WHEN_NO_ROUTE = "connection.closeWhenNoRoute";

    String BROKER_FLOW_TO_DISK_THRESHOLD = "broker.flowToDiskThreshold";
    String BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = "broker.failStartupWithErroredChild";

    String BROKER_MSG_AUTH = "qpid.broker_msg_auth";

    String STORE_FILESYSTEM_MAX_USAGE_PERCENT = "store.filesystem.maxUsagePercent";
    String QPID_AMQP_PORT = "qpid.amqp_port";
    String QPID_HTTP_PORT = "qpid.http_port";
    String QPID_RMI_PORT  = "qpid.rmi_port";
    String QPID_JMX_PORT  = "qpid.jmx_port";

    String NETWORK_BUFFER_SIZE = "qpid.broker.networkBufferSize";
    // network buffer should at least hold a SSL/TLS frame which in jdk1.8 is 33305 bytes
    int MINIMUM_NETWORK_BUFFER_SIZE = 64 * 1024;
    @ManagedContextDefault(name = NETWORK_BUFFER_SIZE)
    int DEFAULT_NETWORK_BUFFER_SIZE = 256 * 1024;

    @ManagedContextDefault(name = "broker.name")
    String DEFAULT_BROKER_NAME = "Broker";

    @ManagedContextDefault(name = QPID_AMQP_PORT)
    String DEFAULT_AMQP_PORT_NUMBER = "5672";
    @ManagedContextDefault(name = QPID_HTTP_PORT)
    String DEFAULT_HTTP_PORT_NUMBER = "8080";
    @ManagedContextDefault(name = QPID_RMI_PORT)
    String DEFAULT_RMI_PORT_NUMBER  = "8999";
    @ManagedContextDefault(name = QPID_JMX_PORT)
    String DEFAULT_JMX_PORT_NUMBER  = "9099";

    @ManagedContextDefault(name = BROKER_FLOW_TO_DISK_THRESHOLD)
    long DEFAULT_FLOW_TO_DISK_THRESHOLD = (long)(0.4 * (double) BrokerAdapter.getMaxDirectMemorySize());

    @ManagedContextDefault(name = CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)
    long DEFAULT_CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = 5000l;

    @ManagedContextDefault(name = STORE_FILESYSTEM_MAX_USAGE_PERCENT)
    int DEFAULT_FILESYSTEM_MAX_USAGE_PERCENT = 90;

    @ManagedContextDefault(name = BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD)
    boolean DEFAULT_BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = false;

    @ManagedContextDefault(name = BROKER_MSG_AUTH)
    boolean DEFAULT_BROKER_MSG_AUTH = false;

    @DerivedAttribute
    String getBuildVersion();

    @DerivedAttribute
    String getOperatingSystem();

    @DerivedAttribute
    String getPlatform();

    @DerivedAttribute
    String getProcessPid();

    @DerivedAttribute
    String getProductVersion();

    @DerivedAttribute
    int getNumberOfCores();

    @ManagedAttribute( defaultValue = "256" )
    int getConnection_sessionCountLimit();

    @ManagedAttribute( defaultValue = "0")
    int getConnection_heartBeatDelay();

    @ManagedAttribute( defaultValue = "true" )
    boolean getConnection_closeWhenNoRoute();

    @ManagedAttribute( defaultValue = "0" )
    int getStatisticsReportingPeriod();

    @ManagedAttribute( defaultValue = "false")
    boolean getStatisticsReportingResetEnabled();

    String BROKER_MESSAGE_COMPRESSION_ENABLED = "broker.messageCompressionEnabled";
    @ManagedContextDefault(name = BROKER_MESSAGE_COMPRESSION_ENABLED)
    boolean DEFAULT_MESSAGE_COMPRESSION_ENABLED = true;

    @ManagedAttribute( defaultValue = "${"+ BROKER_MESSAGE_COMPRESSION_ENABLED +"}")
    boolean isMessageCompressionEnabled();

    String MESSAGE_COMPRESSION_THRESHOLD_SIZE = "connection.messageCompressionThresholdSize";
    @ManagedContextDefault(name = MESSAGE_COMPRESSION_THRESHOLD_SIZE)
    int DEFAULT_MESSAGE_COMPRESSION_THRESHOLD_SIZE = 102400;

    String SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP = "connection.sendQueueDeleteOkRegardlessClientVerRegexp";
    @ManagedContextDefault(name = SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP)
    String DEFAULT_SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP = "^0\\..*$";

    String BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE = "broker.directByteBufferPoolSize";
    @ManagedContextDefault(name = BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE)
    int DEFAULT_BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE = 1024;

    @ManagedAttribute(validValues = {"org.apache.qpid.server.model.adapter.BrokerAdapter#getAvailableConfigurationEncrypters()"})
    String getConfidentialConfigurationEncryptionProvider();

    @DerivedAttribute( persist = true )
    String getModelVersion();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound")
    long getBytesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound")
    long getBytesOut();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound")
    long getMessagesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound")
    long getMessagesOut();

    @ManagedOperation(nonModifying = true, description = "Initiates an orderly shutdown of the Broker.")
    void initiateShutdown();

    //children
    Collection<VirtualHostNode<?>> getVirtualHostNodes();

    Collection<Port<?>> getPorts();

    Collection<AuthenticationProvider<?>> getAuthenticationProviders();

    Collection<AccessControlProvider<?>> getAccessControlProviders();

    Collection<GroupProvider<?>> getGroupProviders();

    /**
     * A temporary hack to expose security manager via broker instance.
     * TODO We need to add and implement an authorization provider configured object instead
     */
    SecurityManager getSecurityManager();

    VirtualHost<?,?,?> findVirtualHostByName(String name);

    VirtualHostNode findDefautVirtualHostNode();

    Collection<KeyStore<?>> getKeyStores();

    Collection<TrustStore<?>> getTrustStores();

    boolean isManagementMode();

    EventLogger getEventLogger();

    void setEventLogger(EventLogger eventLogger);

    boolean isVirtualHostPropertiesNodeEnabled();

    AuthenticationProvider<?> getManagementModeAuthenticationProvider();

    void assignTargetSizes();

    int getNetworkBufferSize();
}
