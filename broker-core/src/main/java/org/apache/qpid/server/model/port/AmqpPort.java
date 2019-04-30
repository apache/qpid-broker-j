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
package org.apache.qpid.server.model.port;

import java.net.SocketAddress;
import java.util.List;
import java.util.Set;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedStatistic;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;

@ManagedObject( category = false, type = "AMQP", amqpName = "org.apache.qpid.AmqpPort")
public interface AmqpPort<X extends AmqpPort<X>> extends Port<X>
{
    String DEFAULT_AMQP_TCP_NO_DELAY = "true";

    String DEFAULT_AMQP_NEED_CLIENT_AUTH = "false";
    String DEFAULT_AMQP_WANT_CLIENT_AUTH = "false";

    String MAX_OPEN_CONNECTIONS = "maxOpenConnections";
    String THREAD_POOL_SIZE = "threadPoolSize";
    String NUMBER_OF_SELECTORS = "numberOfSelectors";

    String DEFAULT_AMQP_PROTOCOLS = "qpid.port.default_amqp_protocols";

    String PORT_AMQP_THREAD_POOL_SIZE = "qpid.port.amqp.threadPool.size";
    String PORT_AMQP_THREAD_POOL_KEEP_ALIVE_TIMEOUT = "qpid.port.amqp.threadPool.keep_alive_timeout";

    String PORT_AMQP_NUMBER_OF_SELECTORS = "qpid.port.amqp.threadPool.numberOfSelectors";
    String PORT_AMQP_ACCEPT_BACKLOG = "qpid.port.amqp.acceptBacklog";

    @ManagedContextDefault(name = DEFAULT_AMQP_PROTOCOLS)
    String INSTALLED_PROTOCOLS = AmqpPortImpl.getInstalledProtocolsAsString();

    String PORT_MAX_OPEN_CONNECTIONS = "qpid.port.max_open_connections";

    @ManagedContextDefault(name = PORT_MAX_OPEN_CONNECTIONS)
    int DEFAULT_MAX_OPEN_CONNECTIONS = -1;

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = PORT_AMQP_THREAD_POOL_SIZE)
    long DEFAULT_PORT_AMQP_THREAD_POOL_SIZE = 8;

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_AMQP_THREAD_POOL_KEEP_ALIVE_TIMEOUT)
    long DEFAULT_PORT_AMQP_THREAD_POOL_KEEP_ALIVE_TIMEOUT = 60; // Minutes

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_AMQP_NUMBER_OF_SELECTORS)
    long DEFAULT_PORT_AMQP_NUMBER_OF_SELECTORS = Math.max(DEFAULT_PORT_AMQP_THREAD_POOL_SIZE / 8, 1);

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_AMQP_ACCEPT_BACKLOG)
    int DEFAULT_PORT_AMQP_ACCEPT_BACKLOG = 1024;

    String OPEN_CONNECTIONS_WARN_PERCENT = "qpid.port.open_connections_warn_percent";

    @ManagedContextDefault(name = OPEN_CONNECTIONS_WARN_PERCENT)
    int DEFAULT_OPEN_CONNECTIONS_WARN_PERCENT = 80;

    String PROTOCOL_HANDSHAKE_TIMEOUT = "qpid.port.protocol_handshake_timeout";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PROTOCOL_HANDSHAKE_TIMEOUT,
                           description = "Maximum time allowed for a new connection to send a protocol header."
                                         + " If the connection does not send a protocol header within this time,"
                                         + " the connection will be aborted.")
    long DEFAULT_PROTOCOL_HANDSHAKE_TIMEOUT = 2000;

    String PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY = "qpid.broker_default_supported_protocol_version_reply";

    String CLOSE_WHEN_NO_ROUTE = "qpid.port.closeWhenNoRoute";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = CLOSE_WHEN_NO_ROUTE)
    boolean DEFAULT_CONNECTION_CLOSE_WHEN_NO_ROUTE = true;

    String SESSION_COUNT_LIMIT = "qpid.port.sessionCountLimit";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = SESSION_COUNT_LIMIT)
    int DEFAULT_SESSION_COUNT_LIMIT = 256;

    String HEART_BEAT_DELAY = "qpid.port.heartbeatDelay";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = HEART_BEAT_DELAY)
    int DEFAULT_HEART_BEAT_DELAY = 0;

    String TLS_SESSION_TIMEOUT = "qpid.port.amqp.tlsSessionTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = TLS_SESSION_TIMEOUT, description = "TLS session timeout for AMQP ports (seconds).")
    int DEFAULT_TLS_SESSION_TIMEOUT = 5* 60;

    String TLS_SESSION_CACHE_SIZE = "qpid.port.amqp.tlsSessionCacheSize";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = TLS_SESSION_CACHE_SIZE, description = "TLS session cache size for AMQP ports.")
    int DEFAULT_TLS_SESSION_CACHE_SIZE = 100;

    String CONNECTION_PROPERTY_ENRICHERS = "qpid.port.connection_property_enrichers";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = CONNECTION_PROPERTY_ENRICHERS,
            description = "The connection property enrichers to apply to connections created on this port.")
    String DEFAULT_CONNECTION_PROTOCOL_ENRICHERS = "[ \"STANDARD\" ] ";

    @ManagedAttribute( defaultValue = AmqpPort.DEFAULT_AMQP_TCP_NO_DELAY )
    boolean isTcpNoDelay();

    @ManagedAttribute( defaultValue = "${" + PORT_AMQP_THREAD_POOL_SIZE + "}")
    int getThreadPoolSize();

    @ManagedAttribute( defaultValue = "${" + PORT_AMQP_NUMBER_OF_SELECTORS + "}")
    int getNumberOfSelectors();

    @Override
    @ManagedAttribute( defaultValue = DEFAULT_AMQP_NEED_CLIENT_AUTH )
    boolean getNeedClientAuth();

    @Override
    @ManagedAttribute( defaultValue = DEFAULT_AMQP_WANT_CLIENT_AUTH )
    boolean getWantClientAuth();

    @Override
    @ManagedAttribute( defaultValue = "TCP",
                       validValues = {"org.apache.qpid.server.model.port.AmqpPortImpl#getAllAvailableTransportCombinations()"})
    Set<Transport> getTransports();

    @Override
    @ManagedAttribute( defaultValue = "${" + DEFAULT_AMQP_PROTOCOLS + "}", validValues = {"org.apache.qpid.server.model.port.AmqpPortImpl#getAllAvailableProtocolCombinations()"} )
    Set<Protocol> getProtocols();

    @ManagedAttribute( defaultValue = "${" + PORT_MAX_OPEN_CONNECTIONS + "}" )
    int getMaxOpenConnections();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Open Connections",
                      description = "Current number of connections made through this port")
    int getConnectionCount();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.COUNT, label = "Total Connections",
            description = "Total number of connections made through this port since broker startup")
    long getTotalConnectionCount();

    @DerivedAttribute(description = "Maximum time allowed for a new connection to send a protocol header."
                                    + " If the connection does not send a protocol header within this time,"
                                    + " the connection will be aborted.")
    long getProtocolHandshakeTimeout();

    @DerivedAttribute(description = "Controls behaviour when the Broker receives a message for which no destination exists"
                                    + " or is otherwise rejected by the destination. For AMQP 0-8..0-91 the connection will"
                                    + " be closed only if transactionally publishing a message with the mandatory flag"
                                    + " set and the Publisher Confirms extension is disabled."
                                    + " For 0-10, the session will be closed if publishing a message (without the discard"
                                    + " unroutable flag).  In all other cases, this flag has no effect.")
    boolean getCloseWhenNoRoute();


    @DerivedAttribute(description = "The maximum number of sessions which can exist concurrently on a connection." )
    int getSessionCountLimit();

    @DerivedAttribute(description = "For AMQP 0-8..0-10 the default period with which Broker and client will exchange"
                                    + " heartbeat messages (in seconds). Clients may negotiate a different heartbeat"
                                    + " frequency or disable it altogether."
                                    + " For AMQP 1.0 this setting controls the incoming idle timeout only.  A value of"
                                    + " 0 disables.")
    int getHeartbeatDelay();

    boolean canAcceptNewConnection(final SocketAddress remoteSocketAddress);

    int incrementConnectionCount();

    int decrementConnectionCount();

    int getNetworkBufferSize();

    List<ConnectionPropertyEnricher> getConnectionPropertyEnrichers();
}
