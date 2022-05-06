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

import java.util.Set;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;

@ManagedObject( category = false, type = "HTTP", amqpName = "org.apache.qpid.HttpPort")
public interface HttpPort<X extends HttpPort<X>> extends Port<X>
{
    String DEFAULT_HTTP_NEED_CLIENT_AUTH = "false";
    String DEFAULT_HTTP_WANT_CLIENT_AUTH = "false";
    String THREAD_POOL_MINIMUM = "threadPoolMinimum";
    String THREAD_POOL_MAXIMUM = "threadPoolMaximum";

    String PORT_HTTP_NUMBER_OF_SELECTORS = "qpid.port.http.threadPool.numberOfSelectors";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_NUMBER_OF_SELECTORS,
            description = "Desired number of selectors, if negative the number of selector is determined by Jetty")
    long DEFAULT_PORT_HTTP_NUMBER_OF_SELECTORS = -1;
    String PORT_HTTP_NUMBER_OF_ACCEPTORS = "qpid.port.http.threadPool.numberOfAcceptors";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_NUMBER_OF_ACCEPTORS,
            description = "Desired number of acceptors. If negative the number of acceptors is determined by Jetty."
                          + " If zero, the selector threads are used as acceptors.")
    long DEFAULT_PORT_HTTP_NUMBER_OF_ACCEPTORS = -1;

    String PORT_HTTP_ACCEPT_BACKLOG = "qpid.port.http.acceptBacklog";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_ACCEPT_BACKLOG, description = "The size of the pending connection backlog")
    int DEFAULT_PORT_HTTP_ACCEPT_BACKLOG = 1024;

    String ABSOLUTE_SESSION_TIMEOUT = "qpid.port.http.absoluteSessionTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = ABSOLUTE_SESSION_TIMEOUT,
                           description = "The maximum amount of time (in milliseconds) a session can be active.")
    long DEFAULT_ABSOLUTE_SESSION_TIMEOUT = -1;

    String TLS_SESSION_TIMEOUT = "qpid.port.http.tlsSessionTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = TLS_SESSION_TIMEOUT, description = "TLS session timeout for HTTP ports (seconds).")
    int DEFAULT_TLS_SESSION_TIMEOUT = 15 * 60;

    String TLS_SESSION_CACHE_SIZE = "qpid.port.http.tlsSessionCacheSize";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = TLS_SESSION_CACHE_SIZE, description = "TLS session cache size for HTTP ports.")
    int DEFAULT_TLS_SESSION_CACHE_SIZE = 1000;

    @Override
    @ManagedAttribute( defaultValue = DEFAULT_HTTP_NEED_CLIENT_AUTH)
    boolean getNeedClientAuth();

    @Override
    @ManagedAttribute( defaultValue = DEFAULT_HTTP_WANT_CLIENT_AUTH)
    boolean getWantClientAuth();

    @Override
    @ManagedAttribute( defaultValue = "TCP",
                       validValues = {"[ \"TCP\" ]", "[ \"SSL\" ]", "[ \"TCP\", \"SSL\" ]"})
    Set<Transport> getTransports();

    @Override
    @ManagedAttribute( defaultValue = "HTTP", validValues = { "[ \"HTTP\"]"} )
    Set<Protocol> getProtocols();

    void setPortManager(PortManager manager);

    String PORT_HTTP_THREAD_POOL_MAXIMUM = "port.http.threadPool.maximum";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = PORT_HTTP_THREAD_POOL_MAXIMUM )
    long DEFAULT_PORT_HTTP_THREAD_POOL_MAXIMUM = 24;

    @ManagedAttribute( defaultValue = "${" + PORT_HTTP_THREAD_POOL_MAXIMUM + "}")
    int getThreadPoolMaximum();

    String PORT_HTTP_THREAD_POOL_MINIMUM = "port.http.threadPool.minimum";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = PORT_HTTP_THREAD_POOL_MINIMUM )
    long DEFAULT_PORT_HTTP_THREAD_POOL_MINIMUM = 8;

    @ManagedAttribute( defaultValue = "${" + PORT_HTTP_THREAD_POOL_MINIMUM + "}")
    int getThreadPoolMinimum();

    @ManagedAttribute( defaultValue = "true", description = "If true then this port will provide HTTP management "
                                                            + "services for the broker, if no virtualhostalaias "
                                                            + "matches the HTTP Host in the request")
    boolean isManageBrokerOnNoAliasMatch();

    @DerivedAttribute(description = "Desired number of acceptors. See context variable '"
                                    + PORT_HTTP_NUMBER_OF_ACCEPTORS
                                    + "'")
    int getDesiredNumberOfAcceptors();

    @DerivedAttribute(description = "Desired number of selectors. See context variable '"
                                    + PORT_HTTP_NUMBER_OF_ACCEPTORS
                                    + "'")
    int getDesiredNumberOfSelectors();

    @DerivedAttribute(description = "Size of accept backlog")
    int getAcceptBacklogSize();

    @DerivedAttribute(description = "Actual number of acceptors.")
    int getNumberOfAcceptors();

    @DerivedAttribute(description = "Actual number of selectors.")
    int getNumberOfSelectors();

    @DerivedAttribute(description = "This timeout defines the maximum amount of time (in milliseconds) a session can be"
                                    + " active, regardless of any session activity. A value of zero or less disables"
                                    + " the limit.")
    long getAbsoluteSessionTimeout();
}
