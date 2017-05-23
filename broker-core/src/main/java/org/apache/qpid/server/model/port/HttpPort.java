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

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject( category = false, type = "HTTP", amqpName = "org.apache.qpid.HttpPort")
public interface HttpPort<X extends HttpPort<X>> extends ClientAuthCapablePort<X>
{
    String DEFAULT_HTTP_NEED_CLIENT_AUTH = "false";
    String DEFAULT_HTTP_WANT_CLIENT_AUTH = "false";
    String THREAD_POOL_MINIMUM = "threadPoolMinimum";
    String THREAD_POOL_MAXIMUM = "threadPoolMaximum";
    String ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS = "allowConfidentialOperationsOnInsecureChannels";

    String PORT_HTTP_NUMBER_OF_SELECTORS = "qpid.port.http.threadPool.numberOfSelectors";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_NUMBER_OF_SELECTORS)
    long DEFAULT_PORT_HTTP_NUMBER_OF_SELECTORS = -1;
    String PORT_HTTP_NUMBER_OF_ACCEPTORS = "qpid.port.http.threadPool.numberOfAcceptors";

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_NUMBER_OF_ACCEPTORS)
    long DEFAULT_PORT_HTTP_NUMBER_OF_ACCEPTORS = -1;

    String PORT_HTTP_ACCEPT_BACKLOG = "qpid.port.http.acceptBacklog";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PORT_HTTP_ACCEPT_BACKLOG)
    int DEFAULT_PORT_HTTP_ACCEPT_BACKLOG = 1024;

    @ManagedAttribute(defaultValue = "*")
    String getBindingAddress();

    @ManagedAttribute( defaultValue = DEFAULT_HTTP_NEED_CLIENT_AUTH)
    boolean getNeedClientAuth();

    @ManagedAttribute( defaultValue = DEFAULT_HTTP_WANT_CLIENT_AUTH)
    boolean getWantClientAuth();

    @ManagedAttribute
    TrustStore<?> getClientCertRecorder();

    @ManagedAttribute( mandatory = true )
    AuthenticationProvider getAuthenticationProvider();


    @ManagedAttribute( defaultValue = "TCP",
                       validValues = {"[ \"TCP\" ]", "[ \"SSL\" ]", "[ \"TCP\", \"SSL\" ]"})
    Set<Transport> getTransports();

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

    @ManagedAttribute( defaultValue = "false", description = "If true allow operations which may return confidential "
                                                             + "information to be executed on insecure connections")
    boolean isAllowConfidentialOperationsOnInsecureChannels();

    @ManagedAttribute( defaultValue = "true", description = "If true then this port will provide HTTP management "
                                                            + "services for the broker, if no virtualhostalaias "
                                                            + "matches the HTTP Host in the request")
    boolean isManageBrokerOnNoAliasMatch();

    @DerivedAttribute (description = "Number of acceptor threads")
    int getNumberOfAcceptors();

    @DerivedAttribute (description = "Number of selector threads")
    int getNumberOfSelectors();

    @DerivedAttribute (description = "Size of accepts backlog")
    int getAcceptsBacklogSize();
}
