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
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.security.SubjectCreator;

@ManagedContextDependency({CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST, CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST})
@ManagedObject( description = Port.CLASS_DESCRIPTION, amqpName = "org.apache.qpid.Port")
public interface Port<X extends Port<X>> extends ConfiguredObject<X>
{
    String CLASS_DESCRIPTION = "<p>The Broker supports configuration of Ports to specify the particular AMQP messaging "
                               + "and HTTP management connectivity it offers for use.</p>"
                               + "<p>Each Port is configured with the particular Protocols and Transports it supports, "
                               + "as well as the Authentication Provider to be used to authenticate connections. Where "
                               + "SSL is in use, the Port configuration also defines which Keystore to use and (where "
                               + "supported) which TrustStore(s) and whether Client Certificates should be "
                               + "requested/required.</p>";

    String BINDING_ADDRESS                      = "bindingAddress";
    String PORT                                 = "port";
    String PROTOCOLS                            = "protocols";
    String TRANSPORTS                           = "transports";
    String TCP_NO_DELAY                         = "tcpNoDelay";
    String NEED_CLIENT_AUTH                     = "needClientAuth";
    String WANT_CLIENT_AUTH                     = "wantClientAuth";
    String AUTHENTICATION_PROVIDER              = "authenticationProvider";
    String KEY_STORE                            = "keyStore";
    String TRUST_STORES                         = "trustStores";
    String CLIENT_CERT_RECORDER                 = "clientCertRecorder";

    String DEFAULT_BINDING_ADDRESS = "*";


    String CONNECTION_MAXIMUM_AUTHENTICATION_DELAY = "connection.maximumAuthenticationDelay";

    @ManagedContextDefault(name = CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)
    long DEFAULT_MAX_CONNECTION_AUTHENTICATION_DELAY = 10000l;
    String ALLOW_CONFIDENTIAL_OPERATIONS_ON_INSECURE_CHANNELS = "allowConfidentialOperationsOnInsecureChannels";

    // Attributes

    @ManagedAttribute( mandatory = true, description = "Port used to accept incoming connections. A value of 0"
                                                       + " will cause a dynamically allocated port to be assigned.")
    int getPort();

    @ManagedAttribute( defaultValue = "false", description = "If true allow operations which may return confidential "
                                                             + "information to be executed on insecure connections")
    boolean isAllowConfidentialOperationsOnInsecureChannels();

    @DerivedAttribute( description = "Actual port used to accept incoming connections. Will be -1 if the port"
                                     + " is not currently bound.")
    int getBoundPort();

    @ManagedAttribute
    Set<Protocol> getProtocols();

    @ManagedAttribute( defaultValue = "TCP" )
    Set<Transport> getTransports();

    @ManagedAttribute
    KeyStore getKeyStore();

    @ManagedAttribute
    Collection<TrustStore> getTrustStores();

    @DerivedAttribute
    List<String> getTlsProtocolWhiteList();

    @DerivedAttribute
    List<String> getTlsProtocolBlackList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteWhiteList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteBlackList();

    @ManagedAttribute(defaultValue = "*",
                      description = "The network interface this port binds to expressed as an IP address or a"
                                    + "hostname.  If null or * then bind to all interfaces.")
    String getBindingAddress();

    SSLContext getSSLContext();

    @ManagedAttribute
    boolean getNeedClientAuth();

    @ManagedAttribute
    boolean getWantClientAuth();

    @ManagedAttribute
    TrustStore<?> getClientCertRecorder();

    @ManagedAttribute( mandatory = true )
    AuthenticationProvider<?> getAuthenticationProvider();

    @DerivedAttribute(description = "TLS session timeout in seconds")
    int getTLSSessionTimeout();

    @DerivedAttribute(description = "TLS session cache size")
    int getTLSSessionCacheSize();

    Collection<Connection> getConnections();

    void start();

    ListenableFuture<Void> startAsync();

    NamedAddressSpace getAddressSpace(String name);

    SubjectCreator getSubjectCreator(final boolean secure, String host);

    @DerivedAttribute(description = "Indicates whether TLS transport support is created.")
    boolean isTlsSupported();

    @ManagedOperation(description =
            "Updates port TLS support without affecting existing connections."
            + " The TLS changes are applied to new connections only."
            + " Returns true if TLS support is successfully updated.",
            nonModifying = true,
            changesConfiguredObjectState = false)
    boolean updateTLS();
}
