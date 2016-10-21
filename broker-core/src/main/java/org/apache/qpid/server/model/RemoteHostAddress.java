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

@ManagedObject( defaultType = RemoteHostAddress.REMOTE_HOST_ADDRESS_TYPE)
public interface RemoteHostAddress<X extends RemoteHostAddress<X>> extends ConfiguredObject<X>
{

    String REMOTE_HOST_ADDRESS_TYPE = "Standard";

    @ManagedAttribute(mandatory = true)
    String getAddress();

    @ManagedAttribute(mandatory = true)
    int getPort();

    @ManagedAttribute
    String getHostName();

    @ManagedAttribute
    Protocol getProtocol();

    @ManagedAttribute( defaultValue = "TCP" )
    Transport getTransport();

    @ManagedAttribute
    KeyStore getKeyStore();

    @ManagedAttribute
    Collection<TrustStore> getTrustStores();

    @ManagedAttribute(defaultValue = "0")
    int getDesiredHeartbeatInterval();


    @DerivedAttribute
    List<String> getTlsProtocolWhiteList();

    @DerivedAttribute
    List<String> getTlsProtocolBlackList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteWhiteList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteBlackList();

}
