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
package org.apache.qpid.server.security;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject(category = false, type = "SiteSpecificTrustStore",
        description = "Obtains a SSL/TLS certificate from a given URL which the Trust Store will trust for secure connections (e.g., HTTPS or AMQPS)")
public interface SiteSpecificTrustStore<X extends SiteSpecificTrustStore<X>> extends TrustStore<X>
{
    String CERTIFICATE = "certificate";

    String TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT = "qpid.trustStore.siteSpecific.connectTimeout";
    @ManagedContextDefault(name = TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT)
    int DEFAULT_TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT = 60000;

    String TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT = "qpid.trustStore.siteSpecific.readTimeout";
    @ManagedContextDefault(name = TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT)
    int DEFAULT_TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT = 60000;

    @ManagedAttribute(immutable = true, description = "The URL from which to obtain the trusted certificate. Example: https://example.com or https://example.com:8443")
    String getSiteUrl();

    @DerivedAttribute(persist = true, description = "The X.509 certificate obtained from the given URL as base64 encoded representation of the ASN.1 DER encoding")
    String getCertificate();

    @ManagedOperation(description = "Re-download the certificate from the URL",
            changesConfiguredObjectState = false /* This should really be true but pragmatically it is set to false because we do not want to block the config thread while getting the certificate from the remote host */)
    void refreshCertificate();
}
