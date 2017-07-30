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

import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.util.List;

import javax.net.ssl.TrustManager;

import org.apache.qpid.server.security.CertificateDetails;

@ManagedObject( defaultType = "FileTrustStore" )
public interface TrustStore<X extends TrustStore<X>> extends ConfiguredObject<X>
{
    String CERTIFICATE_EXPIRY_WARN_PERIOD = "qpid.truststore.certificateExpiryWarnPeriod";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_WARN_PERIOD)
    int DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD = 30;

    String CERTIFICATE_EXPIRY_CHECK_FREQUENCY = "qpid.truststore.certificateExpiryCheckFrequency";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_CHECK_FREQUENCY)
    int DEFAULT_CERTIFICATE_EXPIRY_CHECK_FREQUENCY = 1;

    @Override
    @ManagedAttribute(immutable = true)
    String getName();

    @ManagedAttribute( defaultValue = "false", description = "If true the Trust Store will expose its certificates as a special artificial message source.")
    boolean isExposedAsMessageSource();

    @ManagedAttribute( defaultValue = "[]", description = "If 'exposedAsMessageSource' is true, the trust store will expose its certificates only to VirtualHostNodes in this list or if this list is empty to all VirtualHostNodes who are not in the 'excludedVirtualHostNodeMessageSources' list." )
    List<VirtualHostNode<?>> getIncludedVirtualHostNodeMessageSources();

    @ManagedAttribute( defaultValue = "[]", description = "If 'exposedAsMessageSource' is true and 'includedVirtualHostNodeMessageSources' is empty, the trust store will expose its certificates only to VirtualHostNodes who are not in this list." )
    List<VirtualHostNode<?>> getExcludedVirtualHostNodeMessageSources();

    @DerivedAttribute(description = "List of details about the certificates like validity dates, SANs, issuer and subject names, etc.")
    List<CertificateDetails> getCertificateDetails();

    @DerivedAttribute
    int getCertificateExpiryWarnPeriod();

    @DerivedAttribute
    int getCertificateExpiryCheckFrequency();

    TrustManager[] getTrustManagers() throws GeneralSecurityException;

    Certificate[] getCertificates() throws GeneralSecurityException;

}
