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
    String TRUST_ANCHOR_VALIDITY_ENFORCED = "trustAnchorValidityEnforced";

    String CERTIFICATE_EXPIRY_WARN_PERIOD = "qpid.truststore.certificateExpiryWarnPeriod";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_WARN_PERIOD,
            description = "The number of days before a certificate's expiry"
                          + " that certificate expiration warnings will be written to the log")
    int DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD = 30;

    String CERTIFICATE_EXPIRY_CHECK_FREQUENCY = "qpid.truststore.certificateExpiryCheckFrequency";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_CHECK_FREQUENCY,
            description = "Period (in days) with which the Broker will repeat the certificate expiration warning")
    int DEFAULT_CERTIFICATE_EXPIRY_CHECK_FREQUENCY = 1;
    @ManagedContextDefault(name = "qpid.truststore.trustAnchorValidityEnforced")
    boolean DEFAULT_TRUST_ANCHOR_VALIDITY_ENFORCED = false;

    String CERTIFICATE_REVOCATION_CHECK_ENABLED = "certificateRevocationCheckEnabled";
    String CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES =
            "certificateRevocationCheckWithIgnoringSoftFailures";
    String CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST =
            "certificateRevocationCheckWithPreferringCertificateRevocationList";
    String CERTIFICATE_REVOCATION_CHECK_WITH_NO_FALLBACK = "certificateRevocationCheckWithNoFallback";
    String CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES =
            "certificateRevocationCheckOfOnlyEndEntityCertificates";
    String CERTIFICATE_REVOCATION_LIST_URL = "certificateRevocationListUrl";

    @Override
    @ManagedAttribute(immutable = true)
    String getName();

    @ManagedAttribute(defaultValue = "false", description = "If true the Trust Store will expose its certificates as a special artificial message source.")
    boolean isExposedAsMessageSource();

    @ManagedAttribute(defaultValue = "[]", description = "If 'exposedAsMessageSource' is true, the trust store will expose its certificates only to VirtualHostNodes in this list or if this list is empty to all VirtualHostNodes who are not in the 'excludedVirtualHostNodeMessageSources' list." )
    List<VirtualHostNode<?>> getIncludedVirtualHostNodeMessageSources();

    @ManagedAttribute(defaultValue = "[]", description = "If 'exposedAsMessageSource' is true and 'includedVirtualHostNodeMessageSources' is empty, the trust store will expose its certificates only to VirtualHostNodes who are not in this list." )
    List<VirtualHostNode<?>> getExcludedVirtualHostNodeMessageSources();

    @ManagedAttribute(defaultValue = "${qpid.truststore.trustAnchorValidityEnforced}",
                       description = "If true, the trust anchor's validity dates will be enforced.")
    boolean isTrustAnchorValidityEnforced();

    @ManagedAttribute(defaultValue = "false", description = "If true, enable certificates revocation.")
    boolean isCertificateRevocationCheckEnabled();

    @ManagedAttribute(defaultValue = "false", description = "If true, check the revocation status of only end-entity certificates.")
    boolean isCertificateRevocationCheckOfOnlyEndEntityCertificates();

    @ManagedAttribute(defaultValue = "true", description = "If true, prefer CRL (specified in certificate distribution points) to OCSP, if false prefer OCSP to CRL.")
    boolean isCertificateRevocationCheckWithPreferringCertificateRevocationList();

    @ManagedAttribute(defaultValue = "true", description = "If true, disable fallback to CRL/OCSP (if 'certificateRevocationCheckWithPreferringCertificateRevocationList' set to true, disable fallback to OCSP, otherwise disable fallback to CRL in certificate distribution points).")
    boolean isCertificateRevocationCheckWithNoFallback();

    @ManagedAttribute(defaultValue = "false", description = "If true, revocation check will succeed if CRL/OCSP response cannot be obtained because of network error or OCSP responder returns internalError or tryLater.")
    boolean isCertificateRevocationCheckWithIgnoringSoftFailures();

    @ManagedAttribute(oversize = true, description = "If set, certificates will be validated only against CRL file (CRL in distribution points and OCSP will be ignored).", oversizedAltText = OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT)
    String getCertificateRevocationListUrl();

    @DerivedAttribute
    String getCertificateRevocationListPath();

    @DerivedAttribute(description = "List of details about the certificates like validity dates, SANs, issuer and subject names, etc.")
    List<CertificateDetails> getCertificateDetails();

    @DerivedAttribute
    int getCertificateExpiryWarnPeriod();

    @DerivedAttribute
    int getCertificateExpiryCheckFrequency();

    TrustManager[] getTrustManagers() throws GeneralSecurityException;

    Certificate[] getCertificates() throws GeneralSecurityException;

}
