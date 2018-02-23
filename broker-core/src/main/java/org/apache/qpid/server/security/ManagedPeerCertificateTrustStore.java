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

import java.security.cert.Certificate;
import java.util.List;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject(category = false, type = ManagedPeerCertificateTrustStore.TYPE_NAME,
        description = "Stores multiple PEM or DER encoded certificates in the broker configuration which the Trust Store will trust for secure connections (e.g., HTTPS or AMQPS)")
public interface ManagedPeerCertificateTrustStore<X extends ManagedPeerCertificateTrustStore<X>> extends TrustStore<X>,
                                                                                                         MutableCertificateTrustStore

{

    String TYPE_NAME = "ManagedCertificateStore";
    String STORED_CERTIFICATES = "storedCertificates";


    @Override
    @ManagedAttribute(defaultValue = "true")
    boolean isExposedAsMessageSource();

    @ManagedAttribute(oversize = true, defaultValue = "[]", description = "List of base64 encoded representations of the ASN.1 DER encoded certificates")
    List<Certificate> getStoredCertificates();

    @ManagedOperation(description = "Add a given certificate to the Trust Store",
            changesConfiguredObjectState = true)
    void addCertificate(@Param(name = "certificate",
                               description = "PEM or base64 encoded DER certificate to be added to the Trust Store",
                               mandatory = true)
                        Certificate certificate);

    @ManagedOperation(description = "Remove given certificates from the Trust Store.",
            changesConfiguredObjectState = true)
    void removeCertificates(@Param(name = "certificates", description = "List of certificate details to be removed. The details should take the form given by the certificateDetails attribute", mandatory = true)
                            List<CertificateDetails> certificates);
}
