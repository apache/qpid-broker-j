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

package org.apache.qpid.server.security.certificate;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Private key and self-signed certificate produced by a {@link CertificateGenerator}.
 *
 * @param privateKey private key matching the public key of the certificate
 * @param certificate self-signed certificate
 */
public record GeneratedCertificate(PrivateKey privateKey, X509Certificate certificate)
{
    public GeneratedCertificate
    {
        Objects.requireNonNull(privateKey, "Private key must not be null");
        Objects.requireNonNull(certificate, "Certificate must not be null");
    }

    /** Leaves out the private key, whose own string form depends on its provider. */
    @Override
    public String toString()
    {
        return "GeneratedCertificate[subject=" + certificate.getSubjectX500Principal().getName()
               + ", serialNumber=" + certificate.getSerialNumber().toString(16) + "]";
    }
}
