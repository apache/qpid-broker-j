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

import java.security.GeneralSecurityException;

/**
 * Generates a private key and a self-signed X.509 certificate.
 * <p>
 * Implementations should obtain key pair generators, signatures and secure random instances through the default
 * JCA provider selection, so that the cryptographic providers remain the choice of the JVM configuration.
 * Implementations must not register security providers.
 * <p>
 * Implementations must be thread-safe, because one generator is shared by the JVM.
 */
public interface CertificateGenerator
{
    /**
     * Generates a key pair and a self-signed certificate for its public key.
     *
     * @param request certificate parameters
     * @return private key and certificate
     * @throws java.security.NoSuchAlgorithmException if the key or signature algorithm is not supported
     * @throws GeneralSecurityException if the key pair or the certificate cannot be generated for another reason
     */
    GeneratedCertificate generate(CertificateRequest request) throws GeneralSecurityException;
}
