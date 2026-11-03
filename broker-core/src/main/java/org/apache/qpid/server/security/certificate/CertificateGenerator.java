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
 * Implementations must be thread-safe, as one instance may be used concurrently.
 */
public interface CertificateGenerator
{
    /**
     * Generates a new key pair and a self-signed certificate for its public key.
     * <p>
     * The certificate is X.509 version 3 and DER encoded, so that the X.509
     * {@link java.security.cert.CertificateFactory} of the JVM can parse it. It has:
     * <ul>
     *     <li>the subject of the request as both subject and issuer;</li>
     *     <li>the validity period of the request, unchanged;</li>
     *     <li>a positive serial number of at most 20 octets, unique to the call;</li>
     *     <li>a critical basic constraints extension marking it as an end entity, not a CA;</li>
     *     <li>only when the request has at least one name or address, a non-critical subject alternative name
     *     extension containing exactly the names and addresses of the request.</li>
     * </ul>
     * Further extensions may be added, but none that is critical and unknown to that certificate factory.
     * <p>
     * Failures are reported as {@link GeneralSecurityException}. Implementations convert the unchecked failures
     * they expect from a security provider or a library, keeping the original as the cause: for example the
     * {@link java.security.InvalidParameterException} of an unsupported key length, a
     * {@link java.security.ProviderException}, or a {@link LinkageError} from library code first used while
     * generating. Other unchecked exceptions indicate a bug and are not converted.
     *
     * @param request certificate parameters
     * @return private key and certificate
     * @throws java.security.NoSuchAlgorithmException if the key or signature algorithm is not supported, with a
     * message naming the algorithm as requested
     * @throws GeneralSecurityException if the key pair or the certificate cannot be generated for another reason,
     * including a key length the key algorithm does not support or a signature algorithm that does not match the key
     */
    GeneratedCertificate generate(CertificateRequest request) throws GeneralSecurityException;
}
