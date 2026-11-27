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
package org.apache.qpid.server.security.certificate.bouncycastle;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.certificate.CertificateGenerator;
import org.apache.qpid.server.security.certificate.CertificateGeneratorFactory;

/**
 * Creates certificate generators that use the Bouncy Castle certificate API (bcpkix, which needs bcprov and bcutil).
 * <p>
 * This class must not refer to Bouncy Castle types, so that it can be loaded and asked for its type when the
 * libraries are missing. Bouncy Castle is only used by {@link BouncyCastleCertificateGenerator}.
 */
@SuppressWarnings("unused")
@PluggableService
public class BouncyCastleCertificateGeneratorFactory implements CertificateGeneratorFactory
{
    public static final String TYPE = "BouncyCastle";

    @Override
    public String getType()
    {
        return TYPE;
    }

    /**
     * @throws LinkageError if a Bouncy Castle library is missing from the classpath or incompatible
     */
    @Override
    public CertificateGenerator create()
    {
        // a missing or incompatible library is reported on selection rather than on the first certificate generation
        BouncyCastleCertificateGenerator.checkLibraries();
        return new BouncyCastleCertificateGenerator();
    }
}
