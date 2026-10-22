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

import org.apache.qpid.server.plugin.Pluggable;

/**
 * Factory for a {@link CertificateGenerator}, discovered with {@link org.apache.qpid.server.plugin.QpidServiceLoader}.
 * <p>
 * Implementations must have a public no-argument constructor without side effects: discovering a factory must not
 * load third-party libraries, create or register security providers. The constructor and {@link #getType()} must not
 * fail, even when the libraries the generator requires are missing: work that can fail belongs in {@link #create()}.
 */
public interface CertificateGeneratorFactory extends Pluggable
{
    /**
     * Creates the certificate generator.
     * <p>
     * Implementations should use the classes the generator requires here, so that a missing or incompatible library
     * fails now with a {@link LinkageError}, such as {@link NoClassDefFoundError}, rather than when a certificate is
     * generated. This method lets such errors propagate rather than converting them to
     * {@link GeneralSecurityException}.
     *
     * @return certificate generator
     * @throws GeneralSecurityException if the generator cannot be initialised
     */
    CertificateGenerator create() throws GeneralSecurityException;
}
