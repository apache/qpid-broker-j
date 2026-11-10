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
 * load third-party libraries, create or register security providers. Only the factory of the selected type has its
 * {@link #create()} method called.
 */
public interface CertificateGeneratorFactory extends Pluggable
{
    /**
     * Creates the certificate generator.
     *
     * Implementations should check here that the libraries the generator requires are present and compatible, and
     * let the resulting {@link LinkageError}, such as {@link NoClassDefFoundError}, propagate when they are not.
     *
     * @return certificate generator
     * @throws GeneralSecurityException if the generator cannot be initialised
     */
    CertificateGenerator create() throws GeneralSecurityException;
}
