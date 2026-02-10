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

package org.apache.qpid.test.utils.tls;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Objects;

public record PrivateKeyEntry(String alias, PrivateKey privateKey, Certificate... certificateChain) implements KeyStoreEntry
{
    public PrivateKeyEntry
    {
        Objects.requireNonNull(alias, "alias must not be null");
        Objects.requireNonNull(privateKey, "privateKey must not be null");
        Objects.requireNonNull(certificateChain, "certificateChain must not be null");
        certificateChain = certificateChain.clone();
    }

    @Override
    public Certificate[] certificateChain()
    {
        return certificateChain.clone();
    }

    public PrivateKeyEntry(String alias, KeyCertificatePair keyCertPair)
    {
        this(alias, keyCertPair.privateKey(), new Certificate[]{ keyCertPair.certificate() });
    }

    @Override
    public void addToKeyStore(final KeyStore keyStore, final char[] secret) throws KeyStoreException
    {
        keyStore.setKeyEntry(alias, privateKey, secret, certificateChain);
    }
}
