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
import java.util.Objects;

import javax.crypto.SecretKey;

public record SecretKeyEntry(String alias, SecretKey secretKey) implements KeyStoreEntry
{
    public SecretKeyEntry
    {
        Objects.requireNonNull(alias, "alias must not be null");
        Objects.requireNonNull(secretKey, "secretKey must not be null");
    }

    @Override
    public void addToKeyStore(final KeyStore keyStore, char[] secret) throws KeyStoreException
    {
        keyStore.setKeyEntry(alias(), secretKey(), secret, null);
    }
}
