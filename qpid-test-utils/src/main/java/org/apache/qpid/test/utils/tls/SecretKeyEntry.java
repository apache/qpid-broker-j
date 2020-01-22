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

import javax.crypto.SecretKey;

public class SecretKeyEntry implements KeyStoreEntry
{
    private final String _alias;
    private final SecretKey _secretKey;

    public SecretKeyEntry(final String alias, final SecretKey secretKey)
    {
        _alias = alias;
        _secretKey = secretKey;
    }

    @Override
    public void addEntryToKeyStore(final KeyStore keyStore, char[] secret) throws KeyStoreException
    {
        keyStore.setKeyEntry(getAlias(), getSecretKey(), secret, null);
    }

    public String getAlias()
    {
        return _alias;
    }

    public SecretKey getSecretKey()
    {
        return _secretKey;
    }

}
