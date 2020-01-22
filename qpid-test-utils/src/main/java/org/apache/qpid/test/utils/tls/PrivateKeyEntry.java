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

public final class PrivateKeyEntry implements KeyStoreEntry
{
    private final String _alias;
    private final PrivateKey _privateKey;
    private final Certificate[] _certificates;

    public PrivateKeyEntry(final String alias, final PrivateKey privateKey, Certificate... certificate)
    {
        _alias = alias;
        _privateKey = privateKey;
        _certificates = certificate;
    }

    String getAlias()
    {
        return _alias;
    }

    @Override
    public void addEntryToKeyStore(final KeyStore keyStore, final char[] secret) throws KeyStoreException
    {
        keyStore.setKeyEntry(getAlias(),
                       getPrivateKey(),
                             secret,
                       getCertificates());
    }

    PrivateKey getPrivateKey()
    {
        return _privateKey;
    }

    Certificate[] getCertificates()
    {
        return _certificates;
    }

}
