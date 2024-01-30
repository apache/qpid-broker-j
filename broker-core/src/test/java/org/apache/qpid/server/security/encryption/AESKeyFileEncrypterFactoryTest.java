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
package org.apache.qpid.server.security.encryption;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.security.SecureRandom;

import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Unit test is deprecated due to deprecation of AESKeyFileEncrypterFactory, it will be deleted in one of the next releases
 */
@Deprecated(since = "9.2.0", forRemoval = true)
public class AESKeyFileEncrypterFactoryTest extends UnitTestBase
{
    private AESKeyFileEncrypterFactory _factory;

    @BeforeEach
    public void setUp() throws Exception
    {
        _factory = new AESKeyFileEncrypterFactory();
    }

    @Test
    public void testGetType()
    {
        assertThat(_factory.getType(), is(equalTo(AESKeyFileEncrypterFactory.TYPE)));
    }

    @Test
    public void testCreateEncrypter()
    {
        final SecretKeySpec secretKey = createSecretKey();
        final ConfigurationSecretEncrypter encrypter = _factory.createEncrypter(secretKey);
        assertThat(encrypter, is(instanceOf(AESKeyFileEncrypter.class)));
    }

    static SecretKeySpec createSecretKey()
    {
        final byte[] keyData = new byte[32];
        new SecureRandom().nextBytes(keyData);
        return new SecretKeySpec(keyData, "AES");
    }
}
