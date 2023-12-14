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

import static org.apache.qpid.server.security.encryption.AbstractAESKeyFileEncrypterFactoryTest.isStrongEncryptionEnabled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Unit test is deprecated due to deprecation of AESGCMKeyFileEncrypter, it will be deleted in one of the next releases
 */
@Deprecated(since = "9.1.1", forRemoval = true)
public class AESKeyFileEncrypterTest extends UnitTestBase
{
    private final SecureRandom _random = new SecureRandom();
    public static final String PLAINTEXT = "secret";
    private static SecretKeySpec secretKey;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(isStrongEncryptionEnabled());
        final byte[] keyData = new byte[32];
        _random.nextBytes(keyData);
        secretKey = new SecretKeySpec(keyData, "AES");
    }

    @Test
    public void testSimpleEncryptDecrypt()
    {
        doTestSimpleEncryptDecrypt(PLAINTEXT);
    }


    @Test
    public void testRepeatedEncryptionsReturnDifferentValues()
    {
        final AESKeyFileEncrypter encrypter = new AESKeyFileEncrypter(secretKey);

        final Set<String> encryptions = new HashSet<>();

        int iterations = 10;

        for (int i = 0; i < iterations; i++)
        {
            encryptions.add(encrypter.encrypt(PLAINTEXT));
        }

        assertEquals(iterations, (long) encryptions.size(), "Not all encryptions were distinct");

        for (final String encrypted : encryptions)
        {
            assertEquals(PLAINTEXT, encrypter.decrypt(encrypted), "Not all encryptions decrypt correctly");
        }
    }

    @Test
    public void testCreationFailsOnInvalidSecret() throws Exception
    {
        assertThrows(NullPointerException.class,
                () -> new AESKeyFileEncrypter(null),
                "An encrypter should not be creatable from a null key");

        final PBEKeySpec keySpec = new PBEKeySpec("password".toCharArray());
        final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");

        assertThrows(IllegalArgumentException.class,
                () -> new AESKeyFileEncrypter(factory.generateSecret(keySpec)),
                "An encrypter should not be creatable from the wrong type of secret key");
    }

    @Test
    public void testEncryptionOfEmptyString()
    {
        doTestSimpleEncryptDecrypt("");
    }

    private void doTestSimpleEncryptDecrypt(final String text)
    {
        final AESKeyFileEncrypter encrypter = new AESKeyFileEncrypter(secretKey);

        final String encrypted = encrypter.encrypt(text);
        assertNotNull(encrypted, "Encrypter did not return a result from encryption");
        assertNotEquals(text, encrypted, "Plain text and encrypted version are equal");
        final String decrypted = encrypter.decrypt(encrypted);
        assertNotNull(decrypted, "Encrypter did not return a result from decryption");
        assertEquals(text, decrypted, "Encryption was not reversible");
    }

    @Test
    public void testEncryptingNullFails()
    {
        final AESKeyFileEncrypter encrypter = new AESKeyFileEncrypter(secretKey);

        assertThrows(NullPointerException.class,
                () -> encrypter.encrypt(null),
                "Attempting to encrypt null should fail");
    }

    @Test
    public void testEncryptingVeryLargeSecret()
    {
        final Random random = new Random();
        final byte[] data = new byte[4096];
        random.nextBytes(data);
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) (data[i] & 0xEF);
        }
        doTestSimpleEncryptDecrypt(new String(data, StandardCharsets.US_ASCII));
    }

    @Test
    public void testDecryptNonsense()
    {
        final AESKeyFileEncrypter encrypter = new AESKeyFileEncrypter(secretKey);
        assertThrows(NullPointerException.class,
                () -> encrypter.decrypt(null),
                "Should not decrypt a null value");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt(""),
                "Should not decrypt the empty String");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt("thisisnonsense"),
                "Should not decrypt a small amount of nonsense");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt("thisisn'tvalidBase64!soitshouldfailwithanIllegalArgumentException"),
                "Should not decrypt a larger amount of nonsense");
    }
}
