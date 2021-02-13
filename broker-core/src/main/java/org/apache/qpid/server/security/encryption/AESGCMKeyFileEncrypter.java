/*
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


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.apache.qpid.server.util.Strings;

class AESGCMKeyFileEncrypter implements ConfigurationSecretEncrypter
{
    private static final String CIPHER_NAME = "AES/GCM/NoPadding";
    private static final int GCM_INITIALIZATION_VECTOR_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;
    private static final String AES_ALGORITHM = "AES";
    private final SecureRandom _random = new SecureRandom();
    private final SecretKey _secretKey;

    AESGCMKeyFileEncrypter(SecretKey secretKey)
    {
        if (secretKey == null)
        {
            throw new IllegalArgumentException("A non null secret key must be supplied");
        }
        if (!AES_ALGORITHM.equals(secretKey.getAlgorithm()))
        {
            throw new IllegalArgumentException(String.format(
                    "Provided secret key was for the algorithm: %s when %s was needed.",
                    secretKey.getAlgorithm(),
                    AES_ALGORITHM));
        }
        _secretKey = secretKey;
    }

    @Override
    public String encrypt(final String unencrypted)
    {
        byte[] unencryptedBytes = unencrypted.getBytes(StandardCharsets.UTF_8);
        try
        {
            byte[] ivbytes = new byte[GCM_INITIALIZATION_VECTOR_LENGTH];
            _random.nextBytes(ivbytes);
            Cipher cipher = Cipher.getInstance(CIPHER_NAME);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, ivbytes);
            cipher.init(Cipher.ENCRYPT_MODE, _secretKey, gcmParameterSpec);
            byte[] encryptedBytes = EncryptionHelper.readFromCipherStream(unencryptedBytes, cipher);
            byte[] output = new byte[GCM_INITIALIZATION_VECTOR_LENGTH + encryptedBytes.length];
            System.arraycopy(ivbytes, 0, output, 0, GCM_INITIALIZATION_VECTOR_LENGTH);
            System.arraycopy(encryptedBytes, 0, output, GCM_INITIALIZATION_VECTOR_LENGTH, encryptedBytes.length);
            return Base64.getEncoder().encodeToString(output);
        }
        catch (IOException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            throw new IllegalArgumentException("Unable to encrypt secret", e);
        }
    }

    @Override
    public String decrypt(final String encrypted)
    {
        if (!EncryptionHelper.isValidBase64(encrypted))
        {
            throw new IllegalArgumentException("Encrypted value is not valid Base 64 data: '" + encrypted + "'");
        }
        byte[] encryptedBytes = Strings.decodeBase64(encrypted);
        try
        {
            Cipher cipher = Cipher.getInstance(CIPHER_NAME);
            byte[] ivbytes = new byte[GCM_INITIALIZATION_VECTOR_LENGTH];
            if (encryptedBytes.length >= GCM_INITIALIZATION_VECTOR_LENGTH)
            {
                System.arraycopy(encryptedBytes, 0, ivbytes, 0, GCM_INITIALIZATION_VECTOR_LENGTH);
            }
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, ivbytes);
            cipher.init(Cipher.DECRYPT_MODE, _secretKey, gcmParameterSpec);
            return new String(EncryptionHelper.readFromCipherStream(encryptedBytes,
                                                                    GCM_INITIALIZATION_VECTOR_LENGTH,
                                                                    encryptedBytes.length
                                                                    - GCM_INITIALIZATION_VECTOR_LENGTH,
                                                                    cipher), StandardCharsets.UTF_8);
        }
        catch (IOException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            throw new IllegalArgumentException("Unable to decrypt secret", e);
        }
    }
}
