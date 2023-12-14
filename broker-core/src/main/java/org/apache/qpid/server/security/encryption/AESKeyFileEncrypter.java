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
import javax.crypto.spec.IvParameterSpec;

import org.apache.qpid.server.util.Strings;

/**
 * Class is deprecated in favor of AESGCMKeyFileEncrypter, it will be deleted in one of the next releases
 */
@Deprecated(since = "9.1.1", forRemoval = true)
class AESKeyFileEncrypter implements ConfigurationSecretEncrypter
{
    private static final String CIPHER_NAME = "AES/CBC/PKCS5Padding";
    private static final int AES_INITIALIZATION_VECTOR_LENGTH = 16;
    private static final String AES_ALGORITHM = "AES";
    private final SecretKey _secretKey;
    private final SecureRandom _random = new SecureRandom();

    AESKeyFileEncrypter(SecretKey secretKey)
    {
        if(secretKey == null)
        {
            throw new NullPointerException("A non null secret key must be supplied");
        }
        if(!AES_ALGORITHM.equals(secretKey.getAlgorithm()))
        {
            throw new IllegalArgumentException("Provided secret key was for the algorithm: " + secretKey.getAlgorithm()
                                                + "when" + AES_ALGORITHM + "was needed.");
        }
        _secretKey = secretKey;
    }

    @Override
    public String encrypt(final String unencrypted)
    {
        byte[] unencryptedBytes = unencrypted.getBytes(StandardCharsets.UTF_8);
        try
        {
            byte[] ivbytes = new byte[AES_INITIALIZATION_VECTOR_LENGTH];
            _random.nextBytes(ivbytes);
            Cipher cipher = Cipher.getInstance(CIPHER_NAME);
            cipher.init(Cipher.ENCRYPT_MODE, _secretKey, new IvParameterSpec(ivbytes));
            byte[] encryptedBytes = EncryptionHelper.readFromCipherStream(unencryptedBytes, cipher);
            byte[] output = new byte[AES_INITIALIZATION_VECTOR_LENGTH + encryptedBytes.length];
            System.arraycopy(ivbytes, 0, output, 0, AES_INITIALIZATION_VECTOR_LENGTH);
            System.arraycopy(encryptedBytes, 0, output, AES_INITIALIZATION_VECTOR_LENGTH, encryptedBytes.length);
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
        if(!EncryptionHelper.isValidBase64(encrypted))
        {
            throw new IllegalArgumentException("Encrypted value is not valid Base 64 data: '" + encrypted + "'");
        }
        byte[] encryptedBytes = Strings.decodeBase64(encrypted);
        try
        {
            Cipher cipher = Cipher.getInstance(CIPHER_NAME);

            IvParameterSpec ivParameterSpec = new IvParameterSpec(encryptedBytes, 0, AES_INITIALIZATION_VECTOR_LENGTH);

            cipher.init(Cipher.DECRYPT_MODE, _secretKey, ivParameterSpec);

            return new String(EncryptionHelper.readFromCipherStream(encryptedBytes,
                                                   AES_INITIALIZATION_VECTOR_LENGTH,
                                                   encryptedBytes.length - AES_INITIALIZATION_VECTOR_LENGTH,
                                                   cipher), StandardCharsets.UTF_8);
        }
        catch (IOException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            throw new IllegalArgumentException("Unable to decrypt secret", e);
        }
    }


}
