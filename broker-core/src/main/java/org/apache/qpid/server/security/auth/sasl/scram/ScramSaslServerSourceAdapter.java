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
package org.apache.qpid.server.security.auth.sasl.scram;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

import org.apache.qpid.server.security.auth.sasl.PasswordSource;

public class ScramSaslServerSourceAdapter implements ScramSaslServerSource
{
    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};

    private final int _iterationCount;
    private final String _hmacName;
    private final SecureRandom _random = new SecureRandom();
    private final PasswordSource _passwordSource;
    private final String _digestName;

    public ScramSaslServerSourceAdapter(final int iterationCount,
                                        final String hmacName,
                                        final String digestName,
                                        final PasswordSource passwordSource)
    {
        _iterationCount = iterationCount;
        _hmacName = hmacName;
        _passwordSource = passwordSource;
        _digestName = digestName;
    }

    @Override
    public int getIterationCount()
    {
        return _iterationCount;
    }

    @Override
    public String getDigestName()
    {
        return _digestName;
    }

    @Override
    public String getHmacName()
    {
        return _hmacName;
    }

    private Mac createShaHmac(final byte[] keyBytes)
    {
        try
        {
            SecretKeySpec key = new SecretKeySpec(keyBytes, _hmacName);
            Mac mac = Mac.getInstance(_hmacName);
            mac.init(key);
            return mac;
        }
        catch (NoSuchAlgorithmException | InvalidKeyException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private byte[] computeHmac(final byte[] key, final String string)
    {
        Mac mac = createShaHmac(key);
        mac.update(string.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }

    @Override
    public SaltAndPasswordKeys getSaltAndPasswordKeys(final String username)
    {
        final char[] password = _passwordSource.getPassword(username);
        final byte[] storedKey;
        final byte[] serverKey;
        final byte[] salt = new byte[32];
        final int iterationCount = getIterationCount();
        _random.nextBytes(salt);

        if(password != null)
        {
            try
            {
                byte[] passwordAsBytes = new byte[password.length];
                for (int i = 0; i < password.length; i++)
                {
                    passwordAsBytes[i] = (byte) password[i];
                }

                Mac mac = createShaHmac(passwordAsBytes);

                mac.update(salt);
                mac.update(INT_1);
                byte[] saltedPassword = mac.doFinal();

                byte[] previous = null;
                for (int i = 1; i < iterationCount; i++)
                {
                    mac.update(previous != null ? previous : saltedPassword);
                    previous = mac.doFinal();
                    for (int x = 0; x < saltedPassword.length; x++)
                    {
                        saltedPassword[x] ^= previous[x];
                    }
                }

                byte[] clientKey = computeHmac(saltedPassword, "Client Key");

                storedKey = MessageDigest.getInstance(_digestName).digest(clientKey);

                serverKey = computeHmac(saltedPassword, "Server Key");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new IllegalArgumentException(e);
            }

        }
        else
        {
            storedKey = null;
            serverKey = null;
        }

        return new SaltAndPasswordKeys()
        {
            @Override
            public byte[] getSalt()
            {
                return salt;
            }

            @Override
            public byte[] getStoredKey() throws SaslException
            {
                if(storedKey == null)
                {
                    throw new SaslException("Authentication Failed");
                }
                return storedKey;
            }

            @Override
            public byte[] getServerKey() throws SaslException
            {

                if(serverKey == null)
                {
                    throw new SaslException("Authentication Failed");
                }
                return serverKey;
            }

            @Override
            public int getIterationCount() throws SaslException
            {
                return iterationCount;
            }


        };
    }
}
