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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

import org.apache.qpid.server.security.auth.database.PlainPasswordFilePrincipalDatabase;

public class ScramSaslServerSourceAdapter implements ScramSaslServerSource
{
    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};

    private final int _iterationCount;
    private final String _hmacName;
    private final SecureRandom _random = new SecureRandom();
    private final PasswordSource _passwordSource;

    public interface PasswordSource
    {
        char[] getPassword(String username);
    }

    public ScramSaslServerSourceAdapter(final int iterationCount,
                                        final String hmacName,
                                        final PasswordSource passwordSource)
    {
        _iterationCount = iterationCount;
        _hmacName = hmacName;
        _passwordSource = passwordSource;
    }

    @Override
    public int getIterationCount()
    {
        return _iterationCount;
    }

    @Override
    public SaltAndSaltedPassword getSaltAndSaltedPassword(final String username)
    {
        final char[] password = _passwordSource.getPassword(username);

        final byte[] salt = new byte[32];
        _random.nextBytes(salt);
        return new SaltAndSaltedPassword()
        {
            @Override
            public byte[] getSalt()
            {
                return salt;
            }

            @Override
            public byte[] getSaltedPassword() throws SaslException
            {
                if(password == null)
                {
                    throw new SaslException("Authentication Failed");
                }
                byte[] passwordAsBytes = new byte[password.length];
                for(int i = 0; i< password.length; i++)
                {
                    passwordAsBytes[i] = (byte) password[i];
                }
                Mac mac = createShaHmac(passwordAsBytes);

                mac.update(salt);
                mac.update(INT_1);
                byte[] result = mac.doFinal();

                byte[] previous = null;
                for(int i = 1; i < getIterationCount(); i++)
                {
                    mac.update(previous != null? previous: result);
                    previous = mac.doFinal();
                    for(int x = 0; x < result.length; x++)
                    {
                        result[x] ^= previous[x];
                    }
                }

                return result;

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

        };
    }
}
