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
package org.apache.qpid.server.security.auth.sasl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;

public class SaslUtil
{
    public static byte[] generatePlainClientResponse(final String userName, final String userPassword)
    {
        final byte[] password = userPassword.getBytes(StandardCharsets.UTF_8);
        final byte[] user = userName.getBytes(StandardCharsets.UTF_8);
        final byte[] response = new byte[password.length + user.length + 2];
        int size = 0;
        final byte SEPARATOR = 0;
        response[size++] = SEPARATOR;
        System.arraycopy(user, 0, response, size, user.length);
        size += user.length;
        response[size++] = SEPARATOR;
        System.arraycopy(password, 0, response, size, password.length);
        return response;
    }

    public static byte[] generateCramMD5HexClientResponse(final String userName,
                                                          final String userPassword,
                                                          final byte[] challengeBytes) throws Exception
    {
        final String macAlgorithm = "HmacMD5";
        final byte[] digestedPasswordBytes = MessageDigest.getInstance("MD5")
                .digest(userPassword.getBytes(StandardCharsets.UTF_8));
        final byte[] hexEncodedDigestedPasswordBytes = toHex(digestedPasswordBytes).getBytes(StandardCharsets.UTF_8);
        final Mac mac = Mac.getInstance(macAlgorithm);
        mac.init(new SecretKeySpec(hexEncodedDigestedPasswordBytes, macAlgorithm));
        final byte[] messageAuthenticationCode = mac.doFinal(challengeBytes);
        final String responseAsString = userName + " " + toHex(messageAuthenticationCode);
        return responseAsString.getBytes();
    }

    public static byte[] generateCramMD5HashedClientResponse(final String userName,
                                                             final String userPassword,
                                                             final byte[] challengeBytes) throws Exception
    {
        final char[] hash = toMD5Hashed(userPassword);
        return generateCramMD5ClientResponse(userName, new String(hash), challengeBytes);
    }

    public static char[] toMD5Hashed(final String userPassword)
            throws NoSuchAlgorithmException
    {
        final byte[] digestedPasswordBytes = MessageDigest.getInstance("MD5")
                .digest(userPassword.getBytes(StandardCharsets.UTF_8));
        final char[] hash = new char[digestedPasswordBytes.length];
        int index = 0;
        for (byte b : digestedPasswordBytes)
        {
            hash[index++] = (char) b;
        }
        return hash;
    }

    public static byte[] generateCramMD5ClientResponse(final String userName,
                                                       final String userPassword,
                                                       final byte[] challengeBytes) throws Exception
    {
        final String macAlgorithm = "HmacMD5";
        final Mac mac = Mac.getInstance(macAlgorithm);
        mac.init(new SecretKeySpec(userPassword.getBytes(StandardCharsets.UTF_8), macAlgorithm));
        final byte[] messageAuthenticationCode = mac.doFinal(challengeBytes);
        final String responseAsString = userName + " " + toHex(messageAuthenticationCode);
        return responseAsString.getBytes();
    }

    public static byte[] generateCramMD5ClientResponse(final String mechanism,
                                                       final String userName,
                                                       final String userPassword,
                                                       final byte[] challengeBytes) throws Exception
    {
        if (CramMd5Negotiator.MECHANISM.equals(mechanism))
        {
            return generateCramMD5ClientResponse(userName, userPassword, challengeBytes);
        }
        else if (CramMd5HexNegotiator.MECHANISM.equals(mechanism))
        {
            return generateCramMD5HexClientResponse(userName, userPassword, challengeBytes);
        }
        else if (CramMd5HashedNegotiator.MECHANISM.equals(mechanism))
        {
            return generateCramMD5HashedClientResponse(userName, userPassword, challengeBytes);
        }
        throw new IllegalArgumentException(String.format("Unsupported mechanism '%s'", mechanism));
    }

    public static String toHex(final byte[] data)
    {
        final StringBuilder hash = new StringBuilder();
        for (final byte datum : data)
        {
            final String hex = Integer.toHexString(0xFF & datum);
            if (hex.length() == 1)
            {
                hash.append('0');
            }
            hash.append(hex);
        }
        return hash.toString();
    }
}
