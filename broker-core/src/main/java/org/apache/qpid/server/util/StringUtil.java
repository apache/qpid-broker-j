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
package org.apache.qpid.server.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class StringUtil
{
    private static final String NUMBERS = "0123456789";
    private static final String LETTERS = "abcdefghijklmnopqrstuvwxwy";
    private static final String OTHERS = "_-";
    private static final char[] CHARACTERS = (NUMBERS + LETTERS + LETTERS.toUpperCase() + OTHERS).toCharArray();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private Random _random = new Random();

    public static String elideDataUrl(final String path)
    {
        return String.valueOf(path).toLowerCase().startsWith("data:") ? "data:..." : path;
    }

    public static String toHex(byte[] bin)
    {
        StringBuilder result = new StringBuilder(2 * bin.length);
        for (byte b : bin) {
            result.append(HEX[(b >> 4) & 0xF]);
            result.append(HEX[(b & 0xF)]);
        }
        return result.toString();
    }

    public String randomAlphaNumericString(int maxLength)
    {
        char[] result = new char[maxLength];
        for (int i = 0; i < maxLength; i++)
        {
            result[i] = (char) CHARACTERS[_random.nextInt(CHARACTERS.length)];
        }
        return new String(result);
    }

    /**
     * Builds a legal java name, based on manager name if possible,
     * this is unique for the given input.
     *
     * @param managerName
     * @return unique java name
     */
    public String createUniqueJavaName(String managerName)
    {
        StringBuilder builder = new StringBuilder();
        boolean initialChar = true;
        for (int i = 0; i < managerName.length(); i++)
        {
            char c = managerName.charAt(i);
            if ((initialChar && Character.isJavaIdentifierStart(c))
                    || (!initialChar && Character.isJavaIdentifierPart(c)))
            {
                builder.append(c);
                initialChar = false;
            }
        }
        if (builder.length() > 0)
        {
            builder.append("_");
        }
        try
        {
            byte[] digest = MessageDigest.getInstance("MD5").digest(managerName.getBytes(StandardCharsets.UTF_8));
            builder.append(toHex(digest).toLowerCase());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
        return builder.toString();
    }

}
