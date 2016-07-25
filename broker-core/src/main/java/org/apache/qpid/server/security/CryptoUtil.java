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
 */

package org.apache.qpid.server.security;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class CryptoUtil
{
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    public static String sha256Hex(final String... content)
    {
        MessageDigest md;
        try
        {
            md = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("JVM is non compliant. Seems to not support SHA-256.");
        }

        byte[] credentialDigest;
        try
        {
            for (String part : content)
            {
                md.update(part.getBytes(UTF8));
            }
            credentialDigest = md.digest();
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("JVM is non compliant. Seems to not support UTF-8.");
        }
        return DatatypeConverter.printHexBinary(credentialDigest);
    }

}
