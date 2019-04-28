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
package org.apache.qpid.test.utils;

import java.security.Key;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Base64;

public class TestSSLUtils
{
    public static String certificateToPEM(final Certificate pub) throws CertificateEncodingException
    {
        return toPEM(pub.getEncoded(), "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----");
    }

    public static String privateKeyToPEM(final Key key)
    {
        return toPEM(key.getEncoded(), "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----");
    }

    private static String toPEM(final byte[] bytes, final String header, final String footer)
    {
        StringBuilder pem = new StringBuilder();
        pem.append(header).append("\n");
        String base64encoded = Base64.getEncoder().encodeToString(bytes);
        while (base64encoded.length() > 76)
        {
            pem.append(base64encoded, 0, 76).append("\n");
            base64encoded = base64encoded.substring(76);
        }
        pem.append(base64encoded).append("\n");
        pem.append(footer).append("\n");
        return pem.toString();
    }
}
