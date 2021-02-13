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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

public class EncryptionHelper
{

    public static boolean isValidBase64(final String encrypted)
    {
        return encrypted.matches("^([\\w\\d+/]{4})*([\\w\\d+/]{2}==|[\\w\\d+/]{3}=)?$");
    }


    public static byte[] readFromCipherStream(final byte[] unencryptedBytes, final Cipher cipher) throws IOException
    {
        return readFromCipherStream(unencryptedBytes, 0, unencryptedBytes.length, cipher);
    }

    public static byte[] readFromCipherStream(final byte[] unencryptedBytes,
                                              int offset,
                                              int length,
                                              final Cipher cipher)
            throws IOException
    {
        final byte[] encryptedBytes;
        try (CipherInputStream cipherInputStream = new CipherInputStream(new ByteArrayInputStream(unencryptedBytes,
                                                                                                  offset,
                                                                                                  length), cipher))
        {
            byte[] buf = new byte[512];
            int pos = 0;
            int read;
            while ((read = cipherInputStream.read(buf, pos, buf.length - pos)) != -1)
            {
                pos += read;
                if (pos == buf.length)
                {
                    byte[] tmp = buf;
                    buf = new byte[buf.length + 512];
                    System.arraycopy(tmp, 0, buf, 0, tmp.length);
                }
            }
            encryptedBytes = new byte[pos];
            System.arraycopy(buf, 0, encryptedBytes, 0, pos);
        }
        return encryptedBytes;
    }
}
