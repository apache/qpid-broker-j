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

package org.apache.qpid.server.security.auth.sasl.crammd5;

import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;

public class CramMd5HexNegotiator extends AbstractCramMd5Negotiator
{
    public static final String MECHANISM = "CRAM-MD5-HEX";
    private static final PasswordTransformer HEX_PASSWORD_TRANSFORMER = password ->
    {
        StringBuilder sb = new StringBuilder();
        for (char c : password)
        {
            //toHexString does not prepend 0 so we have to
            if (((byte) c > -1) && (byte) c < 0x10)
            {
                sb.append(0);
            }
            sb.append(Integer.toHexString(c & 0xFF));
        }

        //Extract the hex string as char[]
        char[] hex = new char[sb.length()];
        sb.getChars(0, sb.length(), hex, 0);
        return hex;
    };

    public CramMd5HexNegotiator(final PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider,
                                final String localFQDN,
                                final PasswordSource passwordSource)
    {
        super(authenticationProvider, localFQDN, passwordSource, HEX_PASSWORD_TRANSFORMER);
    }
}
