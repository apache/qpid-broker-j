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

package org.apache.qpid.server.security.auth.sasl.plain;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;

public class PlainNegotiator implements SaslNegotiator
{
    public static final String MECHANISM = "PLAIN";
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    private UsernamePasswordAuthenticationProvider _usernamePasswordAuthenticationProvider;
    private volatile boolean _isComplete;
    private volatile String _username;

    public PlainNegotiator(final UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider)
    {
        _usernamePasswordAuthenticationProvider = usernamePasswordAuthenticationProvider;
    }

    @Override
    public AuthenticationResult handleResponse(final byte[] response)
    {
        if (_isComplete)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                            new IllegalStateException(
                                                    "Multiple Authentications not permitted."));
        }
        else
        {
            _isComplete = true;
        }
        int authzidNullPosition = findNullPosition(response, 0);
        if (authzidNullPosition < 0)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                            new IllegalArgumentException(
                                                    "Invalid PLAIN encoding, authzid null terminator not found"));
        }
        int authcidNullPosition = findNullPosition(response, authzidNullPosition + 1);
        if (authcidNullPosition < 0)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                            new IllegalArgumentException(
                                                    "Invalid PLAIN encoding, authcid null terminator not found"));
        }

        String password;
        try
        {
            _username = new String(response, authzidNullPosition + 1, authcidNullPosition - authzidNullPosition - 1, UTF8);
            // TODO: should not get pwd as a String but as a char array...
            int passwordLen = response.length - authcidNullPosition - 1;
            password = new String(response, authcidNullPosition + 1, passwordLen, UTF8);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("JVM does not support UTF8", e);
        }
        return _usernamePasswordAuthenticationProvider.authenticate(_username, password);
    }

    @Override
    public void dispose()
    {

    }

    @Override
    public String getAttemptedAuthenticationId()
    {
        return _username;
    }

    private int findNullPosition(byte[] response, int startPosition)
    {
        int position = startPosition;
        while (position < response.length)
        {
            if (response[position] == (byte) 0)
            {
                return position;
            }
            position++;
        }
        return -1;
    }
}
