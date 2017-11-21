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

package org.apache.qpid.server.security.auth.sasl.oauth2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;

public class OAuth2Negotiator implements SaslNegotiator
{
    enum State
    {
        INITIAL,
        CHALLENGE_SENT,
        COMPLETE
    }

    public static final String MECHANISM = "XOAUTH2";
    private static final String BEARER_PREFIX = "Bearer ";
    private final NamedAddressSpace _addressSpace;
    private OAuth2AuthenticationProvider<?> _authenticationProvider;
    private volatile State _state = State.INITIAL;

    public OAuth2Negotiator(OAuth2AuthenticationProvider<?> authenticationProvider,
                            final NamedAddressSpace addressSpace)
    {
        _authenticationProvider = authenticationProvider;
        _addressSpace = addressSpace;
    }

    @Override
    public AuthenticationResult handleResponse(final byte[] response)
    {
        if (_state == State.COMPLETE)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                            new IllegalStateException("Multiple Authentications not permitted."));
        }
        else if (_state == State.INITIAL && (response == null || response.length == 0))
        {
            _state = State.CHALLENGE_SENT;
            return new AuthenticationResult(new byte[0], AuthenticationResult.AuthenticationStatus.CONTINUE);
        }

        _state = State.COMPLETE;
        if (response == null || response.length == 0)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,
                                            new IllegalArgumentException("Invalid OAuth2 client response."));
        }

        Map<String, String> responsePairs = splitResponse(response);

        String auth = responsePairs.get("auth");
        if (auth != null)
        {
            if (auth.startsWith(BEARER_PREFIX))
            {
                return _authenticationProvider.authenticateViaAccessToken(auth.substring(BEARER_PREFIX.length()), _addressSpace);
            }
            else
            {
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalArgumentException("The 'auth' part of response does not not begin with the expected prefix"));
            }
        }
        else
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalArgumentException("The mandatory 'auth' part of the response was absent."));
        }
    }

    @Override
    public void dispose()
    {

    }

    @Override
    public String getAttemptedAuthenticationId()
    {
        return null;
    }

    private Map<String, String> splitResponse(final byte[] response)
    {
        String[] splitResponse = new String(response, StandardCharsets.US_ASCII).split("\1");
        Map<String, String> responseItems = new HashMap<>(splitResponse.length);
        for(String nameValue : splitResponse)
        {
            if (nameValue.length() > 0)
            {
                String[] nameValueSplit = nameValue.split("=", 2);
                if (nameValueSplit.length == 2)
                {
                    responseItems.put(nameValueSplit[0], nameValueSplit[1]);
                }
            }
        }
        return responseItems;
    }
}
