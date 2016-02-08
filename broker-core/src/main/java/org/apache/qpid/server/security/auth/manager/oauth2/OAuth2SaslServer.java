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

package org.apache.qpid.server.security.auth.manager.oauth2;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * https://developers.google.com/gmail/xoauth2_protocol
 */
class OAuth2SaslServer implements SaslServer
{
    public static final String MECHANISM = "XOAUTH2";
    public static final String ACCESS_TOKEN_PROPERTY = "accessToken";

    private static final String BEARER_PREFIX = "Bearer ";

    private String _accessToken;
    private boolean _isComplete;

    @Override
    public String getMechanismName()
    {
        return MECHANISM;
    }

    @Override
    public byte[] evaluateResponse(final byte[] response) throws SaslException
    {
        Map<String, String> responsePairs = splitResponse(response);

        String auth = responsePairs.get("auth");
        if (auth != null)
        {
            if (auth.startsWith(BEARER_PREFIX))
            {
                _accessToken = auth.substring(BEARER_PREFIX.length());
                _isComplete = true;
            }
            else
            {
                throw new SaslException("The 'auth' part of response does not not begin with the expected prefix");
            }
        }
        else
        {
            throw new SaslException("The mandatory 'auth' part of the response was absent.");
        }

        return new byte[0];
    }

    @Override
    public boolean isComplete()
    {
        return _isComplete;
    }

    @Override
    public String getAuthorizationID()
    {
        return null;
    }

    @Override
    public Object getNegotiatedProperty(final String propName)
    {
        if (!_isComplete)
        {
            throw new IllegalStateException("authentication exchange has not completed");
        }
        if (ACCESS_TOKEN_PROPERTY.equals(propName))
        {
            return _accessToken;
        }
        else
        {
            return null;
        }
    }

    @Override
    public byte[] unwrap(final byte[] incoming, final int offset, final int len) throws SaslException
    {
        throw new SaslException("");
    }

    @Override
    public byte[] wrap(final byte[] outgoing, final int offset, final int len) throws SaslException
    {
        throw new SaslException("");
    }

    @Override
    public void dispose() throws SaslException
    {
        _accessToken = null;
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
