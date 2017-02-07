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

package org.apache.qpid.server.security.auth.sasl.external;

import java.security.Principal;

import javax.security.auth.x500.X500Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

public class ExternalNegotiator implements SaslNegotiator
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ExternalNegotiator.class);
    private final AuthenticationResult _result;
    private final Principal _principal;
    private volatile boolean _isComplete;

    public ExternalNegotiator(final ExternalAuthenticationManager externalAuthenticationManager,
                              final Principal externalPrincipal)
    {
        boolean useFullDN = externalAuthenticationManager.getUseFullDN();
        if (externalPrincipal instanceof X500Principal && !useFullDN)
        {
            // Construct username as <CN>@<DC1>.<DC2>.<DC3>....<DCN>
            String username;
            String dn = ((X500Principal) externalPrincipal).getName(X500Principal.RFC2253);

            LOGGER.debug("Parsing username from Principal DN: {}", dn);

            username = SSLUtil.getIdFromSubjectDN(dn);
            if (username.isEmpty())
            {
                // CN is empty => Cannot construct username => Authentication failed => return null
                LOGGER.debug("CN value was empty in Principal name, unable to construct username");

                _principal =  null;
            }
            else
            {
                LOGGER.debug("Constructing Principal with username: {}", username);

                _principal = new UsernamePrincipal(username, externalAuthenticationManager);
            }
        }
        else
        {
            LOGGER.debug("Using external Principal: {}", externalPrincipal);

            _principal = externalPrincipal;
        }

        if (_principal == null)
        {
            _result = new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalArgumentException("CN value was empty in Principal name, unable to construct username"));
        }
        else
        {
            _result = new AuthenticationResult(_principal);
        }
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
        return _result;
    }

    @Override
    public void dispose()
    {

    }

    @Override
    public String getAttemptedAuthenticationId()
    {
        return (_principal == null ? null : _principal.getName());
    }
}
