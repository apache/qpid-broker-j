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

package org.apache.qpid.server.security.auth.sasl;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;

public abstract class AbstractSaslServerNegotiator implements SaslNegotiator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSaslServerNegotiator.class);

    @Override
    public void dispose()
    {
        SaslServer saslServer = getSaslServer();
        if (saslServer != null)
        {
            try
            {
                saslServer.dispose();
            }
            catch (SaslException e)
            {
                LOGGER.warn("Disposing of SaslServer failed", e);
            }
        }
    }

    @Override
    public AuthenticationResult handleResponse(final byte[] response)
    {
        SaslServer saslServer = getSaslServer();
        if (saslServer == null)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, getSaslServerCreationException());
        }
        try
        {

            byte[] challenge = saslServer.evaluateResponse(response != null ? response : new byte[0]);

            if (saslServer.isComplete())
            {
                final String userId = saslServer.getAuthorizationID();
                return new AuthenticationResult(new UsernamePrincipal(userId, getAuthenticationProvider()),
                                                challenge);
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException | IllegalStateException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    protected abstract Exception getSaslServerCreationException();

    protected abstract SaslServer getSaslServer();

    protected abstract AuthenticationProvider<?> getAuthenticationProvider();
}
