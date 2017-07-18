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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.AbstractSaslServerNegotiator;

public class AbstractCramMd5Negotiator extends AbstractSaslServerNegotiator implements SaslNegotiator
{
    protected static final PasswordTransformer PLAIN_PASSWORD_TRANSFORMER =
            new PasswordTransformer()
            {
                @Override
                public char[] transform(final char[] passwordData)
                {
                    return passwordData;
                }
            };

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCramMd5Negotiator.class);

    private final SaslServer _saslServer;
    private final SaslException _exception;
    private final PasswordCredentialManagingAuthenticationProvider<?> _authenticationProvider;
    private volatile String _username;

    AbstractCramMd5Negotiator(final PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider,
                              String localFQDN,
                              final PasswordSource passwordSource,
                              final PasswordTransformer passwordTransformer)
    {
        _authenticationProvider = authenticationProvider;
        SaslServer saslServer = null;
        SaslException exception = null;
        try
        {
            saslServer = Sasl.createSaslServer("CRAM-MD5",
                                               "AMQP",
                                               localFQDN,
                                               null,
                                               new ServerCallbackHandler(passwordSource, passwordTransformer));
        }
        catch (SaslException e)
        {
            exception = e;
            LOGGER.warn("Creation of SASL server for mechanism '{}' failed.", "CRAM-MD5", e);
        }
        _saslServer = saslServer;
        _exception = exception;
    }

    @Override
    protected Exception getSaslServerCreationException()
    {
        return _exception;
    }

    @Override
    protected SaslServer getSaslServer()
    {
        return _saslServer;
    }

    @Override
    protected AuthenticationProvider<?> getAuthenticationProvider()
    {
        return _authenticationProvider;
    }

    @Override
    public String getAttemptedAuthenticationId()
    {
        return _username;
    }

    private class ServerCallbackHandler implements CallbackHandler
    {
        private final PasswordSource _passwordSource;
        private final PasswordTransformer _passwordTransformer;

        private ServerCallbackHandler(PasswordSource passwordSource, PasswordTransformer passwordTransformer)
        {
            _passwordTransformer = passwordTransformer;
            _passwordSource = passwordSource;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
            List<Callback> callbackList = new ArrayList<>(Arrays.asList(callbacks));
            Iterator<Callback> iter = callbackList.iterator();
            while (iter.hasNext())
            {
                Callback callback = iter.next();
                if (callback instanceof NameCallback)
                {
                    _username = ((NameCallback) callback).getDefaultName();
                    iter.remove();
                    break;
                }
            }

            if (_username != null)
            {
                iter = callbackList.iterator();
                while (iter.hasNext())
                {
                    Callback callback = iter.next();
                    if (callback instanceof PasswordCallback)
                    {
                        iter.remove();
                        char[] passwordData = _passwordSource.getPassword(_username);
                        if (passwordData != null)
                        {
                            ((PasswordCallback) callback).setPassword(_passwordTransformer.transform(passwordData));
                        }
                        else
                        {
                            ((PasswordCallback) callback).setPassword(null);
                        }
                        break;
                    }
                }
            }

            for (Callback callback : callbackList)
            {

                if (callback instanceof AuthorizeCallback)
                {
                    ((AuthorizeCallback) callback).setAuthorized(true);
                }
                else
                {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

    interface PasswordTransformer
    {
        char[] transform(char[] passwordData);
    }
}
