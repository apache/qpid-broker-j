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

package org.apache.qpid.server.security.auth.manager;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.plain.PlainPasswordCallback;
import org.apache.qpid.server.security.auth.sasl.plain.PlainSaslServer;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServer;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSourceAdapter;

@ManagedObject( category = false, type = "Simple", register = false )
public class SimpleAuthenticationManager extends AbstractAuthenticationManager<SimpleAuthenticationManager>
        implements UsernamePasswordAuthenticationProvider<SimpleAuthenticationManager>
{
    private static final Logger _logger = LoggerFactory.getLogger(SimpleAuthenticationManager.class);

    private static final String PLAIN_MECHANISM = "PLAIN";
    private static final String CRAM_MD5_MECHANISM = "CRAM-MD5";
    private static final String SCRAM_SHA1_MECHANISM = ScramSHA1AuthenticationManager.MECHANISM;
    private static final String SCRAM_SHA256_MECHANISM = ScramSHA256AuthenticationManager.MECHANISM;

    private final Map<String, String> _users = Collections.synchronizedMap(new HashMap<String, String>());
    private final ScramSaslServerSourceAdapter _scramSha1Adapter;
    private final ScramSaslServerSourceAdapter _scramSha256Adapter;

    public SimpleAuthenticationManager(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
        ScramSaslServerSourceAdapter.PasswordSource passwordSource =
                new ScramSaslServerSourceAdapter.PasswordSource()
                {
                    @Override
                    public char[] getPassword(final String username)
                    {
                        String password = _users.get(username);
                        return password == null ? null : password.toCharArray();
                    }
                };

        final int scramIterationCount = getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT);
        _scramSha1Adapter = new ScramSaslServerSourceAdapter(scramIterationCount, "HmacSHA1", "SHA-1", passwordSource);
        _scramSha256Adapter = new ScramSaslServerSourceAdapter(scramIterationCount, "HmacSHA256", "SHA-256", passwordSource);

    }


    public void addUser(String username, String password)
    {
        _users.put(username, password);
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.unmodifiableList(Arrays.asList(PLAIN_MECHANISM, CRAM_MD5_MECHANISM, SCRAM_SHA1_MECHANISM, SCRAM_SHA256_MECHANISM));
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String localFQDN, Principal externalPrincipal) throws SaslException
    {
        if (PLAIN_MECHANISM.equals(mechanism))
        {
            return new PlainSaslServer(new SimplePlainCallbackHandler());
        }
        else if (CRAM_MD5_MECHANISM.equals(mechanism))
        {
            return Sasl.createSaslServer(mechanism, "AMQP", localFQDN, null, new SimpleCramMd5CallbackHandler());
        }
        else if (SCRAM_SHA1_MECHANISM.equals(mechanism))
        {
            return new ScramSaslServer(_scramSha1Adapter, mechanism, "HmacSHA1", "SHA-1");
        }
        else if(ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramSaslServer(_scramSha256Adapter, mechanism, "HmacSHA256", "SHA-256");
        }
        else
        {
            throw new SaslException("Unknown mechanism: " + mechanism);
        }
    }

    @Override
    public AuthenticationResult authenticate(SaslServer server, byte[] response)
    {
        try
        {
            // Process response from the client
            byte[] challenge = server.evaluateResponse(response != null ? response : new byte[0]);

            if (server.isComplete())
            {
                String authorizationID = server.getAuthorizationID();
                _logger.debug("Authenticated as " + authorizationID);

                return new AuthenticationResult(new UsernamePrincipal(authorizationID, this), challenge);
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        if (_users.containsKey(username))
        {
            String userPassword = _users.get(username);
            if (userPassword.equals(password))
            {
                return new AuthenticationResult(new UsernamePrincipal(username, this));
            }
        }
        return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
    }

    private class SimpleCramMd5CallbackHandler implements CallbackHandler
    {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
            String username = null;
            for (Callback callback : callbacks)
            {
                if (callback instanceof NameCallback)
                {
                    username = ((NameCallback) callback).getDefaultName();
                }
                else if (callback instanceof PasswordCallback)
                {
                    if (_users.containsKey(username))
                    {
                        String password = _users.get(username);
                        ((PasswordCallback) callback).setPassword(password.toCharArray());
                    }
                    else
                    {
                        throw new SaslException("Authentication failed");
                    }
                }
                else if (callback instanceof AuthorizeCallback)
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

    private class SimplePlainCallbackHandler implements CallbackHandler
    {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
            String username = null;
             for (Callback callback : callbacks)
            {
                if (callback instanceof NameCallback)
                {
                    username = ((NameCallback) callback).getDefaultName();
                }
                else if (callback instanceof PlainPasswordCallback)
                {
                    if (_users.containsKey(username))
                    {
                        PlainPasswordCallback plainPasswordCallback = (PlainPasswordCallback) callback;
                        String password = plainPasswordCallback.getPlainPassword();
                        plainPasswordCallback.setAuthenticated(password.equals(_users.get(username)));
                    }
                }
                else if (callback instanceof AuthorizeCallback)
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
}
