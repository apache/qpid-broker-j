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


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AccountNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSourceAdapter;

@ManagedObject( category = false, type = "Simple", register = false )
public class SimpleAuthenticationManager extends AbstractAuthenticationManager<SimpleAuthenticationManager>
        implements PasswordCredentialManagingAuthenticationProvider<SimpleAuthenticationManager>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAuthenticationManager.class);

    private static final String PLAIN_MECHANISM = "PLAIN";
    private static final String CRAM_MD5_MECHANISM = "CRAM-MD5";
    private static final String SCRAM_SHA1_MECHANISM = ScramSHA1AuthenticationManager.MECHANISM;
    private static final String SCRAM_SHA256_MECHANISM = ScramSHA256AuthenticationManager.MECHANISM;

    private final Map<String, String> _users = Collections.synchronizedMap(new HashMap<String, String>());
    private volatile ScramSaslServerSourceAdapter _scramSha1Adapter;
    private volatile ScramSaslServerSourceAdapter _scramSha256Adapter;

    public SimpleAuthenticationManager(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();
        PasswordSource passwordSource = getPasswordSource();

        final int scramIterationCount = getContextValue(Integer.class, AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT);
        _scramSha1Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                             ScramSHA1AuthenticationManager.HMAC_NAME,
                                                             ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                             passwordSource);
        _scramSha256Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                               ScramSHA256AuthenticationManager.HMAC_NAME,
                                                               ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                               passwordSource);
    }

    public void addUser(String username, String password)
    {
        createUser(username, password, Collections.EMPTY_MAP);
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.unmodifiableList(Arrays.asList(PLAIN_MECHANISM, CRAM_MD5_MECHANISM, SCRAM_SHA1_MECHANISM, SCRAM_SHA256_MECHANISM));
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if (PlainNegotiator.MECHANISM.equals(mechanism))
        {
            return new PlainNegotiator(this);
        }
        else if (CramMd5Negotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5Negotiator(this,
                                         saslSettings.getLocalFQDN(),
                                         getPasswordSource());
        }
        else if (ScramSHA1AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramNegotiator(this, _scramSha1Adapter, ScramSHA1AuthenticationManager.MECHANISM);
        }
        else if (ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramNegotiator(this, _scramSha256Adapter, ScramSHA256AuthenticationManager.MECHANISM);
        }
        else
        {
            return null;
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

    @Override
    public boolean createUser(final String username, final String password, final Map<String, String> attributes)
    {
        _users.put(username, password);
        return true;
    }

    @Override
    public void deleteUser(final String username) throws AccountNotFoundException
    {
        if (_users.remove(username) == null)
        {
            throw new AccountNotFoundException("No such user: '" + username + "'");
        }
    }

    @Override
    public void setPassword(final String username, final String password) throws AccountNotFoundException
    {
        if (_users.containsKey(username))
        {
            _users.put(username, password);
        }
        else
        {
            throw new AccountNotFoundException("No such user: '" + username + "'");
        }
    }

    @Override
    public Map<String, Map<String, String>> getUsers()
    {
        final HashMap<String, Map<String, String>> users = new HashMap<>();
        for (String username : _users.keySet())
        {
            users.put(username, Collections.EMPTY_MAP);
        }
        return users;
    }

    @Override
    public void reload()
    {
    }

    private PasswordSource getPasswordSource()
    {
        return new PasswordSource()
        {
            @Override
            public char[] getPassword(final String username)
            {
                String password = _users.get(username);
                return password == null ? null : password.toCharArray();
            }
        };
    }
}
