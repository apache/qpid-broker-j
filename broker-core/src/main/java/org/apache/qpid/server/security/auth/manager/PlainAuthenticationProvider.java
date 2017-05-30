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
package org.apache.qpid.server.security.auth.manager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSourceAdapter;

@ManagedObject(category = false, type = "Plain", validChildTypes = "org.apache.qpid.server.security.auth.manager.ConfigModelPasswordManagingAuthenticationProvider#getSupportedUserTypes()")
public class PlainAuthenticationProvider
        extends ConfigModelPasswordManagingAuthenticationProvider<PlainAuthenticationProvider>
{
    private final List<String> _mechanisms = Collections.unmodifiableList(Arrays.asList(PlainNegotiator.MECHANISM,
                                                                                        CramMd5Negotiator.MECHANISM,
                                                                                        ScramSHA1AuthenticationManager.MECHANISM,
                                                                                        ScramSHA256AuthenticationManager.MECHANISM));
    private volatile ScramSaslServerSourceAdapter _scramSha1Adapter;
    private volatile ScramSaslServerSourceAdapter _scramSha256Adapter;


    @ManagedObjectFactoryConstructor
    protected PlainAuthenticationProvider(final Map<String, Object> attributes, final Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        PasswordSource passwordSource = getPasswordSource();

        final int scramIterationCount = getContextValue(Integer.class,
                                                        AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT);
        _scramSha1Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                             ScramSHA1AuthenticationManager.HMAC_NAME,
                                                             ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                             passwordSource);
        _scramSha256Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                               ScramSHA256AuthenticationManager.HMAC_NAME,
                                                               ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                               passwordSource);
    }

    @Override
    protected String createStoredPassword(final String password)
    {
        return password;
    }

    @Override
    void validateUser(final ManagedUser managedUser)
    {
        // NOOP
    }

    @Override
    public List<String> getMechanisms()
    {
        return _mechanisms;
    }

    @Override
    public AuthenticationResult authenticate(final String username, final String password)
    {
        ManagedUser user = getUser(username);
        AuthenticationResult result;
        if (user != null && user.getPassword().equals(password))
        {
            result = new AuthenticationResult(new UsernamePrincipal(username, this));
        }
        else
        {
            result = new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
        }
        return result;
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
}
