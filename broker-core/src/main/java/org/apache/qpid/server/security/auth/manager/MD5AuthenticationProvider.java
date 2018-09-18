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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

@ManagedObject(category = false, type = "MD5", validChildTypes = "org.apache.qpid.server.security.auth.manager.ConfigModelPasswordManagingAuthenticationProvider#getSupportedUserTypes()")
public class MD5AuthenticationProvider
        extends ConfigModelPasswordManagingAuthenticationProvider<MD5AuthenticationProvider>
{
    private final List<String> _mechanisms = Collections.unmodifiableList(Arrays.asList(PlainNegotiator.MECHANISM,
                                                                                        CramMd5HashedNegotiator.MECHANISM,
                                                                                        CramMd5HexNegotiator.MECHANISM));


    @ManagedObjectFactoryConstructor
    protected MD5AuthenticationProvider(final Map<String, Object> attributes, final Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    protected String createStoredPassword(final String password)
    {
        byte[] data = password.getBytes(StandardCharsets.UTF_8);
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ServerScopedRuntimeException("MD5 not supported although Java compliance requires it");
        }

        md.update(data);
        return Base64.getEncoder().encodeToString(md.digest());
    }

    @Override
    void validateUser(final ManagedUser managedUser)
    {
    }

    @Override
    public List<String> getMechanisms()
    {
        return _mechanisms;
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
        else if (CramMd5Base64HashedNegotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5Base64HashedNegotiator(this,
                                                     saslSettings.getLocalFQDN(),
                                                     getPasswordSource());
        }
        else if (CramMd5Base64HexNegotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5Base64HexNegotiator(this,
                                                  saslSettings.getLocalFQDN(),
                                                  getPasswordSource());
        }
        else
        {
            return null;
        }
    }

    @Override
    public AuthenticationResult authenticate(final String username, final String password)
    {
        ManagedUser user = getUser(username);
        AuthenticationResult result;
        if (user != null && user.getPassword().equals(createStoredPassword(password)))
        {
            result = new AuthenticationResult(new UsernamePrincipal(username, this));
        }
        else
        {
            result = new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
        }
        return result;
    }
}
