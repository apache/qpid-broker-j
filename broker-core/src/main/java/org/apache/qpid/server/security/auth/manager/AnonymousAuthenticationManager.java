/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.security.auth.manager;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.anonymous.AnonymousNegotiator;

@ManagedObject( category = false, type= "Anonymous" )
public class AnonymousAuthenticationManager extends AbstractAuthenticationManager<AnonymousAuthenticationManager>
{
    public static final String PROVIDER_TYPE = "Anonymous";
    public static final String MECHANISM_NAME = "ANONYMOUS";

    public static final String ANONYMOUS_USERNAME = "ANONYMOUS";

    private final Principal _anonymousPrincipal;
    private final AuthenticationResult _anonymousAuthenticationResult;

    @ManagedObjectFactoryConstructor
    protected AnonymousAuthenticationManager(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
        _anonymousPrincipal = new UsernamePrincipal(ANONYMOUS_USERNAME, this);
        _anonymousAuthenticationResult = new AuthenticationResult(_anonymousPrincipal);
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.singletonList(MECHANISM_NAME);
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if(MECHANISM_NAME.equals(mechanism))
        {
            return new AnonymousNegotiator(_anonymousAuthenticationResult);
        }
        else
        {
            return null;
        }
    }

    public Principal getAnonymousPrincipal()
    {
        return _anonymousPrincipal;
    }

    public AuthenticationResult getAnonymousAuthenticationResult()
    {
        return _anonymousAuthenticationResult;
    }
}
