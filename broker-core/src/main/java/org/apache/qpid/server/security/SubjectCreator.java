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
package org.apache.qpid.server.security;

import static org.apache.qpid.server.logging.messages.AuthenticationProviderMessages.AUTHENTICATION_FAILED;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;

/**
 * Creates a {@link Subject} formed by the {@link Principal}'s returned from:
 * <ol>
 * <li>Authenticating using an {@link AuthenticationProvider}</li>
 * </ol>
 *
 * <p>
 * SubjectCreator is a facade to the {@link AuthenticationProvider}, and is intended to be
 * the single place that {@link Subject}'s are created in the broker.
 * </p>
 */
public class SubjectCreator
{
    private final NamedAddressSpace _addressSpace;
    private AuthenticationProvider<?> _authenticationProvider;
    private Collection<GroupProvider<?>> _groupProviders;

    public SubjectCreator(AuthenticationProvider<?> authenticationProvider,
                          Collection<GroupProvider<?>> groupProviders,
                          NamedAddressSpace addressSpace)
    {
        _authenticationProvider = authenticationProvider;
        _groupProviders = groupProviders;
        _addressSpace = addressSpace;
    }

    public AuthenticationProvider<?> getAuthenticationProvider()
    {
        return _authenticationProvider;
    }

    public SaslNegotiator createSaslNegotiator(String mechanism, final SaslSettings saslSettings)
    {
        return _authenticationProvider.createSaslNegotiator(mechanism, saslSettings, _addressSpace);
    }

    public SubjectAuthenticationResult authenticate(SaslNegotiator saslNegotiator, byte[] response)
    {
        AuthenticationResult authenticationResult = saslNegotiator.handleResponse(response);
        if(authenticationResult.getStatus() == AuthenticationStatus.SUCCESS)
        {
            return createResultWithGroups(authenticationResult);
        }
        else
        {
            if (authenticationResult.getStatus() == AuthenticationStatus.ERROR)
            {
                String authenticationId = saslNegotiator.getAttemptedAuthenticationId();
                _authenticationProvider.getEventLogger().message(AUTHENTICATION_FAILED(authenticationId, authenticationId != null));
            }
            return new SubjectAuthenticationResult(authenticationResult);
        }
    }

    public SubjectAuthenticationResult createResultWithGroups(final AuthenticationResult authenticationResult)
    {
        if(authenticationResult.getStatus() == AuthenticationStatus.SUCCESS)
        {
            final Subject authenticationSubject = new Subject();

            authenticationSubject.getPrincipals().addAll(authenticationResult.getPrincipals());
            final Set<Principal> groupPrincipals = getGroupPrincipals(authenticationResult.getMainPrincipal());
            authenticationSubject.getPrincipals().addAll(groupPrincipals);

            authenticationSubject.setReadOnly();

            return new SubjectAuthenticationResult(authenticationResult, authenticationSubject);
        }
        else
        {
            return new SubjectAuthenticationResult(authenticationResult);
        }
    }



    public Subject createSubjectWithGroups(Principal userPrincipal)
    {
        Subject authenticationSubject = new Subject();

        authenticationSubject.getPrincipals().add(userPrincipal);
        authenticationSubject.getPrincipals().addAll(getGroupPrincipals(userPrincipal));
        authenticationSubject.setReadOnly();

        return authenticationSubject;
    }

    Set<Principal> getGroupPrincipals(Principal userPrincipal)
    {
        Set<Principal> principals = new HashSet<Principal>();
        for (GroupProvider groupProvider : _groupProviders)
        {
            Set<Principal> groups = groupProvider.getGroupPrincipalsForUser(userPrincipal);
            if (groups != null)
            {
                principals.addAll(groups);
            }
        }

        return Collections.unmodifiableSet(principals);
    }

}
