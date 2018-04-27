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
package org.apache.qpid.server.security.auth;

import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.QpidPrincipal;

/**
 * A simple Principal wrapper. Exists to allow us to identify the "primary" principal
 * by calling {@link Subject#getPrincipals(Class)}, passing in {@link AuthenticatedPrincipal}.class,
 * e.g. when logging.
 */
public final class AuthenticatedPrincipal implements QpidPrincipal
{
    private static final long serialVersionUID = 1L;

    private final Principal _wrappedPrincipal;

    public AuthenticatedPrincipal(Principal wrappedPrincipal)
    {
        if(wrappedPrincipal == null)
        {
            throw new IllegalArgumentException("Wrapped principal is null");
        }

        _wrappedPrincipal = wrappedPrincipal;
    }

    public static AuthenticatedPrincipal getCurrentUser()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        final AuthenticatedPrincipal user;
        if(subject != null)
        {
            Set<AuthenticatedPrincipal> principals = subject.getPrincipals(AuthenticatedPrincipal.class);
            if(!principals.isEmpty())
            {
                user = principals.iterator().next();
            }
            else
            {
                user = null;
            }
        }
        else
        {
            user = null;
        }
        return user;
    }

    @Override
    public ConfiguredObject<?> getOrigin()
    {
        if (_wrappedPrincipal instanceof QpidPrincipal)
        {
            return ((QpidPrincipal) _wrappedPrincipal).getOrigin();
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getName()
    {
        return _wrappedPrincipal.getName();
    }

    @Override
    public int hashCode()
    {
        return _wrappedPrincipal.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof AuthenticatedPrincipal))
        {
            return false;
        }

        AuthenticatedPrincipal other = (AuthenticatedPrincipal) obj;

        return _wrappedPrincipal.equals(other._wrappedPrincipal);
    }

    public static AuthenticatedPrincipal getOptionalAuthenticatedPrincipalFromSubject(final Subject authSubject)
    {
        return getAuthenticatedPrincipalFromSubject(authSubject, true);
    }

    public static AuthenticatedPrincipal getAuthenticatedPrincipalFromSubject(final Subject authSubject)
    {
        return getAuthenticatedPrincipalFromSubject(authSubject, false);
    }

    private static AuthenticatedPrincipal getAuthenticatedPrincipalFromSubject(final Subject authSubject, boolean isPrincipalOptional)
    {
        return QpidPrincipal.getSingletonPrincipal(authSubject, isPrincipalOptional, AuthenticatedPrincipal.class);
    }

    @Override
    public String toString()
    {
        return getName();
    }

}
