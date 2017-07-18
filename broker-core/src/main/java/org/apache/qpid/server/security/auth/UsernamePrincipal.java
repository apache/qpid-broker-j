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
package org.apache.qpid.server.security.auth;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.QpidPrincipal;

/** A principal that is just a wrapper for a simple username. */
public class UsernamePrincipal implements QpidPrincipal
{
    private static final long serialVersionUID = 1L;

    private final String _name;
    private final AuthenticationProvider<?> _authenticationProvider;

    public UsernamePrincipal(String name, AuthenticationProvider<?> authenticationProvider)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name cannot be null");
        }
        _name = name;
        _authenticationProvider = authenticationProvider;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public String toString()
    {
        return _name;
    }

    @Override
    public AuthenticationProvider<?> getOrigin()
    {
        return _authenticationProvider;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final UsernamePrincipal that = (UsernamePrincipal) o;

        if (!_name.equals(that._name))
        {
            return false;
        }
        if (_authenticationProvider == null || that._authenticationProvider == null)
        {
            return _authenticationProvider == null && that._authenticationProvider == null;
        }

        return (_authenticationProvider.getType().equals(that._authenticationProvider.getType())
                && _authenticationProvider.getName().equals(that._authenticationProvider.getName()));
    }

    @Override
    public int hashCode()
    {
        int result = _name.hashCode();
        result = 31 * result + (_authenticationProvider != null ? _authenticationProvider.hashCode() : 0);
        return result;
    }
}
