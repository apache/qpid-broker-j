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
package org.apache.qpid.server.security.auth.manager.oauth2;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.QpidPrincipal;


public class OAuth2UserPrincipal implements QpidPrincipal
{
    private static final long serialVersionUID = 1L;

    private final String _accessToken;
    private final String _name;
    private final AuthenticationProvider<?> _authenticationProvider;

    public OAuth2UserPrincipal(final String name, final String accessToken, final AuthenticationProvider<?> authenticationProvider)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name cannot be null");
        }
        if (accessToken == null)
        {
            throw new IllegalArgumentException("accessToken cannot be null");
        }
        _name = name;
        _accessToken = accessToken;
        _authenticationProvider = authenticationProvider;
    }

    public String getAccessToken()
    {
        return _accessToken;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public ConfiguredObject<?> getOrigin()
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

        final OAuth2UserPrincipal that = (OAuth2UserPrincipal) o;

        if (!_accessToken.equals(that._accessToken))
        {
            return false;
        }
        return _name.equals(that._name);

    }

    @Override
    public int hashCode()
    {
        int result = _accessToken.hashCode();
        result = 31 * result + _name.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
