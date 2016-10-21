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

import java.security.AccessController;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.security.access.Operation;

public class CompoundAccessControl implements AccessControl<CompoundSecurityToken>
{
    private final AtomicReference<List<AccessControl<?>>> _underlyingControls = new AtomicReference<>();
    private final Result _defaultResult;

    public CompoundAccessControl(List<AccessControl<?>> underlying, Result defaultResult)
    {
        setAccessControls(underlying);
        _defaultResult = defaultResult;
    }

    public void setAccessControls(final List<AccessControl<?>> underlying)
    {
        _underlyingControls.set(new CopyOnWriteArrayList<>(underlying));
    }

    @Override
    public Result getDefault()
    {
        List<AccessControl<?>> underlying = _underlyingControls.get();
        for(AccessControl<?> control : underlying)
        {
            final Result result = control.getDefault();
            if(result.isFinal())
            {
                return result;
            }
        }

        return _defaultResult;
    }

    @Override
    public CompoundSecurityToken newToken()
    {
        return new CompoundSecurityToken(_underlyingControls.get(), Subject.getSubject(AccessController.getContext()));
    }

    @Override
    public CompoundSecurityToken newToken(final Subject subject)
    {
        return new CompoundSecurityToken(_underlyingControls.get(), subject);
    }

    @Override
    public Result authorise(final CompoundSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject)
    {
        return authorise(token, operation, configuredObject, Collections.<String,Object>emptyMap());
    }

    @Override
    public Result authorise(final CompoundSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject,
                            final Map<String, Object> arguments)
    {
        List<AccessControl<?>> underlying = _underlyingControls.get();
        Map<AccessControl<?>, SecurityToken> compoundToken = token == null ? null : token.getCompoundToken(underlying);
        for(AccessControl control : underlying)
        {
            SecurityToken underlyingToken = compoundToken == null ? null : compoundToken.get(control);
            final Result result = control.authorise(underlyingToken, operation, configuredObject, arguments);
            if(result.isFinal())
            {
                return result;
            }
        }

        return Result.DEFER;
    }

}
