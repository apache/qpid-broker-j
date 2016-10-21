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
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.security.access.Operation;

public final class SubjectFixedResultAccessControl implements AccessControl<SubjectFixedResultAccessControl.FixedResultSecurityToken>
{
    private final Result _default;
    private final ResultCalculator _calculator;

    public SubjectFixedResultAccessControl(final ResultCalculator calculator,
                                           final Result defaultResult)
    {
        _default = defaultResult;
        _calculator = calculator;
    }

    @Override
    public Result getDefault()
    {
        return _default;
    }

    @Override
    public FixedResultSecurityToken newToken()
    {
        return newToken(Subject.getSubject(AccessController.getContext()));
    }

    @Override
    public FixedResultSecurityToken newToken(final Subject subject)
    {
        return new FixedResultSecurityToken(_calculator.getResult(subject));
    }

    @Override
    public Result authorise(final FixedResultSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject)
    {
        return token == null
                ? _calculator.getResult(Subject.getSubject(AccessController.getContext()))
                : token.getResult();
    }

    @Override
    public Result authorise(final FixedResultSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject,
                            final Map<String, Object> arguments)
    {
        return token == null
                ? _calculator.getResult(Subject.getSubject(AccessController.getContext()))
                : token.getResult();
    }

    public interface ResultCalculator
    {
        Result getResult(Subject subject);
    }

    static final class FixedResultSecurityToken implements SecurityToken
    {
        private final Result _result;

        private FixedResultSecurityToken(final Result result)
        {
            _result = result;
        }

        private Result getResult()
        {
            return _result;
        }
    }
}
