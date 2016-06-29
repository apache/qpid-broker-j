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
 */
package org.apache.qpid.server.security;

import java.util.Map;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.access.Operation;

public interface AccessControl<T extends SecurityToken>
{
	Result getDefault();


    T newToken();

    T newToken(Subject subject);

    Result authorise(Operation operation, ConfiguredObject<?> configuredObject);
    Result authoriseMethod(ConfiguredObject<?> configuredObject, String methodName, Map<String,Object> arguments);
    Result authoriseMethod(T token, ConfiguredObject<?> configuredObject, String methodName, Map<String,Object> arguments);



    AccessControl ALWAYS_ALLOWED = new AccessControl<SecurityToken>()
    {
        @Override
        public Result getDefault()
        {
            return Result.ALLOWED;
        }

        @Override
        public SecurityToken newToken()
        {
            return null;
        }

        @Override
        public SecurityToken newToken(final Subject subject)
        {
            return null;
        }

        @Override
        public Result authorise(final Operation operation, final ConfiguredObject<?> configuredObject)
        {
            return Result.ALLOWED;
        }

        @Override
        public Result authoriseMethod(final ConfiguredObject<?> configuredObject,
                                      final String methodName,
                                      final Map<String, Object> arguments)
        {
            return Result.ALLOWED;
        }

        @Override
        public Result authoriseMethod(final SecurityToken token,
                                      final ConfiguredObject configuredObject,
                                      final String methodName,
                                      final Map arguments)
        {
            return Result.ALLOWED;
        }
    };

    AccessControl ALWAYS_DENIED = new AccessControl<SecurityToken>()
    {
        @Override
        public Result getDefault()
        {
            return Result.DENIED;
        }

        @Override
        public SecurityToken newToken()
        {
            return null;
        }

        @Override
        public SecurityToken newToken(final Subject subject)
        {
            return null;
        }

        @Override
        public Result authorise(final Operation operation, final ConfiguredObject<?> configuredObject)
        {
            return Result.DENIED;
        }

        @Override
        public Result authoriseMethod(final ConfiguredObject<?> configuredObject,
                                      final String methodName,
                                      final Map<String, Object> arguments)
        {
            return Result.DENIED;
        }

        @Override
        public Result authoriseMethod(final SecurityToken token,
                                      final ConfiguredObject<?> configuredObject,
                                      final String methodName,
                                      final Map<String, Object> arguments)
        {
            return Result.DENIED;
        }
    };



}
