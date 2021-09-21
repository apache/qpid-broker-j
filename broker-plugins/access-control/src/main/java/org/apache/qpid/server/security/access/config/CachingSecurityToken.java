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
package org.apache.qpid.server.security.access.config;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;

final class CachingSecurityToken implements SecurityToken
{
    private static final AtomicReferenceFieldUpdater<CachingSecurityToken, AccessControlCache> CACHE_UPDATE =
            AtomicReferenceFieldUpdater.newUpdater(CachingSecurityToken.class, AccessControlCache.class, "_cache");

    private volatile AccessControlCache _cache;

    CachingSecurityToken(final RuleBasedAccessControl accessControl)
    {
        _cache = new AccessControlCache(accessControl);
    }

    Result authorise(final RuleBasedAccessControl ruleBasedAccessControl,
                     final Operation operation,
                     final PermissionedObject configuredObject,
                     final Map<String, Object> arguments)
    {
        return CACHE_UPDATE.updateAndGet(this,
                c -> c.isNotBasedOn(ruleBasedAccessControl) ? new AccessControlCache(ruleBasedAccessControl) : c)
                .authorise(operation, configuredObject, arguments);
    }

    private static final class CachedMethodAuthKey
    {
        private final PermissionedObject _configuredObject;
        private final Operation _operation;
        private final Map<String, Object> _arguments;

        private final int _hashCode;

        public CachedMethodAuthKey(final PermissionedObject configuredObject,
                                   final Operation operation,
                                   final Map<String, Object> arguments)
        {
            _configuredObject = configuredObject;
            _operation = operation;
            _arguments = arguments;
            _hashCode = Objects.hash(configuredObject, operation, arguments);
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

            final CachedMethodAuthKey that = (CachedMethodAuthKey) o;

            return Objects.equals(_configuredObject, that._configuredObject)
                    && Objects.equals(_operation, that._operation)
                    && Objects.equals(_arguments, that._arguments);
        }

        @Override
        public int hashCode()
        {
            return _hashCode;
        }
    }

    private static final class AccessControlCache
    {
        private final RuleBasedAccessControl _accessControl;
        private final ConcurrentMap<CachedMethodAuthKey, Result> _cache = new ConcurrentHashMap<>();

        AccessControlCache(final RuleBasedAccessControl accessControl)
        {
            _accessControl = accessControl;
        }

        boolean isNotBasedOn(RuleBasedAccessControl accessControl)
        {
            return _accessControl != accessControl;
        }

        Result authorise(final Operation operation,
                         final PermissionedObject configuredObject,
                         final Map<String, Object> arguments)
        {
            return _cache.computeIfAbsent(new CachedMethodAuthKey(configuredObject, operation, arguments),
                    key -> _accessControl.authorise(operation, configuredObject, arguments));
        }
    }
}
