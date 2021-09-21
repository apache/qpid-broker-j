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

import java.security.AccessController;
import java.util.Collections;
import java.util.Map;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.PermissionedObject;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;

public class RuleBasedAccessControl implements AccessControl<CachingSecurityToken>, LegacyAccessControl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleBasedAccessControl.class);

    private final LegacyAccessControlAdapter _adapter;

    private final RuleSet _ruleSet;

    public RuleBasedAccessControl(RuleSet rs, final Model model)
    {
        _ruleSet = rs;
        _adapter = new LegacyAccessControlAdapter(this, model);
    }

    @Override
    public Result getDefault()
    {
        return _ruleSet.getDefault();
    }

    @Override
    public final CachingSecurityToken newToken()
    {
        return new CachingSecurityToken(this);
    }

    @Override
    public CachingSecurityToken newToken(final Subject subject)
    {
        return newToken();
    }

    /**
     * Check if an operation is authorised by asking the  configuration object about the access
     * control rules granted to the current thread's {@link Subject}. If there is no current
     * user the plugin will abstain.
     */
    @Override
    public final Result authorise(LegacyOperation operation, ObjectType objectType, ObjectProperties properties)
    {
        final Subject subject = Subject.getSubject(AccessController.getContext());

        // Abstain if there is no subject/principal associated with this thread
        if (subject == null || subject.getPrincipals().isEmpty())
        {
            return Result.DEFER;
        }

        LOGGER.debug("Checking {} {}", operation, objectType);

        try
        {
            return _ruleSet.check(subject, operation, objectType, properties);
        }
        catch (RuntimeException e)
        {
            LOGGER.error(String.format("Unable to check %s %s", operation, objectType), e);
            return Result.DENIED;
        }
    }

    @Override
    public Result authorise(final CachingSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject)
    {
        return authorise(token, operation, configuredObject, Collections.emptyMap());
    }

    @Override
    public Result authorise(final CachingSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject,
                            final Map<String, Object> arguments)
    {
        if (token != null)
        {
            return token.authorise(this, operation, configuredObject, arguments);
        }
        else
        {
            return authorise(operation, configuredObject, arguments);
        }
    }

    Result authorise(final Operation operation,
                     final PermissionedObject configuredObject,
                     final Map<String, Object> arguments)
    {
        return _adapter.authorise(operation, configuredObject, arguments);
    }
}
