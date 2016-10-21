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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.AccessController;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;

public class RuleBasedAccessControl implements AccessControl<CachingSecurityToken>, LegacyAccessControl
{
    private static final Logger _logger = LoggerFactory.getLogger(RuleBasedAccessControl.class);
    private final LegacyAccessControlAdapter _adapter;

    private RuleSet _ruleSet;

    public RuleBasedAccessControl(RuleSet rs, final Model model)
    {
        _ruleSet = rs;
        _adapter = new LegacyAccessControlAdapter(this, model);
    }

    public Result getDefault()
    {
        return _ruleSet.getDefault();
    }

    @Override
    public CachingSecurityToken newToken()
    {
        return newToken(Subject.getSubject(AccessController.getContext()));
    }

    @Override
    public CachingSecurityToken newToken(final Subject subject)
    {
        return new CachingSecurityToken(subject, this);
    }

    /**
     * Check if an operation is authorised by asking the  configuration object about the access
     * control rules granted to the current thread's {@link Subject}. If there is no current
     * user the plugin will abstain.
     */
    public Result authorise(LegacyOperation operation, ObjectType objectType, ObjectProperties properties)
    {
        InetAddress addressOfClient = null;
        final Subject subject = Subject.getSubject(AccessController.getContext());

        // Abstain if there is no subject/principal associated with this thread
        if (subject == null  || subject.getPrincipals().size() == 0)
        {
            return Result.DEFER;
        }

        Set<ConnectionPrincipal> principals = subject.getPrincipals(ConnectionPrincipal.class);
        if(!principals.isEmpty())
        {
            SocketAddress address = principals.iterator().next().getConnection().getRemoteSocketAddress();
            if(address instanceof InetSocketAddress)
            {
                addressOfClient = ((InetSocketAddress) address).getAddress();
            }
        }

        if(_logger.isDebugEnabled())
        {
            _logger.debug("Checking " + operation + " " + objectType + " " +
                          (addressOfClient == null ? "" : addressOfClient));
        }

        try
        {
            return  _ruleSet.check(subject, operation, objectType, properties, addressOfClient);
        }
        catch(Exception e)
        {
            _logger.error("Unable to check " + operation + " " + objectType + " "
                          + (addressOfClient == null ? "" : addressOfClient), e);
            return Result.DENIED;
        }
    }

    @Override
    public Result authorise(final CachingSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject)
    {
        return authorise(token, operation, configuredObject, Collections.<String,Object>emptyMap());
    }

    @Override
    public Result authorise(final CachingSecurityToken token,
                            final Operation operation,
                            final PermissionedObject configuredObject,
                            final Map<String, Object> arguments)
    {
        if(token != null)
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
