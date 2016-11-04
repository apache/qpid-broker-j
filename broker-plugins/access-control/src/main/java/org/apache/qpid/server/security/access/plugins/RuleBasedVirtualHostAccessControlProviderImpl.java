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
package org.apache.qpid.server.security.access.plugins;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostAccessControlProvider;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class RuleBasedVirtualHostAccessControlProviderImpl
        extends AbstractCommonRuleBasedAccessControlProvider<RuleBasedVirtualHostAccessControlProviderImpl, QueueManagingVirtualHost<?>, VirtualHostAccessControlProvider<?>>
        implements RuleBasedVirtualHostAccessControlProvider<RuleBasedVirtualHostAccessControlProviderImpl>
{
    private static final EnumSet<ObjectType> ALLOWED_OBJECT_TYPES = EnumSet.of(ObjectType.ALL,
                                                                               ObjectType.QUEUE,
                                                                               ObjectType.EXCHANGE,
                                                                               ObjectType.VIRTUALHOST,
                                                                               ObjectType.METHOD);

    static
    {
        Handler.register();
    }



    @ManagedObjectFactoryConstructor
    public RuleBasedVirtualHostAccessControlProviderImpl(Map<String, Object> attributes, QueueManagingVirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
    }


    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if(changedAttributes.contains(RULES))
        {
            for(AclRule rule : ((RuleBasedVirtualHostAccessControlProvider<?>)proxyForValidation).getRules())
            {
                if(!ALLOWED_OBJECT_TYPES.contains(rule.getObjectType()))
                {
                    throw new IllegalArgumentException("Cannot use the object type " + rule.getObjectType() + " only the following object types are allowed: " + ALLOWED_OBJECT_TYPES);
                }
            }
        }
    }


}
