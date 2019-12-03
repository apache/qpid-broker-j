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
package org.apache.qpid.server.security.group;

import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.model.AbstractCaseAwareGroupProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.Group;
import org.apache.qpid.server.model.GroupManagingGroupProvider;
import org.apache.qpid.server.model.GroupMember;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.security.CaseAwareGroupProvider;

@ManagedObject(category = false, type = GroupProviderImpl.CONFIG_TYPE)
public class GroupProviderImpl extends AbstractCaseAwareGroupProvider<GroupProviderImpl>
        implements CaseAwareGroupProvider<GroupProviderImpl>, GroupManagingGroupProvider
{

    public static final String CONFIG_TYPE = "ManagedGroupProvider";

    @ManagedObjectFactoryConstructor
    public GroupProviderImpl(Map<String, Object> attributes, Container<?> container)
    {
        super(container, attributes);
    }


    @Override
    public Set<Principal> getGroupPrincipalsForUser(final Principal userPrincipal)
    {
        Set<Principal> principals = new HashSet<>();

        final Collection<Group> groups = getChildren(Group.class);
        for (Group<?> group : groups)
        {
            for (GroupMember<?> member : group.getChildren(GroupMember.class))
            {
                if (member.getName().equals(userPrincipal.getName()))
                {
                    principals.add(new GroupPrincipal(group.getName(), this));
                }
            }
        }
        return principals;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                             final Map<String, Object> attributes)
    {
        if (childClass == Group.class)
        {
            return getObjectFactory().createAsync(childClass, attributes, this);
        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }
}
