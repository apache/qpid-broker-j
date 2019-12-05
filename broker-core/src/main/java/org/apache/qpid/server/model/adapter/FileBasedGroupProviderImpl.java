/*
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
package org.apache.qpid.server.model.adapter;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractCaseAwareGroupProvider;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.Group;
import org.apache.qpid.server.model.GroupMember;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.group.FileGroupDatabase;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.util.FileHelper;

public class FileBasedGroupProviderImpl
        extends AbstractCaseAwareGroupProvider<FileBasedGroupProviderImpl> implements FileBasedGroupProvider<FileBasedGroupProviderImpl>
{
    public static final String GROUP_FILE_PROVIDER_TYPE = "GroupFile";
    private static Logger LOGGER = LoggerFactory.getLogger(FileBasedGroupProviderImpl.class);

    private final Container<?> _container;

    private FileGroupDatabase _groupDatabase;

    @ManagedAttributeField
    private String _path;

    @ManagedObjectFactoryConstructor
    public FileBasedGroupProviderImpl(Map<String, Object> attributes,
                                      Container<?> container)
    {
        super(container, attributes);
        _container = container;
    }

    @Override
    public void onValidate()
    {
        Collection<GroupProvider> groupProviders = _container.getChildren(GroupProvider.class);
        for(GroupProvider<?> provider : groupProviders)
        {
            if(provider instanceof FileBasedGroupProvider && provider != this)
            {
                try
                {
                    if(new File(getPath()).getCanonicalPath().equals(new File(((FileBasedGroupProvider)provider).getPath()).getCanonicalPath()))
                    {
                        throw new IllegalConfigurationException("Cannot have two group providers using the same file: " + getPath());
                    }
                }
                catch (IOException e)
                {
                    throw new IllegalArgumentException("Invalid path", e);
                }
            }
        }
        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        FileGroupDatabase groupDatabase = new FileGroupDatabase(this);
        try
        {
            groupDatabase.setGroupFile(getPath());
        }
        catch(IOException | RuntimeException e)
        {
            if (e instanceof IllegalConfigurationException)
            {
                throw (IllegalConfigurationException) e;
            }
            throw new IllegalConfigurationException(String.format("Cannot load groups from '%s'", getPath()), e);
        }

        _groupDatabase = groupDatabase;

        Set<Principal> groups = getGroupPrincipals();
        Collection<Group> principals = new ArrayList<>(groups.size());
        for (Principal group : groups)
        {
            Map<String,Object> attrMap = new HashMap<String, Object>();
            UUID id = UUID.randomUUID();
            attrMap.put(ConfiguredObject.ID, id);
            attrMap.put(ConfiguredObject.NAME, group.getName());
            GroupAdapter groupAdapter = new GroupAdapter(attrMap);
            principals.add(groupAdapter);
            groupAdapter.registerWithParents();
            // TODO - we know this is safe, but the sync method shouldn't really be called from the management thread
            groupAdapter.openAsync();
        }

    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
        File file = new File(_path);
        if (!file.exists())
        {
            File parent = file.getAbsoluteFile().getParentFile();
            if (!parent.exists() && !parent.mkdirs())
            {
                throw new IllegalConfigurationException(String.format("Cannot create groups file at '%s'",_path));
            }

            try
            {
                String posixFileAttributes = getContextValue(String.class, SystemConfig.POSIX_FILE_PERMISSIONS);
                new FileHelper().createNewFile(file, posixFileAttributes);
            }
            catch (IOException e)
            {
                throw new IllegalConfigurationException(String.format("Cannot create groups file at '%s'", _path), e);
            }
        }
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        File groupsFile = new File(_path);
        if (groupsFile.exists())
        {
            if (!groupsFile.canRead())
            {
                throw new IllegalConfigurationException(String.format("Cannot read groups file '%s'. Please check permissions.", _path));
            }

            FileGroupDatabase groupDatabase = new FileGroupDatabase(this);
            try
            {
                groupDatabase.setGroupFile(_path);
            }
            catch (Exception e)
            {
                throw new IllegalConfigurationException(String.format("Cannot load groups from '%s'", _path), e);
            }
        }
    }

    @Override
    public String getPath()
    {
        return _path;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                          Map<String, Object> attributes)
    {
        if (childClass == Group.class)
        {
            String groupName = (String) attributes.get(ConfiguredObject.NAME);

            if (getState() != State.ACTIVE)
            {
                throw new IllegalConfigurationException(String.format("Group provider '%s' is not activated. Cannot create a group.", getName()));
            }

            _groupDatabase.createGroup(groupName);

            Map<String,Object> attrMap = new HashMap<String, Object>();
            UUID id = UUID.randomUUID();
            attrMap.put(ConfiguredObject.ID, id);
            attrMap.put(ConfiguredObject.NAME, groupName);
            GroupAdapter groupAdapter = new GroupAdapter(attrMap);
            groupAdapter.create();
            return Futures.immediateFuture((C) groupAdapter);

        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }

    private Set<Principal> getGroupPrincipals()
    {

        Set<String> groups = _groupDatabase == null ? Collections.<String>emptySet() : _groupDatabase.getAllGroups();
        if (groups.isEmpty())
        {
            return Collections.emptySet();
        }
        else
        {
            Set<Principal> principals = new HashSet<Principal>();
            for (String groupName : groups)
            {
                principals.add(new GroupPrincipal(groupName, this));
            }
            return principals;
        }
    }

    @StateTransition( currentState = { State.UNINITIALIZED, State.QUIESCED, State.ERRORED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> activate()
    {
        if (_groupDatabase != null)
        {
            setState(State.ACTIVE);
        }
        else
        {
            if (getAncestor(SystemConfig.class).isManagementMode())
            {
                LOGGER.warn("Failed to activate group provider: {}", getName());
            }
            else
            {
                throw new IllegalConfigurationException(String.format("Cannot load groups from '%s'", getPath()));
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        // We manage the storage children so we close (so they may free any resources) them rather than deleting them
        return doAfterAlways(closeChildren(),
                             () -> {
                                 File file = new File(getPath());
                                 if (file.exists())
                                 {
                                     if (!file.delete())
                                     {
                                         throw new IllegalConfigurationException(String.format(
                                                 "Cannot delete group file '%s'",
                                                 file));
                                     }
                                 }
                             });
    }

    @StateTransition( currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    private ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }

    @Override
    public Set<Principal> getGroupPrincipalsForUser(Principal userPrincipal)
    {
        Set<String> groups = _groupDatabase == null ? Collections.<String>emptySet() : _groupDatabase.getGroupsForUser(userPrincipal.getName());
        if (groups.isEmpty())
        {
            return Collections.emptySet();
        }
        else
        {
            Set<Principal> principals = new HashSet<Principal>();
            for (String groupName : groups)
            {
                principals.add(new GroupPrincipal(groupName, this));
            }
            return principals;
        }
    }

    private class GroupAdapter extends AbstractConfiguredObject<GroupAdapter> implements Group<GroupAdapter>
    {
        public GroupAdapter(Map<String, Object> attributes)
        {
            super(FileBasedGroupProviderImpl.this, attributes);
        }

        @Override
        public void onValidate()
        {
            super.onValidate();
            if(!isDurable())
            {
                throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
            }
        }

        @StateTransition( currentState = State.UNINITIALIZED, desiredState = State.ACTIVE )
        private ListenableFuture<Void> activate()
        {
            setState(State.ACTIVE);
            return Futures.immediateFuture(null);
        }

        @Override
        protected void onOpen()
        {
            super.onOpen();
            Set<String> usersInGroup = _groupDatabase.getUsersInGroup(getName());
            Collection<GroupMember> members = new ArrayList<GroupMember>();
            for (String username : usersInGroup)
            {
                UUID id = UUID.randomUUID();
                Map<String,Object> attrMap = new HashMap<String, Object>();
                attrMap.put(ConfiguredObject.ID,id);
                attrMap.put(ConfiguredObject.NAME, username);
                GroupMemberAdapter groupMemberAdapter = new GroupMemberAdapter(attrMap);
                groupMemberAdapter.registerWithParents();
                // todo - this will be safe, but the synchronous open should not be called from the management thread
                groupMemberAdapter.openAsync();
                members.add(groupMemberAdapter);
            }
        }

        @Override
        protected  <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                              Map<String, Object> attributes)
        {
            if (childClass == GroupMember.class)
            {
                String memberName = (String) attributes.get(GroupMember.NAME);

                _groupDatabase.addUserToGroup(memberName, getName());
                UUID id = UUID.randomUUID();
                Map<String,Object> attrMap = new HashMap<String, Object>();
                attrMap.put(GroupMember.ID,id);
                attrMap.put(GroupMember.NAME, memberName);
                GroupMemberAdapter groupMemberAdapter = new GroupMemberAdapter(attrMap);
                groupMemberAdapter.create();
                return Futures.immediateFuture((C) groupMemberAdapter);

            }
            else
            {
                return super.addChildAsync(childClass, attributes);
            }
        }

        @Override
        protected ListenableFuture<Void> onDelete()
        {
            _groupDatabase.removeGroup(getName());
            return super.onDelete();

        }

        private class GroupMemberAdapter extends AbstractConfiguredObject<GroupMemberAdapter> implements
                GroupMember<GroupMemberAdapter>
        {
            public GroupMemberAdapter(Map<String, Object> attrMap)
            {
                // TODO - need to relate to the User object
                super(GroupAdapter.this, attrMap);
            }

            @Override
            public void onValidate()
            {
                super.onValidate();
                if(!isDurable())
                {
                    throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
                }
            }

            @Override
            public <C extends ConfiguredObject> Collection<C> getChildren(
                    Class<C> clazz)
            {
                return Collections.emptySet();
            }

            @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.ACTIVE)
            private ListenableFuture<Void> activate()
            {
                setState(State.ACTIVE);
                return Futures.immediateFuture(null);
            }

            @Override
            protected ListenableFuture<Void> onDelete()
            {
                _groupDatabase.removeUserFromGroup(getName(), GroupAdapter.this.getName());
                return super.onDelete();
            }
        }
    }


}
