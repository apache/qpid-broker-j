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
package org.apache.qpid.server.security.group;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.adapter.FileBasedGroupProvider;
import org.apache.qpid.server.util.FileHelper;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * A group database that reads/writes the following file format:
 * <p>
 * group1.users=user1,user2
 * group2.users=user2,user3
 */
public class FileGroupDatabase implements GroupDatabase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileGroupDatabase.class);

    private final Map<String, Set<String>> _groupToUserMap;
    private final Map<String, Set<String>> _userToGroupMap;
    private final FileBasedGroupProvider<?>  _groupProvider;
    private String _groupFile;

    public FileGroupDatabase(FileBasedGroupProvider<?> groupProvider)
    {
        this._groupProvider = groupProvider;
        _groupToUserMap = new ConcurrentHashMap<>();
        _userToGroupMap = new ConcurrentHashMap<>();
    }

    @Override
    public Set<String> getAllGroups()
    {
        return Collections.unmodifiableSet(_groupToUserMap.keySet());
    }

    public synchronized void setGroupFile(String groupFile) throws IOException
    {
        File file = new File(groupFile);

        if (!file.canRead())
        {
            throw new FileNotFoundException(groupFile + " cannot be found or is not readable");
        }

        readGroupFile(groupFile);
    }

    @Override
    public Set<String> getUsersInGroup(String group)
    {
        if (group == null)
        {
            LOGGER.warn("Requested user set for null group. Returning empty set.");
            return Collections.emptySet();
        }

        Set<String> set = _groupToUserMap.get(keySearch(_groupToUserMap.keySet(), group));
        if (set == null)
        {
            return Collections.emptySet();
        }
        else
        {
            return Collections.unmodifiableSet(set);
        }
    }

    @Override
    public synchronized void addUserToGroup(String user, String group)
    {
        final String groupKey = keySearch(_groupToUserMap.keySet(), group);
        final Set<String> groupUsers = _groupToUserMap.get(groupKey);
        final String userKey = keySearch(_userToGroupMap.keySet(), user);
        if (groupUsers == null)
        {
            throw new IllegalArgumentException(String.format("Group %s does not exist so could not add %s to it",
                                                             group,
                                                             user));
        }
        else if (groupUsers.contains(userKey))
        {
            throw new IllegalConfigurationException(String.format("Group member with name '%s' already exists", user));
        }

        final String groupUserKey = keySearch(groupUsers, user);
        if (!userKey.equals(groupUserKey))
        {
            throw new IllegalConfigurationException(String.format(
                    "Inconsistent data: user  key '%s' is not equal to a group key '%s'",
                    userKey,
                    groupKey));
        }
        else
        {
            groupUsers.add(groupUserKey);
        }
        Set<String> groups = _userToGroupMap.get(userKey);
        if (groups == null)
        {
            groups = new ConcurrentSkipListSet<>();
            _userToGroupMap.put(user, groups);
        }
        groups.add(groupKey);

        update();
    }

    @Override
    public synchronized void removeUserFromGroup(String user, String group)
    {
        Set<String> users = _groupToUserMap.get(keySearch(_groupToUserMap.keySet(), group));
        if (users == null)
        {
            throw new IllegalArgumentException("Group "
                                               + group
                                               + " does not exist so could not remove "
                                               + user
                                               + " from it");
        }

        users.remove(keySearch(users, user));

        Set<String> groups = _userToGroupMap.get(keySearch(_userToGroupMap.keySet(), user));
        if (groups != null)
        {
            groups.remove(keySearch(groups, group));
        }

        update();
    }

    @Override
    public Set<String> getGroupsForUser(String user)
    {
        if (user == null)
        {
            LOGGER.warn("Requested group set for null user. Returning empty set.");
            return Collections.emptySet();
        }

        Set<String> groups = _userToGroupMap.get(keySearch(_userToGroupMap.keySet(), user));
        if (groups == null)
        {
            return Collections.emptySet();
        }
        else
        {
            return Collections.unmodifiableSet(groups);
        }
    }

    @Override
    public synchronized void createGroup(String group)
    {
        if (!exists(group, _groupToUserMap.keySet()))
        {
            Set<String> users = new ConcurrentSkipListSet<>();
            _groupToUserMap.put(group, users);
            update();
        }
        else
        {
            throw new IllegalConfigurationException(String.format("Group with name '%s' already exists", group));
        }
    }

    private boolean exists(final String searchString, final Set<String> set)
    {
        if (!_groupProvider.isCaseSensitive())
        {
            for (String key : set)
            {
                if (key.equalsIgnoreCase(searchString))
                {
                    return true;
                }
            }
            return false;
        }
        return set.contains(searchString);
    }

    @Override
    public synchronized void removeGroup(String group)
    {
        _groupToUserMap.remove(keySearch(_groupToUserMap.keySet(), group));
        for (Set<String> groupsForUser : _userToGroupMap.values())
        {
            groupsForUser.remove(keySearch(groupsForUser, group));
        }

        update();
    }

    private synchronized void update()
    {
        if (_groupFile != null)
        {
            try
            {
                writeGroupFile(_groupFile);
            }
            catch (IOException e)
            {
                throw new ServerScopedRuntimeException("Unable to persist change to file " + _groupFile, e);
            }
        }
    }

    private synchronized void readGroupFile(String groupFile) throws IOException
    {
        _groupFile = groupFile;
        _groupToUserMap.clear();
        _userToGroupMap.clear();
        Properties propertiesFile = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(groupFile))
        {
            propertiesFile.load(fileInputStream);
        }

        for (String propertyName : propertiesFile.stringPropertyNames())
        {
            validatePropertyNameIsGroupName(propertyName);

            String groupName = propertyName.replaceAll("\\.users$", "");
            String userString = propertiesFile.getProperty(propertyName);

            final Set<String> userSet = buildUserSetFromCommaSeparateValue(userString);

            _groupToUserMap.put(groupName, userSet);

            for (String userName : userSet)
            {
                Set<String> groupsForThisUser = _userToGroupMap.get(keySearch(_userToGroupMap.keySet(), userName));

                if (groupsForThisUser == null)
                {
                    groupsForThisUser = new ConcurrentSkipListSet<>();
                    _userToGroupMap.put(userName, groupsForThisUser);
                }

                groupsForThisUser.add(groupName);
            }
        }
    }

    private synchronized void writeGroupFile(final String groupFile) throws IOException
    {
        final Properties propertiesFile = new Properties();

        for (String group : _groupToUserMap.keySet())
        {
            Set<String> users = _groupToUserMap.get(keySearch(_groupToUserMap.keySet(), group));
            final String userList = Joiner.on(",").useForNull("").join(users);

            propertiesFile.setProperty(group + ".users", userList);
        }

        new FileHelper().writeFileSafely(new File(groupFile).toPath(), file ->
        {
            String comment = "Written " + new Date();
            try (FileOutputStream fileOutputStream = new FileOutputStream(file))
            {
                propertiesFile.store(fileOutputStream, comment);
            }
        });
    }

    private void validatePropertyNameIsGroupName(String propertyName)
    {
        if (!propertyName.endsWith(".users"))
        {
            throw new IllegalArgumentException("Invalid definition with name '"
                                               + propertyName
                                               + "'. Group definitions must end with suffix '.users'");
        }
    }

    private ConcurrentSkipListSet<String> buildUserSetFromCommaSeparateValue(String userString)
    {
        String[] users = userString.split(",");
        final ConcurrentSkipListSet<String> userSet = new ConcurrentSkipListSet<>();
        for (String user : users)
        {
            final String trimmed = user.trim();
            if (!trimmed.isEmpty())
            {
                userSet.add(trimmed);
            }
        }
        return userSet;
    }

    private String keySearch(Set<String> set, String requiredKey)
    {
        if (!_groupProvider.isCaseSensitive())
        {
            for (String key : set)
            {
                if (key.equalsIgnoreCase(requiredKey))
                {
                    return key;
                }
            }
        }
        return requiredKey;
    }
}
