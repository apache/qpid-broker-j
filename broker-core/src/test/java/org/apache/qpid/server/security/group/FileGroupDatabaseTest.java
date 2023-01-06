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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.adapter.FileBasedGroupProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileGroupDatabaseTest extends UnitTestBase
{
    private static final String USER1 = "user1";
    private static final String USER2 = "user2";
    private static final String USER3 = "user3";

    private static final String MY_GROUP = "myGroup";
    private static final String MY_GROUP2 = "myGroup2";
    private static final String MY_GROUP1 = "myGroup1";

    private static final boolean CASE_SENSITIVE = true;

    private FileGroupDatabase _fileGroupDatabase;
    private GroupProviderUtil _util;
    private String _groupFile;
    private FileBasedGroupProvider<?> _groupProvider;

    @BeforeEach
    public void setUp() throws Exception
    {
        _groupProvider = mock(FileBasedGroupProvider.class);
        when(_groupProvider.isCaseSensitive()).thenReturn(CASE_SENSITIVE);
        _fileGroupDatabase = new FileGroupDatabase(_groupProvider);
        _util = new GroupProviderUtil(_fileGroupDatabase);
        _groupFile = _util.getGroupFile();
    }

    @Test
    public void testGetAllGroups() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", USER1);

        final Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));
    }

    @Test
    public void testGetAllGroupsWhenGroupFileEmpty()
    {
        final Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testMissingGroupFile()
    {
        assertThrows(FileNotFoundException.class,
                () -> _fileGroupDatabase.setGroupFile("/not/a/file"),
                "Exception not thrown");
    }

    @Test
    public void testInvalidFormat() throws Exception
    {
        _util.writeGroupFile("name.notvalid", USER1);

        assertThrows(IllegalArgumentException.class,
                () -> _fileGroupDatabase.setGroupFile(_groupFile),
                "Exception not thrown");
    }

    @Test
    public void testGetUsersInGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");

        final Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
    }

    @Test
    public void testDuplicateUsersInGroupAreConflated() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user1,user3,user1");

        final Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());
    }

    @Test
    public void testGetUsersWithEmptyGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "");

        final Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetUsersInNonExistentGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");

        final Set<String> users = _fileGroupDatabase.getUsersInGroup("groupDoesntExist");
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetUsersInNullGroup() throws Exception
    {
        _util.writeAndSetGroupFile();
        assertTrue(_fileGroupDatabase.getUsersInGroup(null).isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToOneGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");
        final Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToTwoGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user3");
        final Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", USER2);
        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));

        _fileGroupDatabase.addUserToGroup(USER1, MY_GROUP2);

        groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(_fileGroupDatabase.getGroupsForUser(USER1.toUpperCase()).isEmpty());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        final Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP2);
        assertEquals(2, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(users.contains(USER2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserRemovedFromGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");
        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        _fileGroupDatabase.removeUserFromGroup(USER1, MY_GROUP2);

        groups = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(_fileGroupDatabase.getGroupsForUser(USER1.toUpperCase()).isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupTheyAreAlreadyIn() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", USER1);
        assertThrows(IllegalConfigurationException.class,
                () -> _fileGroupDatabase.addUserToGroup(USER1, MY_GROUP),
                "Expected exception not thrown");
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserNotKnown() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");
        final Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER3);
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForNullUser() throws Exception
    {
        _util.writeAndSetGroupFile();
        assertTrue(_fileGroupDatabase.getGroupsForUser(null).isEmpty());
    }

    @Test
    public void testAddUserToExistingGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());

        _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP);

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
        assertTrue(_fileGroupDatabase.getGroupsForUser(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testAddUserToEmptyGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());

        _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP);

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
        assertTrue(_fileGroupDatabase.getGroupsForUser(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testAddUserToNonExistentGroup() throws Exception
    {
        _util.writeAndSetGroupFile();

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());

        assertThrows(IllegalArgumentException.class,
                () -> _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP),
                "Expected exception not thrown");

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testRemoveUserFromExistingGroup() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());

        _fileGroupDatabase.removeUserFromGroup(USER2, MY_GROUP);

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testRemoveUserFromNonexistentGroup() throws Exception
    {
        _util.writeAndSetGroupFile();

        assertThrows(IllegalArgumentException.class,
                () -> _fileGroupDatabase.removeUserFromGroup(USER1, MY_GROUP),
                "Expected exception not thrown");

        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testRemoveUserFromGroupTwice() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", USER1);
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).contains(USER1));

        _fileGroupDatabase.removeUserFromGroup(USER1, MY_GROUP);
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).isEmpty());

        _fileGroupDatabase.removeUserFromGroup(USER1, MY_GROUP);
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testAddUserPersistedToFile() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(2, users.size());

        _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP);
        assertEquals(3, users.size());

        final FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        final Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testRemoveUserPersistedToFile() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(2, users.size());

        _fileGroupDatabase.removeUserFromGroup(USER2, MY_GROUP);
        assertEquals(1, users.size());

        final FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        final Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testCreateGroupPersistedToFile() throws Exception
    {
        _util.writeAndSetGroupFile();

        Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertTrue(groups.isEmpty());

        _fileGroupDatabase.createGroup(MY_GROUP);

        groups = _fileGroupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        final FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        final Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP));
    }

    @Test
    public void testRemoveGroupPersistedToFile() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");

        Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertEquals(2, groups.size());

        Set<String> groupsForUser1 = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(2, groupsForUser1.size());

        _fileGroupDatabase.removeGroup(MY_GROUP1);

        groups = _fileGroupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP2));

        groupsForUser1 = _fileGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, groupsForUser1.size());

        final FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        final Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP2));

        final Set<String> newGroupsForUser1 = newGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, newGroupsForUser1.size());
        assertTrue(newGroupsForUser1.contains(MY_GROUP2));
    }

    @AfterEach
    public void tearDown()
    {
        if (_groupFile != null)
        {
            final File groupFile = new File(_groupFile);
            if (groupFile.exists())
            {
                groupFile.delete();
            }
        }
    }
}
