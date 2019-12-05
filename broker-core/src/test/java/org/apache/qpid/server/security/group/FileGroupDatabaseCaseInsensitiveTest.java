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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.adapter.FileBasedGroupProvider;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileGroupDatabaseCaseInsensitiveTest extends UnitTestBase
{
    private static final String USER1 = "user1";
    private static final String USER2 = "user2";
    private static final String USER3 = "user3";

    private static final String MY_GROUP = "myGroup";
    private static final String MY_GROUP2 = "myGroup2";
    private static final String MY_GROUP1 = "myGroup1";

    private static final boolean CASE_SENSITIVE = false;
    private FileGroupDatabase _fileGroupDatabase;
    private GroupProviderUtil _util;
    private String _groupFile;
    private FileBasedGroupProvider _groupProvider;

    @Before
    public void setUp() throws IOException
    {
        _groupProvider = mock(FileBasedGroupProvider.class);
        when(_groupProvider.isCaseSensitive()).thenReturn(CASE_SENSITIVE);
        _fileGroupDatabase = new FileGroupDatabase(_groupProvider);
        _util = new GroupProviderUtil(_fileGroupDatabase);
        _groupFile = _util.getGroupFile();
    }

    @Test
    public void testGetUsersInGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
        Set<String> users2 = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users2);
        assertEquals(3, users2.size());
        Set<String> users3 = _fileGroupDatabase.getUsersInGroup("MyGrouP");
        assertNotNull(users3);
        assertEquals(3, users3.size());
    }

    @Test
    public void testDuplicateUsersInGroupAreConflatedCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user1,user3,user1");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());
    }

    @Test
    public void testGetUsersWithEmptyGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToOneGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> groups2 = _fileGroupDatabase.getGroupsForUser("User2");
        assertEquals(1, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToTwoGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2",
                                   "myGroup1.users", "user1,user3",
                                   "myGroup2.users", "user2,user3");

        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP));
        assertTrue(groups.contains(MY_GROUP1));

        Set<String> groups2 = _fileGroupDatabase.getGroupsForUser("User2");
        assertEquals(2, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
        assertTrue(groups2.contains(MY_GROUP2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", USER2);
        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));

        _fileGroupDatabase.addUserToGroup(USER1, MY_GROUP2.toUpperCase());

        groups = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP2.toUpperCase());
        assertEquals(2, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(users.contains(USER2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserRemovedFromGroupCaseInsensitive() throws Exception
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
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupTheyAreAlreadyInCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", USER1);
        _fileGroupDatabase.addUserToGroup(USER1, MY_GROUP);

        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());

        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(1, users.size());
        assertTrue(users.contains(USER1));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserNotKnownCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");
        Set<String> groups = _fileGroupDatabase.getGroupsForUser(USER3.toUpperCase());
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForNullUserCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile();
        assertTrue(_fileGroupDatabase.getGroupsForUser(null).isEmpty());
    }

    @Test
    public void testAddUserToExistingGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP);

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(3, users.size());
    }

    @Test
    public void testAddUserToEmptyGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP.toUpperCase());

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testAddUserToNonExistentGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile();

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        try
        {
            _fileGroupDatabase.addUserToGroup(USER3, MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testRemoveUserFromExistingGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        _fileGroupDatabase.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP.toUpperCase());

        users = _fileGroupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testRemoveUserFromNonexistentGroupCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile();

        try
        {
            _fileGroupDatabase.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testRemoveUserFromGroupTwiceCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", USER1);
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).contains(USER1));

        _fileGroupDatabase.removeUserFromGroup(USER1, MY_GROUP.toUpperCase());
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).isEmpty());

        _fileGroupDatabase.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
        assertTrue(_fileGroupDatabase.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testAddUserPersistedToFileCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        _fileGroupDatabase.addUserToGroup(USER3.toUpperCase(), MY_GROUP);
        assertEquals(3, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testRemoveUserPersistedToFileCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = _fileGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        _fileGroupDatabase.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP);
        assertEquals(1, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testCreateGroupPersistedToFileCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile();

        Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertTrue(groups.isEmpty());

        _fileGroupDatabase.createGroup(MY_GROUP);

        groups = _fileGroupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP));
    }

    @Test
    public void testRemoveGroupPersistedToFileCaseInsensitive() throws Exception
    {
        _util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");

        Set<String> groups = _fileGroupDatabase.getAllGroups();
        assertEquals(2, groups.size());

        Set<String> groupsForUser1 = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groupsForUser1.size());

        _fileGroupDatabase.removeGroup(MY_GROUP1.toUpperCase());

        groups = _fileGroupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP2));

        groupsForUser1 = _fileGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groupsForUser1.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(_groupProvider);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP2));

        Set<String> newGroupsForUser1 = newGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, newGroupsForUser1.size());
        assertTrue(newGroupsForUser1.contains(MY_GROUP2));
    }

    @After
    public void tearDown()
    {
        if (_groupFile != null)
        {
            File groupFile = new File(_groupFile);
            if (groupFile.exists())
            {
                groupFile.delete();
            }
        }
    }
}
