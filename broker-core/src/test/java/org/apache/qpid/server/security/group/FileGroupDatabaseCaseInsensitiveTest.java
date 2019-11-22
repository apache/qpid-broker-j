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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

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
    private static final FileGroupDatabase groupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
    private static GroupProviderUtil util;
    private String _groupFile = util.getGroupFile();

    static
    {
        try
        {
            util = new GroupProviderUtil(groupDatabase);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetUsersInGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");
        groupDatabase.setGroupFile(_groupFile);

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
        Set<String> users2 = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users2);
        assertEquals(3, users2.size());
        Set<String> users3 = groupDatabase.getUsersInGroup("MyGrouP");
        assertNotNull(users3);
        assertEquals(3, users3.size());
    }

    @Test
    public void testDuplicateUsersInGroupAreConflatedCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user1,user3,user1");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());
    }

    @Test
    public void testGetUsersWithEmptyGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToOneGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");
        groupDatabase.setGroupFile(_groupFile);

        Set<String> groups = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> groups2 = groupDatabase.getGroupsForUser("User2");
        assertEquals(1, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToTwoGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2",
                                  "myGroup1.users", "user1,user3",
                                  "myGroup2.users", "user2,user3");
        groupDatabase.setGroupFile(_groupFile);

        Set<String> groups = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP));
        assertTrue(groups.contains(MY_GROUP1));

        Set<String> groups2 = groupDatabase.getGroupsForUser("User2");
        assertEquals(2, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
        assertTrue(groups2.contains(MY_GROUP2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", USER2);
        Set<String> groups = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));

        groupDatabase.addUserToGroup(USER1, MY_GROUP2.toUpperCase());

        groups = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP2.toUpperCase());
        assertEquals(2, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(users.contains(USER2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserRemovedFromGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");
        Set<String> groups = groupDatabase.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        groupDatabase.removeUserFromGroup(USER1, MY_GROUP2);

        groups = groupDatabase.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupTheyAreAlreadyInCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", USER1);
        groupDatabase.addUserToGroup(USER1, MY_GROUP);

        Set<String> groups = groupDatabase.getGroupsForUser(USER1.toUpperCase());

        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(1, users.size());
        assertTrue(users.contains(USER1));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserNotKnownCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");
        Set<String> groups = groupDatabase.getGroupsForUser(USER3.toUpperCase());
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForNullUserCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile();
        assertTrue(groupDatabase.getGroupsForUser(null).isEmpty());
    }

    @Test
    public void testAddUserToExistingGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        groupDatabase.addUserToGroup(USER3, MY_GROUP);

        users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(3, users.size());
    }

    @Test
    public void testAddUserToEmptyGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        groupDatabase.addUserToGroup(USER3, MY_GROUP.toUpperCase());

        users = groupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testAddUserToNonExistentGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile();

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        try
        {
            groupDatabase.addUserToGroup(USER3, MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        users = groupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testRemoveUserFromExistingGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        groupDatabase.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP.toUpperCase());

        users = groupDatabase.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testRemoveUserFromNonexistentGroupCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile();

        try
        {
            groupDatabase.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        assertTrue(groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testRemoveUserFromGroupTwiceCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", USER1);
        assertTrue(groupDatabase.getUsersInGroup(MY_GROUP).contains(USER1));

        groupDatabase.removeUserFromGroup(USER1, MY_GROUP.toUpperCase());
        assertTrue(groupDatabase.getUsersInGroup(MY_GROUP).isEmpty());

        groupDatabase.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
        assertTrue(groupDatabase.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testAddUserPersistedToFileCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        groupDatabase.addUserToGroup(USER3.toUpperCase(), MY_GROUP);
        assertEquals(3, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testRemoveUserPersistedToFileCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = groupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        groupDatabase.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP);
        assertEquals(1, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testCreateGroupPersistedToFileCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile();

        Set<String> groups = groupDatabase.getAllGroups();
        assertTrue(groups.isEmpty());

        groupDatabase.createGroup(MY_GROUP);

        groups = groupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP));
    }

    @Test
    public void testRemoveGroupPersistedToFileCaseInsensitive() throws Exception
    {
        util.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");

        Set<String> groups = groupDatabase.getAllGroups();
        assertEquals(2, groups.size());

        Set<String> groupsForUser1 = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groupsForUser1.size());

        groupDatabase.removeGroup(MY_GROUP1.toUpperCase());

        groups = groupDatabase.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP2));

        groupsForUser1 = groupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groupsForUser1.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(_groupFile);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP2));

        Set<String> newGroupsForUser1 = newGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, newGroupsForUser1.size());
        assertTrue(newGroupsForUser1.contains(MY_GROUP2));
    }

    @After
    public void tearDown() throws Exception
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
