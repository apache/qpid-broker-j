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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class FileGroupDatabaseCaseInsensitiveTest extends UnitTestBase
{
    private static String USER1;
    private static String USER2;
    private static String USER3;

    private static String MY_GROUP;
    private static String MY_GROUP2;
    private static String MY_GROUP1;

    private static boolean CASE_SENSITIVE;
    private static FileGroupDatabase FILE_GROUP_DATABASE;
    private static GroupProviderUtil UTIL;
    private static String GROUP_FILE;

    @BeforeClass
    public static void onlyOnce()
    {
        USER1 = "user1";
        USER2 = "user2";
        USER3 = "user3";

        MY_GROUP = "myGroup";
        MY_GROUP2 = "myGroup2";
        MY_GROUP1 = "myGroup1";

        CASE_SENSITIVE = false;
        FILE_GROUP_DATABASE = new FileGroupDatabase(CASE_SENSITIVE);

        try
        {
            UTIL = new GroupProviderUtil(FILE_GROUP_DATABASE);
            GROUP_FILE = UTIL.getGroupFile();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetUsersInGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");
        FILE_GROUP_DATABASE.setGroupFile(GROUP_FILE);

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
        Set<String> users2 = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users2);
        assertEquals(3, users2.size());
        Set<String> users3 = FILE_GROUP_DATABASE.getUsersInGroup("MyGrouP");
        assertNotNull(users3);
        assertEquals(3, users3.size());
    }

    @Test
    public void testDuplicateUsersInGroupAreConflatedCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user1,user3,user1");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());
    }

    @Test
    public void testGetUsersWithEmptyGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToOneGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");
        FILE_GROUP_DATABASE.setGroupFile(GROUP_FILE);

        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> groups2 = FILE_GROUP_DATABASE.getGroupsForUser("User2");
        assertEquals(1, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToTwoGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2",
                                  "myGroup1.users", "user1,user3",
                                  "myGroup2.users", "user2,user3");
        FILE_GROUP_DATABASE.setGroupFile(GROUP_FILE);

        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP));
        assertTrue(groups.contains(MY_GROUP1));

        Set<String> groups2 = FILE_GROUP_DATABASE.getGroupsForUser("User2");
        assertEquals(2, groups2.size());
        assertTrue(groups2.contains(MY_GROUP));
        assertTrue(groups2.contains(MY_GROUP2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", USER2);
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));

        FILE_GROUP_DATABASE.addUserToGroup(USER1, MY_GROUP2.toUpperCase());

        groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP2.toUpperCase());
        assertEquals(2, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(users.contains(USER2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserRemovedFromGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        FILE_GROUP_DATABASE.removeUserFromGroup(USER1, MY_GROUP2);

        groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupTheyAreAlreadyInCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", USER1);
        FILE_GROUP_DATABASE.addUserToGroup(USER1, MY_GROUP);

        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());

        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(1, users.size());
        assertTrue(users.contains(USER1));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserNotKnownCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER3.toUpperCase());
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForNullUserCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile();
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(null).isEmpty());
    }

    @Test
    public void testAddUserToExistingGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP);

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(3, users.size());
    }

    @Test
    public void testAddUserToEmptyGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP.toUpperCase());

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testAddUserToNonExistentGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile();

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertTrue(users.isEmpty());

        try
        {
            FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testRemoveUserFromExistingGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertNotNull(users);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP.toUpperCase());

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testRemoveUserFromNonexistentGroupCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile();

        try
        {
            FILE_GROUP_DATABASE.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testRemoveUserFromGroupTwiceCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", USER1);
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).contains(USER1));

        FILE_GROUP_DATABASE.removeUserFromGroup(USER1, MY_GROUP.toUpperCase());
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).isEmpty());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER1.toUpperCase(), MY_GROUP);
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testAddUserPersistedToFileCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.addUserToGroup(USER3.toUpperCase(), MY_GROUP);
        assertEquals(3, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testRemoveUserPersistedToFileCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER2.toUpperCase(), MY_GROUP);
        assertEquals(1, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP.toUpperCase());
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testCreateGroupPersistedToFileCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile();

        Set<String> groups = FILE_GROUP_DATABASE.getAllGroups();
        assertTrue(groups.isEmpty());

        FILE_GROUP_DATABASE.createGroup(MY_GROUP);

        groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP));
    }

    @Test
    public void testRemoveGroupPersistedToFileCaseInsensitive() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");

        Set<String> groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(2, groups.size());

        Set<String> groupsForUser1 = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(2, groupsForUser1.size());

        FILE_GROUP_DATABASE.removeGroup(MY_GROUP1.toUpperCase());

        groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP2));

        groupsForUser1 = FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, groupsForUser1.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP2));

        Set<String> newGroupsForUser1 = newGroupDatabase.getGroupsForUser(USER1.toUpperCase());
        assertEquals(1, newGroupsForUser1.size());
        assertTrue(newGroupsForUser1.contains(MY_GROUP2));
    }

    @AfterClass
    public void tearDown() throws Exception
    {

        if (GROUP_FILE != null)
        {
            File groupFile = new File(GROUP_FILE);
            if (groupFile.exists())
            {
                groupFile.delete();
            }
        }
    }
}
