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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class FileGroupDatabaseTest extends UnitTestBase
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

        CASE_SENSITIVE = true;
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
    public void testGetAllGroups() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", USER1);

        Set<String> groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));
    }

    @Test
    public void testGetAllGroupsWhenGroupFileEmpty() throws Exception
    {
        FILE_GROUP_DATABASE.setGroupFile(UTIL.createEmptyTestGroupFile());

        Set<String> groups = FILE_GROUP_DATABASE.getAllGroups();
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testMissingGroupFile() throws Exception
    {
        try
        {
            FILE_GROUP_DATABASE.setGroupFile("/not/a/file");
            fail("Exception not thrown");
        }
        catch (FileNotFoundException fnfe)
        {
            // PASS
        }
    }

    @Test
    public void testInvalidFormat() throws Exception
    {
        UTIL.writeGroupFile("name.notvalid", USER1);

        try
        {
            FILE_GROUP_DATABASE.setGroupFile(GROUP_FILE);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException gde)
        {
            // PASS
        }
    }

    @Test
    public void testGetUsersInGroup() throws Exception
    {
        UTIL.writeGroupFile("myGroup.users", "user1,user2,user3");

        FILE_GROUP_DATABASE.setGroupFile(GROUP_FILE);

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
    }

    @Test
    public void testDuplicateUsersInGroupAreConflated() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user1,user3,user1");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());
    }

    @Test
    public void testGetUsersWithEmptyGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetUsersInNonExistentGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2,user3");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup("groupDoesntExist");
        assertNotNull(users);
        assertTrue(users.isEmpty());
    }

    @Test
    public void testGetUsersInNullGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile();
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(null).isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToOneGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserBelongsToTwoGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user3");
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", USER2);
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP1));

        FILE_GROUP_DATABASE.addUserToGroup(USER1, MY_GROUP2);

        groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(2, groups.size());
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase()).isEmpty());
        assertTrue(groups.contains(MY_GROUP1));
        assertTrue(groups.contains(MY_GROUP2));

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP2);
        assertEquals(2, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(users.contains(USER2));
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserRemovedFromGroup() throws Exception
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
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(USER1.toUpperCase()).isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserAddedToGroupTheyAreAlreadyIn() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", USER1);
        FILE_GROUP_DATABASE.addUserToGroup(USER1, MY_GROUP);

        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER1);

        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP));

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertEquals(1, users.size());
        assertTrue(users.contains(USER1));
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForUserWhenUserNotKnown() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");
        Set<String> groups = FILE_GROUP_DATABASE.getGroupsForUser(USER3);
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetGroupPrincipalsForNullUser() throws Exception
    {
        UTIL.writeAndSetGroupFile();
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(null).isEmpty());
    }

    @Test
    public void testAddUserToExistingGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP);

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(3, users.size());
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testAddUserToEmptyGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertTrue(users.isEmpty());

        FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP);

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
        assertTrue(FILE_GROUP_DATABASE.getGroupsForUser(MY_GROUP.toUpperCase()).isEmpty());
    }

    @Test
    public void testAddUserToNonExistentGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile();

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
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
    public void testRemoveUserFromExistingGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER2, MY_GROUP);

        users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertNotNull(users);
        assertEquals(1, users.size());
    }

    @Test
    public void testRemoveUserFromNonexistentGroup() throws Exception
    {
        UTIL.writeAndSetGroupFile();

        try
        {
            FILE_GROUP_DATABASE.removeUserFromGroup(USER1, MY_GROUP);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testRemoveUserFromGroupTwice() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", USER1);
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).contains(USER1));

        FILE_GROUP_DATABASE.removeUserFromGroup(USER1, MY_GROUP);
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).isEmpty());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER1, MY_GROUP);
        assertTrue(FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP).isEmpty());
    }

    @Test
    public void testAddUserPersistedToFile() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.addUserToGroup(USER3, MY_GROUP);
        assertEquals(3, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testRemoveUserPersistedToFile() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup.users", "user1,user2");

        Set<String> users = FILE_GROUP_DATABASE.getUsersInGroup(MY_GROUP);
        assertEquals(2, users.size());

        FILE_GROUP_DATABASE.removeUserFromGroup(USER2, MY_GROUP);
        assertEquals(1, users.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newUsers = newGroupDatabase.getUsersInGroup(MY_GROUP);
        assertEquals(users.size(), newUsers.size());
    }

    @Test
    public void testCreateGroupPersistedToFile() throws Exception
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
    public void testRemoveGroupPersistedToFile() throws Exception
    {
        UTIL.writeAndSetGroupFile("myGroup1.users", "user1,user2", "myGroup2.users", "user1,user2");

        Set<String> groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(2, groups.size());

        Set<String> groupsForUser1 = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(2, groupsForUser1.size());

        FILE_GROUP_DATABASE.removeGroup(MY_GROUP1);

        groups = FILE_GROUP_DATABASE.getAllGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.contains(MY_GROUP2));

        groupsForUser1 = FILE_GROUP_DATABASE.getGroupsForUser(USER1);
        assertEquals(1, groupsForUser1.size());

        FileGroupDatabase newGroupDatabase = new FileGroupDatabase(CASE_SENSITIVE);
        newGroupDatabase.setGroupFile(GROUP_FILE);

        Set<String> newGroups = newGroupDatabase.getAllGroups();
        assertEquals(1, newGroups.size());
        assertTrue(newGroups.contains(MY_GROUP2));

        Set<String> newGroupsForUser1 = newGroupDatabase.getGroupsForUser(USER1);
        assertEquals(1, newGroupsForUser1.size());
        assertTrue(newGroupsForUser1.contains(MY_GROUP2));
    }

    @AfterClass
    public static void tearDown()
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
