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
package org.apache.qpid.systest.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.User;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class UserRestTest extends QpidRestTestCase
{
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        getRestTestHelper().setUsernameAndPassword("user1", "user1");
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile("user1", "user2");
    }

    public void testGet() throws Exception
    {
        List<Map<String, Object>> users = getRestTestHelper().getJsonAsList("user");
        assertNotNull("Users cannot be null", users);
        assertTrue("Unexpected number of users", users.size() > 1);
        for (Map<String, Object> user : users)
        {
            assertUser(user);
        }
    }

    public void testGetUserByName() throws Exception
    {
        List<Map<String, Object>> users = getRestTestHelper().getJsonAsList("user");
        assertNotNull("Users cannot be null", users);
        assertTrue("Unexpected number of users", users.size() > 1);
        for (Map<String, Object> user : users)
        {
            assertNotNull("Attribute " + User.ID, user.get(User.ID));
            String userName = (String) user.get(User.NAME);
            assertNotNull("Attribute " + User.NAME, userName);
            Map<String, Object> userDetails = getRestTestHelper().getJsonAsSingletonList("user/"
                    + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER + "/" + userName);
            assertUser(userDetails);
            assertEquals("Unexpected user name", userName, userDetails.get(User.NAME));
        }
    }

    public void testCreateUserByPutUsingUserURI() throws Exception
    {
        String userName = getTestName();
        getRestTestHelper().createOrUpdateUser(userName, "newPassword");

        Map<String, Object> userDetails = getRestTestHelper().getJsonAsSingletonList("user/"
                + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER + "/" + userName);
        assertUser(userDetails);
        assertEquals("Unexpected user name", userName, userDetails.get(User.NAME));
    }

    public void testCreateUserByPostUsingParentURI() throws Exception
    {
        String userName = getTestName();

        Map<String,Object> userAttributes = new HashMap<>();
        userAttributes.put("password", "newPassword");
        userAttributes.put("name", userName);

        String url = "user/" + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER;
        getRestTestHelper().submitRequest(url, "POST", userAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> userDetails = getRestTestHelper().getJsonAsSingletonList(url+ "/" + userName);
        assertUser(userDetails);
        assertEquals("Unexpected user name", userName, userDetails.get(User.NAME));

        // verify that second create request fails
        getRestTestHelper().submitRequest(url, "POST", userAttributes, HttpServletResponse.SC_CONFLICT);
    }

    public void testCreateUserByPutUsingParentURI() throws Exception
    {
        String userName = getTestName();

        Map<String,Object> userAttributes = new HashMap<>();
        userAttributes.put("password", "newPassword");
        userAttributes.put("name", userName);

        String url = "user/" + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER;
        getRestTestHelper().submitRequest(url, "PUT", userAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> userDetails = getRestTestHelper().getJsonAsSingletonList(url+ "/" + userName);
        assertUser(userDetails);
        assertEquals("Unexpected user name", userName, userDetails.get(User.NAME));

        // verify that second create request fails
        getRestTestHelper().submitRequest(url, "PUT", userAttributes, HttpServletResponse.SC_CONFLICT);
    }

    public void testSetPasswordForNonExistingUserByPostFails() throws Exception
    {
        String userName = getTestName();

        Map<String,Object> userAttributes = new HashMap<>();
        userAttributes.put("password", "newPassword");
        userAttributes.put("name", userName);

        String url = "user/" + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER + "/" + userName;
        getRestTestHelper().submitRequest(url, "POST", userAttributes, HttpServletResponse.SC_NOT_FOUND);
        getRestTestHelper().submitRequest(url, "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    public void testDelete() throws Exception
    {
        String userName = getTestName();
        getRestTestHelper().createOrUpdateUser(userName, "newPassword");

        Map<String, Object> userDetails = getRestTestHelper().getJsonAsSingletonList("user/"
                + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER + "/" + userName);
        String id = (String) userDetails.get(User.ID);

        getRestTestHelper().removeUserById(id);

        getRestTestHelper().submitRequest("user/" + TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER
                + "/" + userName, "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    private void assertUser(Map<String, Object> user)
    {
        assertNotNull("Attribute " + User.ID, user.get(User.ID));
        assertNotNull("Attribute " + User.NAME, user.get(User.NAME));
    }
}
