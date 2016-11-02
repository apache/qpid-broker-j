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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.servlet.rest.RestServlet;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.AllowAllAccessControlProvider;
import org.apache.qpid.server.security.access.plugins.AclFileAccessControlProvider;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.test.utils.TestFileUtils;

public class AccessControlProviderRestTest extends QpidRestTestCase
{
    private static final String ALLOWED_USER = "allowed";
    private static final String DENIED_USER = "denied";
    private static final String OTHER_USER = "other";

    private String  _aclFileContent1 =
                          "ACL ALLOW-LOG " + ALLOWED_USER + " ACCESS MANAGEMENT\n" +
                          "ACL ALLOW-LOG " + ALLOWED_USER + " CONFIGURE BROKER\n" +
                          "ACL DENY-LOG ALL ALL";

    private String  _aclFileContent2 =
                          "ACL ALLOW-LOG " + ALLOWED_USER + " ACCESS MANAGEMENT\n" +
                          "ACL ALLOW-LOG " + OTHER_USER + " ACCESS MANAGEMENT\n" +
                          "ACL ALLOW-LOG " + ALLOWED_USER + " CONFIGURE BROKER\n" +
                          "ACL DENY-LOG ALL ALL";

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile(ALLOWED_USER, DENIED_USER, OTHER_USER);
    }

    public void testCreateAccessControlProvider() throws Exception
    {
        String accessControlProviderName = getTestName();

        //verify that the access control provider doesn't exist, and
        //in doing so implicitly verify that the 'denied' user can
        //actually currently connect because no ACL is in effect yet
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertAccessControlProviderExistence(accessControlProviderName, false);

        //create the access control provider using the 'allowed' user
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        int responseCode = createAccessControlProvider(accessControlProviderName, _aclFileContent1);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);

        //verify it exists with the 'allowed' user
        assertAccessControlProviderExistence(accessControlProviderName, true);

        //verify the 'denied' user can no longer access the management interface
        //due to the just-created ACL file now preventing it
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertCanAccessManagementInterface(accessControlProviderName, false);
    }

    public void testRemoveAccessControlProvider() throws Exception
    {
        String accessControlProviderName = getTestName();

        //verify that the access control provider doesn't exist, and
        //in doing so implicitly verify that the 'denied' user can
        //actually currently connect because no ACL is in effect yet
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertAccessControlProviderExistence(accessControlProviderName, false);

        //create the access control provider using the 'allowed' user
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        int responseCode = createAccessControlProvider(accessControlProviderName, _aclFileContent1);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);

        //verify it exists with the 'allowed' user
        assertAccessControlProviderExistence(accessControlProviderName, true);

        // add a second, low priority AllowAll provider
        Map<String,Object> attributes = new HashMap<>();
        final String secondProviderName = "AllowAll";
        attributes.put(ConfiguredObject.NAME, secondProviderName);
        attributes.put(ConfiguredObject.TYPE, AllowAllAccessControlProvider.ALLOW_ALL);
        attributes.put(AccessControlProvider.PRIORITY, 9999);
        responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + secondProviderName, "PUT", attributes);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);


        //verify the 'denied' user can no longer access the management interface
        //due to the just-created ACL file now preventing it
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertCanAccessManagementInterface(accessControlProviderName, false);

        //remove the access control provider using the 'allowed' user
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "DELETE");
        assertEquals("Access control provider deletion should be allowed", 200, responseCode);
        assertAccessControlProviderExistence(accessControlProviderName, false);

        //verify it is gone again, using the 'denied' user to implicitly confirm it is
        //now able to connect to the management interface again because the ACL was removed.
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertAccessControlProviderExistence(accessControlProviderName, false);
    }

    public void testReplaceAccessControlProvider() throws Exception
    {


        //create the access control provider using the 'allowed' user
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        int responseCode = createAccessControlProvider(getTestName(), _aclFileContent1);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);

        //verify it exists with the 'allowed' user
        assertAccessControlProviderExistence(getTestName(), true);

        //verify the 'denied' and 'other' user can no longer access the management
        //interface due to the just-created ACL file now preventing them
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertCanAccessManagementInterface(getTestName(), false);
        getRestTestHelper().setUsernameAndPassword(OTHER_USER, OTHER_USER);
        assertCanAccessManagementInterface(getTestName(), false);

        //create the replacement access control provider using the 'allowed' user.
        String accessControlProviderName2 = getTestName() + "2";
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        responseCode = createAccessControlProvider(accessControlProviderName2, _aclFileContent2);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);

        //Verify that first access control provider is used

        //verify the 'denied' user still can't access the management interface
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        assertCanAccessManagementInterface(accessControlProviderName2, false);
        getRestTestHelper().setUsernameAndPassword(OTHER_USER, OTHER_USER);
        assertCanAccessManagementInterface(accessControlProviderName2, false);
    }



    public void testRemovalOfAccessControlProviderInErrorStateUsingManagementMode() throws Exception
    {
        stopDefaultBroker();

        File file = new File(TMP_FOLDER, getTestName());
        if (file.exists())
        {
            file.delete();
        }
        assertFalse("ACL file should not exist", file.exists());
        TestBrokerConfiguration configuration = getDefaultBrokerConfiguration();
        UUID id = configuration.addAclFileConfiguration(file.getAbsolutePath());

        Map<String,Object> attributes = new HashMap<>();
        final String secondProviderName = "AllowAll";
        attributes.put(ConfiguredObject.NAME, secondProviderName);
        attributes.put(ConfiguredObject.TYPE, AllowAllAccessControlProvider.ALLOW_ALL);
        attributes.put(AccessControlProvider.PRIORITY, 9999);
        configuration.addObjectConfiguration(AccessControlProvider.class, attributes);
        configuration.setSaved(false);
        startDefaultBroker(true);

        getRestTestHelper().setUsernameAndPassword(SystemConfig.MANAGEMENT_MODE_USER_NAME, MANAGEMENT_MODE_PASSWORD);

        Map<String, Object> acl = getRestTestHelper().getJsonAsSingletonList("accesscontrolprovider/" + TestBrokerConfiguration.ENTRY_NAME_ACL_FILE + "?" + RestServlet.OVERSIZE_PARAM + "=" + (file.getAbsolutePath().length()+10));
        assertEquals("Unexpected id", id.toString(), acl.get(AccessControlProvider.ID));
        assertEquals("Unexpected path", file.getAbsolutePath() , acl.get(AclFileAccessControlProvider.PATH));
        assertEquals("Unexpected state", State.ERRORED.name(), acl.get(AccessControlProvider.STATE));

        int status = getRestTestHelper().submitRequest("accesscontrolprovider/" + TestBrokerConfiguration.ENTRY_NAME_ACL_FILE, "DELETE");
        assertEquals("ACL was not deleted", 200, status);

        getRestTestHelper().submitRequest("accesscontrolprovider/" + TestBrokerConfiguration.ENTRY_NAME_ACL_FILE, "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    private void assertCanAccessManagementInterface(String accessControlProviderName, boolean canAccess) throws Exception
    {
        int expected = canAccess ? 200 : 403;
        int responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "GET");
        assertEquals("Unexpected response code", expected, responseCode);
    }

    private void assertAccessControlProviderExistence(String accessControlProviderName, boolean exists) throws Exception
    {
        String path = "accesscontrolprovider/" + accessControlProviderName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private int createAccessControlProvider(String accessControlProviderName, String content) throws Exception
    {
        File file = TestFileUtils.createTempFile(this, ".acl", content);
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.NAME, accessControlProviderName);
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);
        attributes.put(AclFileAccessControlProvider.PATH, file.getAbsoluteFile());

        return getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "PUT", attributes);
    }
}
