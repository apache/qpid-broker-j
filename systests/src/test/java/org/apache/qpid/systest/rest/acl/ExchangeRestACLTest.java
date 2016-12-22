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
package org.apache.qpid.systest.rest.acl;

import static org.apache.qpid.server.security.acl.AbstractACLTestCase.writeACLFileUtil;
import static org.apache.qpid.test.utils.TestBrokerConfiguration.ENTRY_NAME_ACL_FILE;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.systest.rest.QpidRestTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class ExchangeRestACLTest extends QpidRestTestCase
{
    private static final String ALLOWED_USER = "user1";
    private static final String DENIED_USER = "user2";
    private static final String ADMIN = "ADMIN";

    private String _queueName;
    private String _exchangeName;
    private String _exchangeUrl;
    private String _aclFilePath;

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile(ALLOWED_USER, DENIED_USER, ADMIN);

        _aclFilePath = writeACLFileUtil(this,
                                        "ACL ALLOW-LOG ALL ACCESS MANAGEMENT",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " CREATE QUEUE",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " CREATE EXCHANGE",
                                        "ACL DENY-LOG " + DENIED_USER + " CREATE EXCHANGE",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " UPDATE EXCHANGE",
                                        "ACL DENY-LOG " + DENIED_USER + " UPDATE EXCHANGE",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " DELETE EXCHANGE",
                                        "ACL DENY-LOG " + DENIED_USER + " DELETE EXCHANGE",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " BIND EXCHANGE",
                                        "ACL DENY-LOG " + DENIED_USER + " BIND EXCHANGE",
                                        "ACL ALLOW-LOG " + ALLOWED_USER + " UNBIND EXCHANGE",
                                        "ACL DENY-LOG " + DENIED_USER + " UNBIND EXCHANGE",
                                        "ACL ALLOW-LOG " + ADMIN + " ALL ALL",
                                        "ACL DENY-LOG ALL ALL");
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _queueName = getTestQueueName();
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        Map<String, Object> queueData = new HashMap<String, Object>();
        queueData.put(Queue.NAME, _queueName);
        queueData.put(Queue.DURABLE, Boolean.TRUE);
        int status = getRestTestHelper().submitRequest("queue/test/test/" + _queueName, "PUT", queueData);
        assertEquals("Unexpected status", 201, status);

        _exchangeName = getTestName();
        _exchangeUrl = "exchange/test/test/" + _exchangeName;
    }

    public void testCreateExchangeAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createExchange();
        assertEquals("Exchange creation should be allowed", 201, responseCode);

        assertExchangeExists();
    }

    public void testCreateExchangeDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        int responseCode = createExchange();
        assertEquals("Exchange creation should be denied", 403, responseCode);

        assertExchangeDoesNotExist();
    }


    public void testCreateExchangeAllowedAfterReload() throws Exception
    {


        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        int responseCode = createExchange();
        assertEquals("Exchange creation should be denied", 403, responseCode);

        assertExchangeDoesNotExist();

        overwriteAclFile("ACL ALLOW-LOG ALL ACCESS MANAGEMENT",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " CREATE QUEUE",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " CREATE EXCHANGE",
                         "ACL ALLOW-LOG " + DENIED_USER + " CREATE EXCHANGE",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " UPDATE EXCHANGE",
                         "ACL DENY-LOG " + DENIED_USER + " UPDATE EXCHANGE",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " DELETE EXCHANGE",
                         "ACL DENY-LOG " + DENIED_USER + " DELETE EXCHANGE",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " BIND EXCHANGE",
                         "ACL DENY-LOG " + DENIED_USER + " BIND EXCHANGE",
                         "ACL ALLOW-LOG " + ALLOWED_USER + " UNBIND EXCHANGE",
                         "ACL DENY-LOG " + DENIED_USER + " UNBIND EXCHANGE",
                         "ACL ALLOW-LOG " + ADMIN + " ALL ALL",
                         "ACL DENY-LOG ALL ALL");
        getRestTestHelper().setUsernameAndPassword(ADMIN, ADMIN);
        getRestTestHelper().submitRequest("/api/latest/"
                                          + AccessControlProvider.class.getSimpleName().toLowerCase()
                                          + "/" + ENTRY_NAME_ACL_FILE + "/reload", "POST", Collections.emptyMap(), 200);
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        responseCode = createExchange();
        assertEquals("Exchange creation should be allowed", 201, responseCode);

        assertExchangeExists();

    }

    private void overwriteAclFile(final String... rules) throws IOException
    {
        try(FileWriter fw = new FileWriter(_aclFilePath); PrintWriter out = new PrintWriter(fw))
        {
            for(String line :rules)
            {
                out.println(line);
            }
        }
    }

    public void testDeleteExchangeAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createExchange();
        assertEquals("Exchange creation should be allowed", 201, responseCode);

        assertExchangeExists();


        responseCode = getRestTestHelper().submitRequest(_exchangeUrl, "DELETE");
        assertEquals("Exchange deletion should be allowed", 200, responseCode);

        assertExchangeDoesNotExist();
    }

    public void testDeleteExchangeDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createExchange();
        assertEquals("Exchange creation should be allowed", 201, responseCode);

        assertExchangeExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        responseCode = getRestTestHelper().submitRequest(_exchangeUrl, "DELETE");
        assertEquals("Exchange deletion should be denied", 403, responseCode);

        assertExchangeExists();
    }

    public void testSetExchangeAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createExchange();

        assertExchangeExists();

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Exchange.NAME, _exchangeName);
        attributes.put(Exchange.ALTERNATE_EXCHANGE, "my-alternate-exchange");

        responseCode = getRestTestHelper().submitRequest(_exchangeUrl, "PUT", attributes);
        assertEquals("Exchange 'my-alternate-exchange' does not exist", AbstractServlet.SC_UNPROCESSABLE_ENTITY, responseCode);
    }

    public void testSetExchangeAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createExchange();
        assertExchangeExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Exchange.NAME, _exchangeName);
        attributes.put(Exchange.ALTERNATE_EXCHANGE, "my-alternate-exchange");

        responseCode = getRestTestHelper().submitRequest(_exchangeUrl, "PUT", attributes);
        assertEquals("Setting of exchange attribites should be allowed", 403, responseCode);
    }

    public void testBindToExchangeAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String bindingName = getTestName();
        int responseCode = createBinding(bindingName);
        assertEquals("Binding creation should be allowed", 200, responseCode);

        assertBindingExists(bindingName);
    }

    public void testBindToExchangeDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String bindingName = getTestName();
        int responseCode = createBinding(bindingName);
        assertEquals("Binding creation should be denied", 403, responseCode);

        assertBindingDoesNotExist(bindingName);
    }

    private int createExchange() throws Exception
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Exchange.NAME, _exchangeName);
        attributes.put(Exchange.TYPE, "direct");
        return getRestTestHelper().submitRequest(_exchangeUrl, "PUT", attributes);
    }

    private void assertExchangeDoesNotExist() throws Exception
    {
        assertExchangeExistence(false);
    }

    private void assertExchangeExists() throws Exception
    {
        assertExchangeExistence(true);
    }

    private void assertExchangeExistence(boolean exists) throws Exception
    {
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(_exchangeUrl, "GET", expectedResponseCode);
    }

    private int createBinding(String bindingName) throws IOException
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("bindingKey", bindingName);
        attributes.put("destination", _queueName);

        int responseCode = getRestTestHelper().submitRequest("exchange/test/test/amq.direct/bind", "POST", attributes);
        return responseCode;
    }

    private void assertBindingDoesNotExist(String bindingName) throws Exception
    {
        assertBindingExistence(bindingName, false);
    }

    private void assertBindingExists(String bindingName) throws Exception
    {
        assertBindingExistence(bindingName, true);
    }

    private void assertBindingExistence(String bindingName, boolean exists) throws Exception
    {
        String path = "exchange/test/test/amq.direct";

        final Map<String, Object> exch = getRestTestHelper().getJsonAsSingletonList(path);
        final Collection<Map<String, Object>> bindings = (Collection<Map<String, Object>>) exch.get("bindings");
        boolean found = false;
        if (bindings != null)
        {
            for(Map<String, Object> binding : bindings)
            {
                if (bindingName.equals(binding.get("bindingKey")))
                {
                    found = true;
                    break;
                }
            }
        }

        assertEquals(exists, found);
    }
}
