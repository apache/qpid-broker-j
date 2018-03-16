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
package org.apache.qpid.tests.http.endtoend.port;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class HttpPortTest extends HttpTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPortTest.class);

    @Test
    public void createdPortAcceptsConnections() throws Exception
    {
        final String portName = getTestName();
        final String authenticationProvider = portName + "AuthenticationProvider";
        createAnonymousAuthenticationProvider(authenticationProvider);
        createPort(portName, authenticationProvider);
        String portUrl = "port/" + portName;

        Map<String, Object> attributes = getHelper().getJsonAsMap(portUrl);
        assertTrue(attributes.containsKey("boundPort"));
        assertTrue(attributes.get("boundPort") instanceof Number);

        HttpTestHelper helper =
                new HttpTestHelper(getBrokerAdmin(), null, ((Number) attributes.get("boundPort")).intValue());

        Map<String, Object> ownAttributes = helper.getJsonAsMap(portUrl);
        assertEquals(attributes, ownAttributes);
    }

    @Test
    public void portDeletion() throws Exception
    {
        final String portName = getTestName();
        final String authenticationProvider = portName + "AuthenticationProvider";
        createAnonymousAuthenticationProvider(authenticationProvider);
        createPort(portName, authenticationProvider);
        String portUrl = "port/" + portName;

        Map<String, Object> attributes = getHelper().getJsonAsMap(portUrl);
        assertTrue(attributes.containsKey("boundPort"));
        assertTrue(attributes.get("boundPort") instanceof Number);

        HttpTestHelper helper =
                new HttpTestHelper(getBrokerAdmin(), null, ((Number) attributes.get("boundPort")).intValue());

        try
        {
            helper.submitRequest("port/" + portName, "DELETE", SC_OK);
        }
        catch (ConnectException e)
        {
            // Extra logging to investigate unexpected exception
            LOGGER.debug("Unexpected connection exception", e);
            Assert.fail("Unexpected exception " + e.getMessage());
        }
    }

    private void createPort(final String portName, final String authenticationProvider) throws IOException
    {
        final Map<String, Object> port = new HashMap<>();
        port.put(Port.NAME, portName);
        port.put(Port.AUTHENTICATION_PROVIDER, authenticationProvider);
        port.put(Port.TYPE, "HTTP");
        port.put(Port.PORT, 0);

        getHelper().submitRequest("port/" + portName, "PUT",
                                  port, SC_CREATED);
    }

    private void createAnonymousAuthenticationProvider(final String providerName) throws IOException
    {
        Map<String, Object> data = Collections.singletonMap(ConfiguredObject.TYPE,
                                                            AnonymousAuthenticationManager.PROVIDER_TYPE);
        getHelper().submitRequest("authenticationprovider/" + providerName,
                                  "PUT",
                                  data,
                                  SC_CREATED);
    }
}
