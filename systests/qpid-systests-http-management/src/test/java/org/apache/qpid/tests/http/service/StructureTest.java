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

package org.apache.qpid.tests.http.service;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class StructureTest extends HttpTestBase
{
    @SuppressWarnings("unchecked")
    @Test
    public void testGet() throws Exception
    {
        String nodeName = getVirtualHostNode();
        String hostName = getVirtualHost();
        HttpTestHelper helper = getHelper();
        Map<String, Object> structure = helper.getJsonAsMap("/service/structure");

        assertNotNull("Structure data cannot be null", structure);
        assertEquals("Unexpected name", "Broker", structure.get("name"));
        assertNotNull("Unexpected id", structure.get("id"));

        List<Map<String, Object>>  virtualhostnodes = (List<Map<String, Object>>) structure.get("virtualhostnodes");
        assertNotNull("Virtual host nodes not present", virtualhostnodes);

        Map<String, Object> testNode = assertPresent(nodeName, virtualhostnodes);

        List<Map<String, Object>>  virtualhosts = (List<Map<String, Object>>) testNode.get("virtualhosts");
        assertNotNull("Virtual host not present", virtualhosts);
        assertEquals(String.format("VH %s not found", hostName), 1, virtualhosts.size());

        Map<String, Object> testHost = assertPresent(hostName, virtualhosts);

        List<Map<String, Object>> exchanges = (List<Map<String, Object>>) testHost.get("exchanges");
        assertNotNull("Exchanges not present", exchanges);
        assertPresent("Exchange amq.direct is not found","amq.direct", exchanges);
        assertPresent("Exchange amq.topic is not found","amq.topic", exchanges);
        assertPresent("Exchange amq.fanout is not found","amq.fanout", exchanges);
        assertPresent("Exchange amq.match is not found","amq.match", exchanges);

        List<Map<String, Object>> queues = (List<Map<String, Object>>) testHost.get("queues");
        assertNull("Unexpected queues", queues);

        List<Map<String, Object>> ports = (List<Map<String, Object>>) structure.get("ports");
        assertNotNull("Ports not present", ports);
        Map<String, Object> amqpPort = assertPresent("AMQP", ports);
        assertNotNull("Port AMQP is not found", amqpPort);
        assertPresent("Port ANONYMOUS_AMQP is not found", "ANONYMOUS_AMQP", ports);
        assertPresent("Port HTTP is not found", "HTTP", ports);

        List<Map<String, Object>> aliases = (List<Map<String, Object>>) amqpPort.get("virtualhostaliases");
        assertNotNull("Virtual host aliases not found", aliases);
        assertPresent("Alias defaultAlias is not found","defaultAlias", aliases);
        assertPresent("Alias hostnameAlias is not found","hostnameAlias", aliases);
        assertPresent("Alias nameAlias is not found","nameAlias", aliases);

        List<Map<String, Object>> providers = (List<Map<String, Object>>) structure.get("authenticationproviders");
        assertNotNull("Authentication providers not present", providers);
        assertPresent("Authentication provider 'anon' is not found", "anon", providers);
        assertPresent("Authentication provider 'plain' is not found", "plain", providers);

        Map<String, Object> plainProvider = assertPresent("plain", providers);
        List<Map<String, Object>> users = (List<Map<String, Object>>) plainProvider.get("users");
        assertNotNull("Authentication provider users not present", users);
        assertPresent("User 'admin' not found","admin", users);
        assertPresent("User 'guest' not found","guest", users);

        List<Map<String, Object>> keystores = (List<Map<String, Object>>) structure.get("keystores");
        assertNotNull("Key stores not present", keystores);
        assertPresent("systestsKeyStore not found","systestsKeyStore", keystores);

        List<Map<String, Object>> plugins = (List<Map<String, Object>>) structure.get("plugins");
        assertNotNull("Plugins not present", plugins);
        assertPresent("httpManagement not found","httpManagement", plugins);

        String queueName = getTestName() + "Queue";
        helper.submitRequest(String.format("queue/%s/%s/%s", nodeName, hostName, queueName),
                             "PUT",
                             Collections.singletonMap("name", queueName));

        structure = helper.getJsonAsMap("/service/structure");

        virtualhostnodes = (List<Map<String, Object>>) structure.get("virtualhostnodes");
        assertNotNull("Virtual host nodes not present", virtualhostnodes);

        testNode = assertPresent(hostName, virtualhostnodes);

        virtualhosts = (List<Map<String, Object>>) testNode.get("virtualhosts");
        assertNotNull("Virtual host not present", virtualhosts);
        assertEquals(String.format("VH %s not found", hostName), 1, virtualhosts.size());

        testHost = assertPresent(hostName, virtualhosts);

        queues = (List<Map<String, Object>>) testHost.get("queues");
        assertNotNull("Queues not present", queues);
        assertPresent(String.format("Queue %s not found", queueName), queueName, queues);
    }

    private Map<String, Object> assertPresent(final String entryName,
                                              final List<Map<String, Object>> entries)
    {
        return assertPresent(null, entryName, entries);
    }

    private Map<String, Object> assertPresent(final String notFoundErrorMessage,
                                              final String entryName,
                                              final List<Map<String, Object>> entries)
    {
        Iterator<Map<String, Object>> it = entries.stream().filter(e -> entryName.equals(e.get("name"))).iterator();
        assertTrue(notFoundErrorMessage == null ? String.format("Entry with name '%s' is not found", entryName) : notFoundErrorMessage, it.hasNext());
        Map<String, Object> data = it.next();
        assertFalse(String.format("More than one entry found with name %s", entryName), it.hasNext());
        return data;
    }
}
