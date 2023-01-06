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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

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

        assertNotNull(structure, "Structure data cannot be null");
        assertEquals("Broker", structure.get("name"), "Unexpected name");
        assertNotNull(structure.get("id"), "Unexpected id");

        List<Map<String, Object>>  virtualhostnodes = (List<Map<String, Object>>) structure.get("virtualhostnodes");
        assertNotNull(virtualhostnodes, "Virtual host nodes not present");

        Map<String, Object> testNode = assertPresent(nodeName, virtualhostnodes);

        List<Map<String, Object>>  virtualhosts = (List<Map<String, Object>>) testNode.get("virtualhosts");
        assertNotNull(virtualhosts, "Virtual host not present");
        assertEquals(1, virtualhosts.size(), String.format("VH %s not found", hostName));

        Map<String, Object> testHost = assertPresent(hostName, virtualhosts);

        List<Map<String, Object>> exchanges = (List<Map<String, Object>>) testHost.get("exchanges");
        assertNotNull(exchanges, "Exchanges not present");
        assertPresent("Exchange amq.direct is not found","amq.direct", exchanges);
        assertPresent("Exchange amq.topic is not found","amq.topic", exchanges);
        assertPresent("Exchange amq.fanout is not found","amq.fanout", exchanges);
        assertPresent("Exchange amq.match is not found","amq.match", exchanges);

        List<Map<String, Object>> queues = (List<Map<String, Object>>) testHost.get("queues");
        assertNull(queues, "Unexpected queues");

        List<Map<String, Object>> ports = (List<Map<String, Object>>) structure.get("ports");
        assertNotNull(ports, "Ports not present");
        Map<String, Object> amqpPort = assertPresent("AMQP", ports);
        assertNotNull(amqpPort, "Port AMQP is not found");
        assertPresent("Port ANONYMOUS_AMQP is not found", "ANONYMOUS_AMQP", ports);
        assertPresent("Port HTTP is not found", "HTTP", ports);

        List<Map<String, Object>> aliases = (List<Map<String, Object>>) amqpPort.get("virtualhostaliases");
        assertNotNull(aliases, "Virtual host aliases not found");
        assertPresent("Alias defaultAlias is not found","defaultAlias", aliases);
        assertPresent("Alias hostnameAlias is not found","hostnameAlias", aliases);
        assertPresent("Alias nameAlias is not found","nameAlias", aliases);

        List<Map<String, Object>> providers = (List<Map<String, Object>>) structure.get("authenticationproviders");
        assertNotNull(providers, "Authentication providers not present");
        assertPresent("Authentication provider 'anon' is not found", "anon", providers);
        assertPresent("Authentication provider 'plain' is not found", "plain", providers);

        Map<String, Object> plainProvider = assertPresent("plain", providers);
        List<Map<String, Object>> users = (List<Map<String, Object>>) plainProvider.get("users");
        assertNotNull(users, "Authentication provider users not present");
        assertPresent("User 'admin' not found","admin", users);
        assertPresent("User 'guest' not found","guest", users);

        List<Map<String, Object>> keystores = (List<Map<String, Object>>) structure.get("keystores");
        assertNotNull(keystores, "Key stores not present");
        assertPresent("systestsKeyStore not found","systestsKeyStore", keystores);

        List<Map<String, Object>> plugins = (List<Map<String, Object>>) structure.get("plugins");
        assertNotNull(plugins, "Plugins not present");
        assertPresent("httpManagement not found","httpManagement", plugins);

        String queueName = getTestName() + "Queue";
        helper.submitRequest(String.format("queue/%s/%s/%s", nodeName, hostName, queueName),
                             "PUT",
                             Collections.singletonMap("name", queueName));

        structure = helper.getJsonAsMap("/service/structure");

        virtualhostnodes = (List<Map<String, Object>>) structure.get("virtualhostnodes");
        assertNotNull(virtualhostnodes, "Virtual host nodes not present");

        testNode = assertPresent(hostName, virtualhostnodes);

        virtualhosts = (List<Map<String, Object>>) testNode.get("virtualhosts");
        assertNotNull(virtualhosts, "Virtual host not present");
        assertEquals(1, virtualhosts.size(), String.format("VH %s not found", hostName));

        testHost = assertPresent(hostName, virtualhosts);

        queues = (List<Map<String, Object>>) testHost.get("queues");
        assertNotNull(queues, "Queues not present");
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
        assertTrue(it.hasNext(), notFoundErrorMessage == null ?
                String.format("Entry with name '%s' is not found", entryName) : notFoundErrorMessage);
        Map<String, Object> data = it.next();
        assertFalse(it.hasNext(), String.format("More than one entry found with name %s", entryName));
        return data;
    }
}
