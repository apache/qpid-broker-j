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

package org.apache.qpid.tests.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class SystemTestBrokerContextTest extends UnitTestBase
{
    @Test
    public void resourceDefaultsAreAppliedWhenAbsent()
    {
        final Map<String, String> context = new HashMap<>();

        SystemTestBrokerContext.applyResourceDefaults(context);

        assertEquals(Map.of(
                AmqpPort.PORT_AMQP_THREAD_POOL_SIZE, SystemTestBrokerContext.DEFAULT_PORT_AMQP_THREAD_POOL_SIZE,
                AmqpPort.PORT_AMQP_NUMBER_OF_SELECTORS, SystemTestBrokerContext.DEFAULT_PORT_AMQP_NUMBER_OF_SELECTORS,
                HttpPort.PORT_HTTP_THREAD_POOL_MINIMUM, SystemTestBrokerContext.DEFAULT_PORT_HTTP_THREAD_POOL_MINIMUM,
                HttpPort.PORT_HTTP_THREAD_POOL_MAXIMUM, SystemTestBrokerContext.DEFAULT_PORT_HTTP_THREAD_POOL_MAXIMUM,
                HttpPort.PORT_HTTP_NUMBER_OF_SELECTORS, SystemTestBrokerContext.DEFAULT_PORT_HTTP_NUMBER_OF_SELECTORS,
                HttpPort.PORT_HTTP_NUMBER_OF_ACCEPTORS, SystemTestBrokerContext.DEFAULT_PORT_HTTP_NUMBER_OF_ACCEPTORS,
                QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE,
                SystemTestBrokerContext.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE,
                QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS,
                SystemTestBrokerContext.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS), context);
    }

    @Test
    public void systemPropertiesOverrideResourceDefaults()
    {
        setTestSystemProperty(AmqpPort.PORT_AMQP_THREAD_POOL_SIZE, "17");
        setTestSystemProperty(QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE, "19");
        setTestSystemProperty(HttpPort.PORT_HTTP_THREAD_POOL_MAXIMUM, "23");
        final Map<String, String> context = new HashMap<>();

        SystemTestBrokerContext.applyResourceDefaults(context);
        SystemTestBrokerContext.copyBrokerSystemProperties(context);

        assertEquals("17", context.get(AmqpPort.PORT_AMQP_THREAD_POOL_SIZE));
        assertEquals("19", context.get(QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE));
        assertEquals("23", context.get(HttpPort.PORT_HTTP_THREAD_POOL_MAXIMUM));
    }

    @Test
    public void brokerSystemPropertyNamespacesAreCopied()
    {
        setTestSystemProperty("qpid.test.context", "qpid-value");
        setTestSystemProperty("virtualhost.test.context", "virtualhost-value");
        setTestSystemProperty("port.http.test.context", "http-value");
        final Map<String, String> context = new HashMap<>();

        SystemTestBrokerContext.copyBrokerSystemProperties(context);

        assertEquals("qpid-value", context.get("qpid.test.context"));
        assertEquals("virtualhost-value", context.get("virtualhost.test.context"));
        assertEquals("http-value", context.get("port.http.test.context"));
    }

    @Test
    public void unrelatedSystemPropertiesAreNotCopied()
    {
        setTestSystemProperty("broker.version", "excluded");
        setTestSystemProperty("virtualhostnode.test.context", "excluded");
        final Map<String, String> context = new HashMap<>();

        SystemTestBrokerContext.copyBrokerSystemProperties(context);

        assertFalse(context.containsKey("java.version"));
        assertFalse(context.containsKey("broker.version"));
        assertFalse(context.containsKey("virtualhostnode.test.context"));
    }
}
