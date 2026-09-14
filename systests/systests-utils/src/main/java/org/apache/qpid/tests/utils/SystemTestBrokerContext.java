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

import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public final class SystemTestBrokerContext
{
    public static final String DEFAULT_PORT_AMQP_THREAD_POOL_SIZE = "4";
    public static final String DEFAULT_PORT_AMQP_NUMBER_OF_SELECTORS = "1";
    public static final String DEFAULT_PORT_HTTP_THREAD_POOL_MINIMUM = "4";
    public static final String DEFAULT_PORT_HTTP_THREAD_POOL_MAXIMUM = "8";
    public static final String DEFAULT_PORT_HTTP_NUMBER_OF_SELECTORS = "2";
    public static final String DEFAULT_PORT_HTTP_NUMBER_OF_ACCEPTORS = "1";
    public static final String DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE = "4";
    public static final String DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS = "1";

    private static final Map<String, String> RESOURCE_DEFAULTS = Map.of(
            AmqpPort.PORT_AMQP_THREAD_POOL_SIZE, DEFAULT_PORT_AMQP_THREAD_POOL_SIZE,
            AmqpPort.PORT_AMQP_NUMBER_OF_SELECTORS, DEFAULT_PORT_AMQP_NUMBER_OF_SELECTORS,
            HttpPort.PORT_HTTP_THREAD_POOL_MINIMUM, DEFAULT_PORT_HTTP_THREAD_POOL_MINIMUM,
            HttpPort.PORT_HTTP_THREAD_POOL_MAXIMUM, DEFAULT_PORT_HTTP_THREAD_POOL_MAXIMUM,
            HttpPort.PORT_HTTP_NUMBER_OF_SELECTORS, DEFAULT_PORT_HTTP_NUMBER_OF_SELECTORS,
            HttpPort.PORT_HTTP_NUMBER_OF_ACCEPTORS, DEFAULT_PORT_HTTP_NUMBER_OF_ACCEPTORS,
            QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE,
            DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE,
            QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS,
            DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS);

    private static final List<String> BROKER_SYSTEM_PROPERTY_PREFIXES = List.of("qpid.", "virtualhost.", "port.http.");

    private SystemTestBrokerContext()
    {
        // utility class has private constructor
    }

    public static void applyResourceDefaults(final Map<String, String> context)
    {
        RESOURCE_DEFAULTS.forEach(context::putIfAbsent);
    }

    public static void copyBrokerSystemProperties(final Map<String, String> context)
    {
        System.getProperties().stringPropertyNames().stream()
              .filter(SystemTestBrokerContext::isBrokerSystemProperty)
              .forEach(name -> context.put(name, System.getProperty(name)));
    }

    private static boolean isBrokerSystemProperty(final String name)
    {
        return BROKER_SYSTEM_PROPERTY_PREFIXES.stream().anyMatch(name::startsWith);
    }
}
