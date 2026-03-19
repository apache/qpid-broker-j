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

package org.apache.qpid.tests.http.security;

import static jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(
        name = "qpid.initialConfigurationLocation",
        value = "classpath:config-http-management-tests-system-privilege-guard.json",
        jvm = true
)
@HttpRequestConfig
public class SystemPrivilegeEscalationGuardHttpTest extends HttpTestBase
{
    private static final String QUEUE_1 = "queue1";
    private static final String QUEUE_2 = "queue2";

    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_1);
        getBrokerAdmin().createQueue(QUEUE_2);
    }

    @Test
    public void guestCannotDeleteQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin sees queue exists
        getHelper().submitRequest("queue/" + QUEUE_1, "GET", SC_OK);

        // Guest tries to delete
        HttpTestHelper guest = new HttpTestHelper(getBrokerAdmin(), getVirtualHost());
        guest.setUserName("guest");
        guest.setPassword("guest");

        int rc = guest.submitRequest("queue/" + QUEUE_1, "DELETE");
        assertEquals(SC_FORBIDDEN, rc, "Guest must not be able to delete queue");

        // Queue must still exist (admin checks)
        getHelper().submitRequest("queue/" + QUEUE_1, "GET", SC_OK);
    }

    @Test
    public void guestCannotBulkDeleteQueues() throws Exception
    {
        // Guest tries to delete all queues
        HttpTestHelper guest = new HttpTestHelper(getBrokerAdmin(), getVirtualHost());
        guest.setUserName("guest");
        guest.setPassword("guest");

        int rc = guest.submitRequest("queue/", "DELETE");
        assertEquals(SC_FORBIDDEN, rc, "Guest must not be able to bulk delete queues");

        // Both queues must still exist
        getHelper().submitRequest("queue/" + QUEUE_1, "GET", SC_OK);
        getHelper().submitRequest("queue/" + QUEUE_2, "GET", SC_OK);
    }

    @Test
    public void adminCanDeleteQueue() throws Exception
    {
        HttpTestHelper admin = new HttpTestHelper(getBrokerAdmin(), getVirtualHost());
        admin.setUserName("admin");
        admin.setPassword("admin");

        // Sanity: our ACL still allows admin via ALLOW ALL rule
        admin.submitRequest("queue/" + QUEUE_1, "DELETE", SC_OK);
        admin.submitRequest("queue/" + QUEUE_1, "GET", SC_NOT_FOUND);
    }
}
