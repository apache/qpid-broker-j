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

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.security.acl.AbstractACLTestCase;
import org.apache.qpid.systest.rest.QpidRestTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class QueueRestACLTest extends QpidRestTestCase
{
    private static final String ALLOWED_USER = "user1";
    private static final String DENIED_USER = "user2";
    private String _queueUrl;
    private String _queueName;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _queueName = getTestName();
        _queueUrl = "queue/test/test/" + _queueName;
    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile(ALLOWED_USER, DENIED_USER);

        AbstractACLTestCase.writeACLFileUtil(this, "ACL ALLOW-LOG ALL ACCESS MANAGEMENT",
                "ACL ALLOW-LOG " + ALLOWED_USER + " CREATE QUEUE",
                "ACL DENY-LOG " + DENIED_USER + " CREATE QUEUE",
                "ACL ALLOW-LOG " + ALLOWED_USER + " UPDATE QUEUE",
                "ACL DENY-LOG " + DENIED_USER + " UPDATE QUEUE",
                "ACL ALLOW-LOG " + ALLOWED_USER + " DELETE QUEUE",
                "ACL DENY-LOG " + DENIED_USER + " DELETE QUEUE",
                "ACL DENY-LOG ALL ALL");

    }

    public void testCreateQueueAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createQueue();
        assertEquals("Queue creation should be allowed", 201, responseCode);

        assertQueueExists();
    }

    public void testCreateQueueDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        int responseCode = createQueue();
        assertEquals("Queue creation should be denied", 403, responseCode);

        assertQueueDoesNotExist();
    }

    public void testDeleteQueueAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createQueue();
        assertEquals("Queue creation should be allowed", 201, responseCode);

        assertQueueExists();

        responseCode = getRestTestHelper().submitRequest(_queueUrl, "DELETE");
        assertEquals("Queue deletion should be allowed", 200, responseCode);

        assertQueueDoesNotExist();
    }

    public void testDeleteQueueDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createQueue();
        assertEquals("Queue creation should be allowed", 201, responseCode);

        assertQueueExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        responseCode = getRestTestHelper().submitRequest(_queueUrl, "DELETE");
        assertEquals("Queue deletion should be denied", 403, responseCode);

        assertQueueExists();
    }



    public void testSetQueueAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createQueue();

        assertQueueExists();

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, _queueName);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000);

        getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes, HttpServletResponse.SC_OK);

        Map<String, Object> queueData = getRestTestHelper().getJsonAsMap(_queueUrl);
        assertEquals("Unexpected " + Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000, queueData.get(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES));
    }

    public void testSetQueueAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = createQueue();
        assertQueueExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, _queueName);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000);

        getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes, HttpServletResponse.SC_FORBIDDEN);

        Map<String, Object> queueData = getRestTestHelper().getJsonAsMap(_queueUrl);
        assertEquals("Unexpected " + Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, -1, queueData.get(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES));
    }

    private int createQueue() throws Exception
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.NAME, _queueName);

        return getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes);
    }

    private void assertQueueDoesNotExist() throws Exception
    {
        assertQueueExistence(false);
    }

    private void assertQueueExists() throws Exception
    {
        assertQueueExistence(true);
    }

    private void assertQueueExistence(boolean exists) throws Exception
    {
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(_queueUrl, "GET", expectedResponseCode);
    }
}
