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

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
                "ACL ALLOW-LOG " + ALLOWED_USER + " INVOKE QUEUE method_name=\"clearQueue\"",
                "ACL ALLOW-LOG " + ALLOWED_USER + " INVOKE QUEUE method_name=\"get*\"",
                "ACL DENY-LOG ALL ALL");

    }

    public void testCreateQueueAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);

        assertQueueExists();
    }

    public void testCreateQueueDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        createQueue(SC_FORBIDDEN);

        assertQueueDoesNotExist();
    }

    public void testDeleteQueueAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);

        assertQueueExists();

        getRestTestHelper().submitRequest(_queueUrl, "DELETE", SC_OK);

        assertQueueDoesNotExist();
    }

    public void testDeleteQueueDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);

        assertQueueExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        getRestTestHelper().submitRequest(_queueUrl, "DELETE", SC_FORBIDDEN);

        assertQueueExists();
    }



    public void testSetQueueAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);

        assertQueueExists();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, _queueName);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000);

        getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes, SC_OK);

        Map<String, Object> queueData = getRestTestHelper().getJsonAsMap(_queueUrl);
        assertEquals("Unexpected " + Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000, queueData.get(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES));
    }

    public void testSetQueueAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);
        assertQueueExists();

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, _queueName);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100000);

        getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes, SC_FORBIDDEN);

        Map<String, Object> queueData = getRestTestHelper().getJsonAsMap(_queueUrl);
        assertEquals("Unexpected " + Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, -1, queueData.get(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES));
    }

    public void testInvokeQueueOperation() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        createQueue(SC_CREATED);

        getRestTestHelper().submitRequest(_queueUrl + "/clearQueue", "POST", Collections.emptyMap(), SC_OK);
        getRestTestHelper().submitRequest(_queueUrl + "/getStatistics", "POST", Collections.emptyMap(), SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        getRestTestHelper().submitRequest(_queueUrl + "/clearQueue", "POST", Collections.emptyMap(), SC_FORBIDDEN);
    }

    private void createQueue(final int expectedResponseCode) throws Exception
    {
        Map<String, Object> attributes = Collections.singletonMap(Queue.NAME, _queueName);

        getRestTestHelper().submitRequest(_queueUrl, "PUT", attributes, expectedResponseCode);
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
        int expectedResponseCode = exists ? SC_OK : SC_NOT_FOUND;
        getRestTestHelper().submitRequest(_queueUrl, "GET", expectedResponseCode);
    }
}
