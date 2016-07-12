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



import static org.apache.qpid.server.security.access.RuleOutcome.*;
import static org.apache.qpid.server.security.access.config.LegacyOperation.*;
import static org.apache.qpid.server.security.access.config.ObjectType.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHostAccessControlProvider;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.access.RuleOutcome;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.security.access.plugins.AclRule;
import org.apache.qpid.server.security.access.plugins.RuleBasedVirtualHostAccessControlProvider;
import org.apache.qpid.systest.rest.QpidRestTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class VirtualHostAccessControlProviderRestTest extends QpidRestTestCase
{
    private static final String ADMIN = "admin";

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";
    private static final String USER3 = "user3";
    private static final String USER4 = "user4";
    private static final String USER5 = "user5";
    private static final String USER6 = "user6";


    private String _queueUrl;
    private String _queueName;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _queueName = getTestName();
        _queueUrl = "queue/test/test/" + _queueName;

        getRestTestHelper().setUsernameAndPassword(ADMIN, ADMIN);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "rules");
        attributes.put(ConfiguredObject.TYPE, RuleBasedVirtualHostAccessControlProvider.RULE_BASED_TYPE);
        final AclRule[] rules = {
                new TestAclRule(USER1, ObjectType.QUEUE, CREATE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, CREATE, ALLOW_LOG),
                new TestAclRule(USER4, ObjectType.QUEUE, CREATE, ALLOW_LOG),

                new TestAclRule(USER1, ObjectType.QUEUE, UPDATE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, UPDATE, ALLOW_LOG),
                new TestAclRule(USER4, ObjectType.QUEUE, UPDATE, ALLOW_LOG),

                new TestAclRule(USER1, ObjectType.QUEUE, DELETE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, DELETE, ALLOW_LOG),
                new TestAclRule(USER4, ObjectType.QUEUE, DELETE, ALLOW_LOG),

        };
        attributes.put(RuleBasedVirtualHostAccessControlProvider.RULES, rules);
        getRestTestHelper().submitRequest(VirtualHostAccessControlProvider.class.getSimpleName().toLowerCase() + "/test/test/rules", "PUT", attributes);

    }

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile(ADMIN, USER1, USER2, USER3, USER4, USER5, USER6);
        final AclRule[] rules = {
                new TestAclRule(ADMIN, ObjectType.ALL, LegacyOperation.ALL, ALLOW_LOG),

                new TestAclRule("ALL", MANAGEMENT, ACCESS, ALLOW_LOG),
                new TestAclRule(USER1, ObjectType.QUEUE, CREATE, ALLOW_LOG),
                new TestAclRule(USER2, ObjectType.QUEUE, CREATE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, CREATE, DENY_LOG),
                new TestAclRule(USER5, ObjectType.QUEUE, CREATE, ALLOW_LOG),

                new TestAclRule(USER1, ObjectType.QUEUE, UPDATE, ALLOW_LOG),
                new TestAclRule(USER2, ObjectType.QUEUE, UPDATE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, UPDATE, DENY_LOG),
                new TestAclRule(USER5, ObjectType.QUEUE, UPDATE, ALLOW_LOG),

                new TestAclRule(USER1, ObjectType.QUEUE, DELETE, ALLOW_LOG),
                new TestAclRule(USER2, ObjectType.QUEUE, DELETE, DENY_LOG),
                new TestAclRule(USER3, ObjectType.QUEUE, DELETE, DENY_LOG),
                new TestAclRule(USER5, ObjectType.QUEUE, DELETE, ALLOW_LOG)

        };
        defaultBrokerConfiguration.addAclRuleConfiguration(rules);

    }

    public void testCreateAndDeleteQueueAllowedFromBrokerRule() throws Exception
    {
        assertCreateAndDeleteQueueSucceeds(USER5);
    }

    public void testCreateDeleteQueueAllowedFromVirtualHostRule() throws Exception
    {
        assertCreateAndDeleteQueueSucceeds(USER4);
    }

    public void testCreateDeleteQueueAllowedFromVirtualHostOverridingBrokerRule() throws Exception
    {
        assertCreateAndDeleteQueueSucceeds(USER3);
    }

    public void testCreateQueueDeniedFromVirtualHostRule() throws Exception
    {
        assertCreateQueueDenied(USER1);
    }

    public void testCreateQueueDeniedFromBrokerRule() throws Exception
    {
        assertCreateQueueDenied(USER2);
    }


    public void testCreateQueueDeniedFromDefault() throws Exception
    {
        assertCreateQueueDenied(USER6);
    }

    public void assertCreateAndDeleteQueueSucceeds(final String username) throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(username, username);

        int responseCode = createQueue();
        assertEquals("Queue creation should be allowed", 201, responseCode);

        assertQueueExists();

        responseCode = getRestTestHelper().submitRequest(_queueUrl, "DELETE");
        assertEquals("Queue deletion should be allowed", 200, responseCode);

        assertQueueDoesNotExist();
    }



    public void assertCreateQueueDenied(String username) throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(username, username);

        int responseCode = createQueue();
        assertEquals("Queue creation should be denied", 403, responseCode);

        assertQueueDoesNotExist();
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

    public static class TestAclRule implements AclRule
    {
        private String _identity;
        private ObjectType _objectType;
        private LegacyOperation _operation;
        private RuleOutcome _outcome;

        public TestAclRule(final String identity,
                           final ObjectType objectType,
                           final LegacyOperation operation,
                           final RuleOutcome outcome)
        {
            _identity = identity;
            _objectType = objectType;
            _operation = operation;
            _outcome = outcome;
        }

        @Override
        public String getIdentity()
        {
            return _identity;
        }

        @Override
        public ObjectType getObjectType()
        {
            return _objectType;
        }

        @Override
        public LegacyOperation getOperation()
        {
            return _operation;
        }

        @Override
        public Map<ObjectProperties.Property, String> getAttributes()
        {
            return Collections.emptyMap();
        }

        @Override
        public RuleOutcome getOutcome()
        {
            return _outcome;
        }
    }
}
