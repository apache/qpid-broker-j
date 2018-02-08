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
package org.apache.qpid.systest.rest;

import static javax.servlet.http.HttpServletResponse.SC_OK;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;

import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class VirtualHostRestTest extends QpidRestTestCase
{
    public static final String EMPTY_VIRTUALHOSTNODE_NAME = "emptyVHN";

    private Connection _connection;

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        createTestVirtualHostNode(getDefaultBroker(), EMPTY_VIRTUALHOSTNODE_NAME, false);
    }

    public void testAddValidAutoCreationPolicies() throws IOException
    {
        String hostToUpdate = TEST3_VIRTUALHOST;
        String restHostUrl = "virtualhost/" + hostToUpdate + "/" + hostToUpdate;

        Map<String, Object> hostDetails = getRestTestHelper().getJsonAsMap(restHostUrl);
        Asserts.assertVirtualHost(hostToUpdate, hostDetails);

        NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
            new NodeAutoCreationPolicy()
            {
                @Override
                public String getPattern()
                {
                    return "fooQ*";
                }

                @Override
                public boolean isCreatedOnPublish()
                {
                    return true;
                }

                @Override
                public boolean isCreatedOnConsume()
                {
                    return true;
                }

                @Override
                public String getNodeType()
                {
                    return "Queue";
                }

                @Override
                public Map<String, Object> getAttributes()
                {
                    return Collections.emptyMap();
                }
            },
                new NodeAutoCreationPolicy()
                {
                    @Override
                    public String getPattern()
                    {
                        return "barE*";
                    }

                    @Override
                    public boolean isCreatedOnPublish()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCreatedOnConsume()
                    {
                        return false;
                    }

                    @Override
                    public String getNodeType()
                    {
                        return "Exchange";
                    }

                    @Override
                    public Map<String, Object> getAttributes()
                    {
                        return Collections.<String, Object>singletonMap(Exchange.TYPE, "amq.fanout");
                    }
                }
        };
        Map<String, Object> newAttributes = Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES,
                                                                                     Arrays.asList(policies));
        getRestTestHelper().submitRequest(restHostUrl, "POST", newAttributes, SC_OK);
        Map<String, Object> rereadHostDetails = getRestTestHelper().getJsonAsMap(restHostUrl);

        Object retrievedPolicies = rereadHostDetails.get(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES);
        assertNotNull("Retrieved node policies are null", retrievedPolicies);
        assertTrue("Retrieved node policies are not of expected type", retrievedPolicies instanceof List);
        List retrievedPoliciesList = (List) retrievedPolicies;
        assertFalse("Retrieved node policies is empty", retrievedPoliciesList.isEmpty());
        assertEquals("Retrieved node policies list has incorrect size", 2, retrievedPoliciesList.size());
        assertTrue("First policy is not a map", retrievedPoliciesList.get(0) instanceof Map);
        assertTrue("Second policy is not a map", retrievedPoliciesList.get(1) instanceof Map);
        Map firstPolicy = (Map) retrievedPoliciesList.get(0);
        Map secondPolicy = (Map) retrievedPoliciesList.get(1);
        assertEquals("fooQ*", firstPolicy.get("pattern"));
        assertEquals("barE*", secondPolicy.get("pattern"));
        assertEquals(Boolean.TRUE, firstPolicy.get("createdOnConsume"));
        assertEquals(Boolean.FALSE, secondPolicy.get("createdOnConsume"));

    }


    public void testAddInvalidAutoCreationPolicies() throws IOException
    {

        String hostToUpdate = TEST3_VIRTUALHOST;
        String restHostUrl = "virtualhost/" + hostToUpdate + "/" + hostToUpdate;

        Map<String, Object> hostDetails = getRestTestHelper().getJsonAsMap(restHostUrl);
        Asserts.assertVirtualHost(hostToUpdate, hostDetails);

        NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
                new NodeAutoCreationPolicy()
                {
                    @Override
                    public String getPattern()
                    {
                        return null;
                    }

                    @Override
                    public boolean isCreatedOnPublish()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCreatedOnConsume()
                    {
                        return true;
                    }

                    @Override
                    public String getNodeType()
                    {
                        return "Queue";
                    }

                    @Override
                    public Map<String, Object> getAttributes()
                    {
                        return Collections.emptyMap();
                    }
                }
        };
        Map<String, Object> newAttributes = Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES,
                                                                                     Arrays.asList(policies));
        getRestTestHelper().submitRequest(restHostUrl, "POST", newAttributes, 422);

        Map<String, Object> rereadHostDetails = getRestTestHelper().getJsonAsMap(restHostUrl);

        Object retrievedPolicies = rereadHostDetails.get(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES);
        assertNotNull("Retrieved node policies are null", retrievedPolicies);
        assertTrue("Retrieved node policies are not of expected type", retrievedPolicies instanceof List);
        assertTrue("Retrieved node policies is not empty", ((List)retrievedPolicies).isEmpty());
    }


}
