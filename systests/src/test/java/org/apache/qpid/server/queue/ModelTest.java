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
package org.apache.qpid.server.queue;

import java.io.IOException;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

/**
 * This Test validates the Queue Model on the broker.
 * Currently it has some basic queue creation / deletion tests.
 * However, it should be expanded to include other tests that relate to the
 * model. i.e.
 *
 * The Create and Delete tests should ensure that the requisite logging is
 * performed.
 *
 * Additions to this suite would be to complete testing of creations, validating
 * fields such as owner/exclusive, autodelete and priority are correctly set.
 *
 * Currently this test uses the REST interface to validate that the queue has
 * been declared as expected so these tests cannot run against a CPP broker.
 *
 *
 * Tests should ensure that they clean up after themselves.
 * e,g. Durable queue creation test should perform a queue delete.
 */
public class ModelTest extends QpidBrokerTestCase
{
    private RestTestHelper _restTestHelper;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            _restTestHelper.tearDown();
        }
        finally
        {
            super.tearDown();
        }
    }

    /**
     * Test that an exclusive transient queue can be created via AMQP.
     *
     * @throws Exception On unexpected error
     */
    public void testExclusiveQueueCreationTransientViaAMQP() throws Exception
    {
        Connection connection = getConnection();

        String queueName = getTestQueueName();
        boolean durable = false;
        boolean autoDelete = false;
        boolean exclusive = true;

        createViaAMQPandValidateViaREST(connection, queueName, durable,
                                        autoDelete, exclusive);
    }



    /**
     * Test that a transient queue can be created via AMQP.
     *
     * @throws Exception On unexpected error
     */
    public void testQueueCreationTransientViaAMQP() throws Exception
    {
        Connection connection = getConnection();

        String queueName = getTestQueueName();
        boolean durable = false;
        boolean autoDelete = false;
        boolean exclusive = true;

        createViaAMQPandValidateViaREST(connection, queueName, durable,
                                        autoDelete, exclusive);
    }

    /**
     * Test that a durable exclusive queue can be created via AMQP.
     *
     * @throws Exception On unexpected error
     */

    public void testExclusiveQueueCreationDurableViaAMQP() throws Exception
    {
        Connection connection = getConnection();

        String queueName = getTestQueueName();
        boolean durable = true;
        boolean autoDelete = false;
        boolean exclusive = true;

        createViaAMQPandValidateViaREST(connection, queueName, durable,
                                        autoDelete, exclusive);

        // Clean up
        String queueUrl = getQueueUrl(queueName);
        _restTestHelper.submitRequest(queueUrl, "DELETE");
    }

    /**
     * Test that a durable queue can be created via AMQP.
     *
     * @throws Exception On unexpected error
     */

    public void testQueueCreationDurableViaAMQP() throws Exception
    {
        Connection connection = getConnection();

        String queueName = getTestQueueName();
        boolean durable = true;
        boolean autoDelete = false;
        boolean exclusive = false;

        createViaAMQPandValidateViaREST(connection, queueName, durable,
                                        autoDelete, exclusive);

        // Clean up
        String queueUrl = getQueueUrl(queueName);
        _restTestHelper.submitRequest(queueUrl, "DELETE");
    }


    private void createViaAMQPandValidateViaREST(Connection connection,
                                                 String queueName,
                                                 boolean durable,
                                                 boolean autoDelete,
                                                 boolean exclusive)
            throws Exception
    {
        AMQSession session = (AMQSession) connection.createSession(false,
                                                                   Session.AUTO_ACKNOWLEDGE);

        session.createQueue(queueName,
                            autoDelete, durable, exclusive);

        String owner = (exclusive && durable && !isBroker010()) ? connection.getClientID() : null;
        boolean isAutoDelete = autoDelete || (exclusive && !isBroker010() && !durable);
        validateQueueViaREST(queueName, owner, durable, isAutoDelete);
    }

    private void validateQueueViaREST(String queueName, String owner, boolean durable, boolean autoDelete)
            throws Exception
    {
        Map<String, Object> queueAttributes = getQueueAttributes(queueName);

        assertEquals(queueName, (String) queueAttributes.get(Queue.NAME));
        assertEquals(owner, (String) queueAttributes.get(Queue.OWNER));
        assertEquals(durable, (boolean) queueAttributes.get(Queue.DURABLE));
        LifetimePolicy lifeTimePolicy = LifetimePolicy.valueOf((String)queueAttributes.get(Queue.LIFETIME_POLICY));
        assertEquals("Unexpected life time policy " + lifeTimePolicy, autoDelete, lifeTimePolicy != LifetimePolicy.PERMANENT);
    }

    private Map<String, Object> getQueueAttributes(final String queueName) throws IOException
    {
        String queueUrl = getQueueUrl(queueName);
        return _restTestHelper.getJsonAsMap(queueUrl);
    }

    private String getQueueUrl(final String queueName)
    {
        return String.format("queue/%1$s/%1$s/%2$s",
                             TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST,
                             queueName);
    }
}
