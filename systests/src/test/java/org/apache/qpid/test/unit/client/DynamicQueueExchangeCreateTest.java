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
package org.apache.qpid.test.unit.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.AMQException;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.url.BindingURL;

public class DynamicQueueExchangeCreateTest extends QpidBrokerTestCase
{
    private static final String TEST_VHOST = TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST;
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

    /*
     * Tests to validate that setting the respective qpid.declare_queues,
     * qpid.declare_exchanges system properties functions as expected.
     */

    public void testQueueNotDeclaredDuringConsumerCreation() throws Exception
    {
        setSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");

        Connection connection = getConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestQueueName());

        try
        {
            session.createConsumer(queue);
            fail("JMSException should be thrown as the queue does not exist");
        }
        catch (JMSException e)
        {
            checkExceptionErrorCode(e, ErrorCodes.NOT_FOUND);
        }
    }

    public void testExchangeNotDeclaredDuringConsumerCreation() throws Exception
    {
        setSystemProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "false");

        Connection connection = getConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String exchangeName = getTestQueueName();
        Queue queue = session.createQueue("direct://" + exchangeName + "/queue/queue");

        try
        {
            session.createConsumer(queue);
            fail("JMSException should be thrown as the exchange does not exist");
        }
        catch (JMSException e)
        {
            checkExceptionErrorCode(e, ErrorCodes.NOT_FOUND);
        }

        assertFalse("Exchange exists", exchangeExists(exchangeName));
    }


    /**
     * Checks that setting {@value ClientProperties#QPID_DECLARE_EXCHANGES_PROP_NAME} false results in
     * disabling implicit ExchangeDeclares during producer creation when using a {@link BindingURL}
     */
    public void testExchangeNotDeclaredDuringProducerCreation() throws Exception
    {
        Connection connection = getConnection();
        Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String exchangeName1 = getTestQueueName() + "1";


        Queue queue = session1.createQueue("direct://" + exchangeName1 + "/queue/queue");
        session1.createProducer(queue);
        ((AMQSession<?,?>)session1).sync();

        assertTrue("Exchange does not exist", exchangeExists(exchangeName1));

        //Now disable the implicit exchange declares and try again
        setSystemProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "false");

        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String exchangeName2 = getTestQueueName() + "2";

        Queue queue2 = session2.createQueue("direct://" + exchangeName2 + "/queue/queue");
        session2.createProducer(queue2);
        ((AMQSession<?,?>)session2).sync();
        assertFalse("Exchange exists", exchangeExists(exchangeName2));
    }

    public void testQueueNotBoundDuringConsumerCreation() throws Exception
    {
        setSystemProperty(ClientProperties.QPID_BIND_QUEUES_PROP_NAME, "false");
        setSystemProperty(ClientProperties.VERIFY_QUEUE_ON_SEND, "true");

        Connection connection = getConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestQueueName());
        session.createConsumer(queue);

        try
        {
            session.createProducer(queue).send(session.createMessage());
            fail("JMSException should be thrown as the queue does not exist");
        }
        catch (InvalidDestinationException ide)
        {
            //PASS
        }
    }

    public void testTemporaryExchangeDeletedWhenLastBindingRemoved() throws Exception
    {

        Connection connection = getConnection();
        connection.start();
        AMQSession session = (AMQSession) connection.createSession(true, Session.SESSION_TRANSACTED);

        final String exchangeName = getTestName() + "_exch";
        final String queueName = getTestName() + "_queue";


        String tmpQueueBoundToTmpExchange = String.format("direct://%s/%s/%s?%s='%b'&%s='%b'",
                                                          exchangeName,
                                                          queueName,
                                                          queueName,
                                                          BindingURL.OPTION_AUTODELETE,
                                                          true,
                                                          BindingURL.OPTION_EXCHANGE_AUTODELETE,
                                                          true);
        Queue queue = session.createQueue(tmpQueueBoundToTmpExchange);

        MessageConsumer consumer = session.createConsumer(queue);
        assertTrue("Exchange does not exist", exchangeExists(exchangeName));

        sendMessage(session, queue, 1);

        Message message = consumer.receive(1000);
        session.commit();
        assertNotNull("Message not received", message);

        // Closing the session will cause the temporary queue to be deleted, causing the
        // binding to be deleted.  This will trigger the auto deleted exchange to be removed too
        consumer.close();

        assertFalse("Exchange " + exchangeName + " should no longer exist",
                    exchangeExists(exchangeName));
    }

    private void checkExceptionErrorCode(JMSException original, int code)
    {
        Exception linked = original.getLinkedException();
        assertNotNull("Linked exception should have been set", linked);
        assertTrue("Linked exception should be an AMQProtocolException", linked instanceof AMQException);
        assertEquals("Error code should be " + code, code, ((AMQException) linked).getErrorCode());
    }

    public void testAutoDeleteExchangeDeclarationByProducer() throws Exception
    {
        String exchangeName = createExchange(true, BindingURL.OPTION_EXCHANGE_AUTODELETE);
        verifyDeclaredExchangeAttribute(exchangeName, Exchange.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_NO_LINKS.name());
    }

    public void testDurableExchangeDeclarationByProducer() throws Exception
    {
        String exchangeName = createExchange(true, BindingURL.OPTION_EXCHANGE_DURABLE);
        verifyDeclaredExchangeAttribute(exchangeName, Exchange.DURABLE, true);
    }

    public void testAutoDeleteExchangeDeclarationByConsumer() throws Exception
    {
        String exchangeName = createExchange(false, BindingURL.OPTION_EXCHANGE_AUTODELETE);
        verifyDeclaredExchangeAttribute(exchangeName, Exchange.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_NO_LINKS.name());
    }


    public void testDurableExchangeDeclarationByConsumer() throws Exception
    {
        String exchangeName = createExchange(false, BindingURL.OPTION_EXCHANGE_DURABLE);
        verifyDeclaredExchangeAttribute(exchangeName, Exchange.DURABLE, true);
    }

    private String createExchange(final boolean createProducer,
                                  final String optionName)
            throws Exception
    {
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String queueName = getTestQueueName() + "1";
        String exchangeName = getTestQueueName() + "1";
        Queue queue = session.createQueue("direct://" + exchangeName + "/" + queueName + "/" + queueName + "?" + optionName + "='true'");
        if(createProducer)
        {
            session.createProducer(queue);
        }
        else
        {
            session.createConsumer(queue);
        }
        session.close();
        return exchangeName;
    }

    private void verifyDeclaredExchangeAttribute(String exchangeName, String attributeName, Object expectedAttributeValue) throws IOException
    {
        Map<String, Object> attributes = getExchangeAttributes(exchangeName);
        Object realAttributeValue = attributes.get(attributeName);
        assertEquals("Unexpected attribute " + attributeName + " value " + realAttributeValue, realAttributeValue, expectedAttributeValue);
    }

    private Map<String, Object> getExchangeAttributes(final String exchangeName) throws IOException
    {
        String exchangeUrl = "exchange/" + TEST_VHOST + "/" + TEST_VHOST + "/" + exchangeName;
        List<Map<String, Object>> exchanges = _restTestHelper.getJsonAsList(exchangeUrl);
        int exchangesSize = exchanges.size();
        if (exchangesSize == 1)
        {
            return exchanges.get(0);
        }
        else if (exchangesSize == 0)
        {
            return null;
        }
        else
        {
            fail("Exchange REST service returned more then 1 exchange " + exchanges);
        }
        return null;
    }

    private boolean exchangeExists(final String exchangeName)
            throws Exception
    {
        try
        {
            return getExchangeAttributes(exchangeName) != null;
        }
        catch (FileNotFoundException e)
        {
            return false;
        }
    }
}
