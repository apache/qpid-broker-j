/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.acl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;


public class MessagingACLTest extends AbstractACLTestCase
{
    public void setUpAccessAuthorizedSuccess() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST");
    }

    public void testAccessAuthorizedSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");
        conn.close();
    }

    public void setUpAccessNoRightsFailure() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL DENY-LOG client ACCESS VIRTUALHOST");
    }

    public void testAccessNoRightsFailure() throws Exception
    {
        try
        {
            getConnection("test", "client", "guest");
            fail("Connection was created.");
        }
        catch (JMSException e)
        {
            assertAccessDeniedException(e);
        }
    }

    private void assertAccessDeniedException(JMSException e) throws Exception
    {
        assertTrue("Unexpected exception message:" + e.getMessage(),
                   e.getMessage().contains("Permission PERFORM_ACTION(connect) is denied"));
        if (isBroker10())
        {
            assertTrue("Unexpected error condition reported:" + e.getMessage(),
                       e.getMessage().contains("amqp:not-allowed"));
        }
    }

    public void setUpAccessVirtualHostWithName() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST name='test'",
                                       "ACL DENY-LOG guest ACCESS VIRTUALHOST name='test'",
                                       "ACL ALLOW-LOG server ACCESS VIRTUALHOST name='*'");
    }

    public void testAccessVirtualHostWithName() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");
        conn.close();

        try
        {
            getConnection("test", "guest", "guest");
            fail("Access should be denied");
        }
        catch (JMSException e)
        {
            assertAccessDeniedException(e);
        }

        Connection conn2 = getConnection("test", "server", "guest");
        conn2.close();
    }

    public void setUpConsumeFromTempQueueSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG client CREATE QUEUE temporary=\"true\"",
                                                           "ACL ALLOW-LOG client CONSUME QUEUE temporary=\"true\""));
        if (!isBroker10())
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testConsumeFromTempQueueSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        conn.start();

        sess.createConsumer(sess.createTemporaryQueue());
    }

    public void setUpConsumeFromTempQueueFailure() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG client CREATE QUEUE temporary=\"true\"",
                                                           "ACL DENY-LOG client CONSUME QUEUE temporary=\"true\""));
        if (!isBroker10())
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testConsumeFromTempQueueFailure() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        conn.start();

        TemporaryQueue temporaryQueue = sess.createTemporaryQueue();
        try
        {
            sess.createConsumer(temporaryQueue);
            fail("Exception is not thrown");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e,
                                              isBrokerPre010()
                                                      ? "Cannot subscribe to queue"
                                                      : "Permission CREATE is denied for : Consumer");
        }
    }

    public void  setUpConsumeOwnQueueSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG guest ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG OWNER CONSUME QUEUE",
                                                           "ACL DENY-LOG ALL CONSUME QUEUE"));

        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE temporary=\"true\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.topic\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testConsumeOwnQueueSuccess() throws Exception
    {
        final String queueName = "user1Queue";

        createQueue(queueName);

        final String queueAddress = String.format(isBroker10() ? "%s" :  "ADDR:%s; {create:never}", queueName);

        Connection queueOwnerCon = getConnectionBuilder().setUsername("admin").setPassword("admin").build();
        try
        {
            Session queueOwnerSession = queueOwnerCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = queueOwnerSession.createQueue(queueAddress);
            queueOwnerSession.createConsumer(queue).close();
        }
        finally
        {
            queueOwnerCon.close();
        }

        Connection otherUserCon = getConnectionBuilder().setUsername("guest").setPassword("guest").build();
        try
        {
            Session otherUserSession = otherUserCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                otherUserSession.createConsumer(otherUserSession.createQueue(queueAddress)).close();
                fail("Exception not thrown");
            }
            catch (JMSException e)
            {
                final String expectedMessage =
                        isBroker10()
                                ? "Permission CREATE is denied for : Consumer"
                                : "403: access refused";
                assertJMSExceptionMessageContains(e, expectedMessage);
            }
        }
        finally
        {
            otherUserCon.close();
        }
    }

    public void setUpConsumeFromTempTopicSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG client CREATE QUEUE temporary=\"true\"",
                                                           "ACL ALLOW-LOG client CONSUME QUEUE temporary=\"true\""));

        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE temporary=\"true\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.topic\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testConsumeFromTempTopicSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();

        TemporaryTopic temporaryTopic = sess.createTemporaryTopic();
        sess.createConsumer(temporaryTopic);
    }

    public void setUpConsumeFromNamedQueueValid() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG client CONSUME QUEUE name=\"example.RequestQueue\""));
        if (!isBroker10())
        {
            rules.add("ACL ALLOW-LOG client CREATE QUEUE name=\"example.RequestQueue\"");
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }


    public void testConsumeFromNamedQueueValid() throws Exception
    {
        final String testQueueName = "example.RequestQueue";
        createQueue(testQueueName);

        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        conn.start();

        Queue queue = createTestQueue(sess, testQueueName);

        sess.createConsumer(queue);
    }

    public void setUpConsumeFromNamedQueueFailure() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL DENY-LOG client CONSUME QUEUE name=\"example.RequestQueue\""));
        if (!isBroker10())
        {
            rules.add("ACL ALLOW-LOG client CREATE QUEUE name=\"example.RequestQueue\"");
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testConsumeFromNamedQueueFailure() throws Exception
    {
        String testQueueName = "example.RequestQueue";
        createQueue(testQueueName);

        Connection conn = getConnection("test", "client", "guest");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();

        Destination dest = sess.createQueue(testQueueName);

        try
        {
            sess.createConsumer(dest);

            fail("Test failed as consumer was created.");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e,
                                              isBrokerPre010()
                                                      ? "Cannot subscribe to queue"
                                                      : "Permission CREATE is denied for : Consumer");
        }
    }

    public void setUpCreateTemporaryQueueSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                                           "ACL ALLOW-LOG client CREATE QUEUE temporary=\"true\""));
        if (!isBroker10())
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\" temporary=true");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testCreateTemporaryQueueSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        sess.createTemporaryQueue();
        conn.close();
    }

    public void setUpCreateTempTopicSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST"));
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    /* For AMQP 1.0 the server causes a temporary instance of the fanout exchange to come into being.  For early AMQP
       version, there are no server side objects created as amq.topic is used.
     */
    public void testCreateTempTopicSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();

        TemporaryTopic temporaryTopic = sess.createTemporaryTopic();
        conn.close();
    }

    public void setUpCreateTemporaryQueueFailed() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                       "ACL DENY-LOG client CREATE QUEUE temporary=\"true\"");
    }

    public void testCreateTemporaryQueueFailed() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();

        try
        {

            session.createTemporaryQueue();

            fail("Test failed as creation succeeded.");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Queue");
        }
    }

    public void setUpPublishUsingTransactionSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>();
        rules.add("ACL ALLOW-LOG client ACCESS VIRTUALHOST");

        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"\" routingKey=\"example.RequestQueue\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testPublishUsingTransactionSuccess() throws Exception
    {
        String queueName = "example.RequestQueue";
        createQueue(queueName);

        bindExchangeToQueue("amq.direct", queueName);

        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

        Queue queue = sess.createQueue(queueName);

        MessageProducer sender = sess.createProducer(queue);

        sender.send(sess.createTextMessage("test"));

        //Send the message using a transaction as this will allow us to retrieve any errors that occur on the broker.
        sess.commit();

        conn.close();
    }

    public void setUpPublishToExchangeUsingTransactionSuccess() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                       "ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");
    }

    public void testPublishToExchangeUsingTransactionSuccess() throws Exception
    {
        String queueName = "example.RequestQueue";
        createQueue(queueName);
        bindExchangeToQueue("amq.direct", queueName);

        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

        Queue queue = sess.createQueue(isBroker10() ? ("amq.direct/" + queueName) : ("ADDR:amq.direct/" + queueName));

        MessageProducer sender = sess.createProducer(queue);

        sender.send(sess.createTextMessage("test"));

        //Send the message using a transaction as this will allow us to retrieve any errors that occur on the broker.
        sess.commit();

        conn.close();
    }

    public void setUpRequestResponseSuccess() throws Exception
    {
        // The group "messaging-users", referenced in the ACL below, is currently defined
        // in broker/etc/groups-systests.
        // We tolerate a dependency from this test to that file because its
        // contents are expected to change rarely.

        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG messaging-users ACCESS VIRTUALHOST",
                                                           "# Server side",
                                                           "ACL ALLOW-LOG server CONSUME QUEUE name=\"example.RequestQueue\"",
                                                           "# Client side",
                                                           "ACL ALLOW-LOG client CONSUME QUEUE temporary=true",
                                                           "ACL ALLOW-LOG client CREATE QUEUE temporary=true"));
        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG server PUBLISH EXCHANGE name=\"\" routingKey=\"TempQueue*\"");
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"\" routingKey=\"example.RequestQueue\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client BIND EXCHANGE name=\"amq.direct\" temporary=true");
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");

            rules.add("ACL ALLOW-LOG server CREATE QUEUE name=\"example.RequestQueue\"");
            rules.add("ACL ALLOW-LOG server BIND EXCHANGE");
            rules.add("ACL ALLOW-LOG server PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"TempQueue*\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }


    public void testRequestResponseSuccess() throws Exception
    {
        String queueName = "example.RequestQueue";
        createQueue(queueName);

        //Set up the Server
        Connection serverConnection = getConnection("test", "server", "guest");
        Session serverSession = serverConnection.createSession(true, Session.SESSION_TRANSACTED);
        Queue requestQueue = serverSession.createQueue(queueName);
        MessageConsumer server = serverSession.createConsumer(requestQueue);
        serverConnection.start();

        //Set up the consumer
        Connection clientConnection = getConnection("test", "client", "guest");
        Session clientSession = clientConnection.createSession(true, Session.SESSION_TRANSACTED);
        Queue responseQueue = clientSession.createTemporaryQueue();
        MessageConsumer clientResponse = clientSession.createConsumer(responseQueue);
        clientConnection.start();

        // Client
        Message request = clientSession.createTextMessage("Request");
        request.setJMSReplyTo(responseQueue);

        clientSession.createProducer(requestQueue).send(request);
        clientSession.commit();

        // Server
        Message msg = server.receive(getReceiveTimeout());
        assertNotNull("Server should have received client's request", msg);
        assertNotNull("Received msg should have Reply-To", msg.getJMSReplyTo());

        MessageProducer sender = serverSession.createProducer(msg.getJMSReplyTo());
        sender.send(serverSession.createTextMessage("Response"));
        serverSession.commit();

        // Client
        Message clientResponseMsg = clientResponse.receive(getReceiveTimeout());
        clientSession.commit();
        assertNotNull("Client did not receive response message,", clientResponseMsg);
        assertEquals("Incorrect message received", "Response", ((TextMessage) clientResponseMsg).getText());
    }

    public void setUpPublishToTempTopicSuccess() throws Exception
    {
        List<String> rules = new ArrayList<>(Arrays.asList("ACL ALLOW-LOG client ACCESS VIRTUALHOST"));

        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE temporary=\"true\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"amq.topic\"");
        }
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testPublishToTempTopicSuccess() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
        conn.start();

        TemporaryTopic temporaryTopic = sess.createTemporaryTopic();
        MessageProducer producer = sess.createProducer(temporaryTopic);
        producer.send(sess.createMessage());
        sess.commit();
    }


    public void setUpFirewallAllow() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW client ACCESS VIRTUALHOST from_network=\"127.0.0.1\"");
    }

    public void testFirewallAllow() throws Exception
    {
        getConnection("test", "client", "guest");
    }

    public void setUpFirewallDeny() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL DENY client ACCESS VIRTUALHOST from_network=\"127.0.0.1\"");
    }

    public void testFirewallDeny() throws Exception
    {
        try
        {
            getConnection("test", "client", "guest");
            fail("We expected the connection to fail");
        }
        catch (JMSException e)
        {
            // pass
        }
    }


    public void setUpPublishToDefaultExchangeSuccess() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                       "ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"\" routingKey=\"example.RequestQueue\"",
                                       "ACL DENY-LOG ALL ALL");
    }

    public void testPublishToDefaultExchangeSuccess() throws Exception
    {
        String queueName = "example.RequestQueue";
        createQueue(queueName);

        Connection conn = getConnection("test", "client", "guest");

        Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer sender = sess.createProducer(sess.createQueue("ADDR: example.RequestQueue"));

        sender.send(sess.createTextMessage("test"));

        //Send the message using a transaction as this will allow us to retrieve any errors that occur on the broker.
        sess.commit();

        conn.close();
    }


    public void setUpPublishToDefaultExchangeFailure() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                       "ACL DENY-LOG ALL ALL");
    }

    public void testPublishToDefaultExchangeFailure() throws Exception
    {
        String queueName = "example.RequestQueue";
        createQueue(queueName);

        try
        {
            Connection conn = getConnection("test", "client", "guest");
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

            MessageProducer sender = sess.createProducer(sess.createQueue("ADDR: example.RequestQueue"));

            sender.send(sess.createTextMessage("test"));

            //Send the message using a transaction as this will allow us to retrieve any errors that occur on the broker.
            sess.commit();

            fail("Sending to the anonymousExchange without permission should fail");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e, "Access denied to publish to default exchange");
        }
    }

    public void setUpAnonymousProducerFailsToSendMessageIntoDeniedDestination() throws Exception
    {
        List<String> rules = new ArrayList<>();
        rules.add("ACL ALLOW-LOG client ACCESS VIRTUALHOST");

        if (isBroker10())
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"\" routingKey=\"example.RequestQueue\"");
        }
        else
        {
            rules.add("ACL ALLOW-LOG client PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"example.RequestQueue\"");
        }
        rules.add("ACL DENY-LOG client PUBLISH EXCHANGE name=\"*\" routingKey=\"deniedQueue\"");
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testAnonymousProducerFailsToSendMessageIntoDeniedDestination() throws Exception
    {
        final String allowedDestinationName =  "example.RequestQueue";
        final String deniedDestinationName = "deniedQueue";
        createQueue(allowedDestinationName);
        createQueue(deniedDestinationName);

        if (!isBroker10())
        {
            bindExchangeToQueue("amq.direct", allowedDestinationName);
            bindExchangeToQueue("amq.direct", deniedDestinationName);
        }

        Connection connection = getConnection("test", "client", "guest");
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(null);
        producer.send(session.createQueue(allowedDestinationName), session.createTextMessage("test1"));

        try
        {
            producer.send(session.createQueue(deniedDestinationName), session.createTextMessage("test2"));

            fail("Sending should fail");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e,
                                              "Permission PERFORM_ACTION(publish) is denied for : " + (isBroker10()
                                                      ? "Queue"
                                                      : "Exchange"));
        }

        try
        {
            session.commit();
            fail("Commit should fail");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    public void setUpPublishIntoDeniedDestinationFails() throws Exception
    {
        List<String> rules = new ArrayList<>();
        rules.add("ACL ALLOW-LOG client ACCESS VIRTUALHOST");
        rules.add("ACL DENY-LOG client PUBLISH EXCHANGE name=\"*\" routingKey=\"deniedQueue\"");
        writeACLFileWithAdminSuperUser(rules.toArray(new String[rules.size()]));
    }

    public void testPublishIntoDeniedDestinationFails() throws Exception
    {
        final String deniedDestinationName = "deniedQueue";
        createQueue(deniedDestinationName);

        if (!isBroker10())
        {
            bindExchangeToQueue("amq.direct", deniedDestinationName);
        }

        try
        {
            Connection connection = getConnection("test", "client", "guest");
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            MessageProducer producer = session.createProducer(session.createQueue(deniedDestinationName));

            producer.send(session.createTextMessage("test"));

            fail("Sending should fail");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e,
                                              "Permission PERFORM_ACTION(publish) is denied for : " + (isBroker10()
                                                      ? "Queue"
                                                      : "Exchange"));
        }
    }
}
