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
package org.apache.qpid.test.unit.message;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.qpid.test.utils.QpidBrokerTestCase;


/**
 * This test makes sure that utf8 characters can be used for
 * specifying exchange, queue name and routing key.
 *
 * those tests are related to qpid-1384
 */
public class UTF8Test extends QpidBrokerTestCase
{
    public void testPlainEn() throws Exception
    {
         invoke("UTF8En");
    }


    public void testUTF8Jp() throws Exception
    {
        invoke("UTF8Jp");
    }

    private void invoke(String name) throws Exception
    {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("org/apache/qpid/test/unit/message/" + name);

        BufferedReader in = new BufferedReader(new InputStreamReader(stream, "UTF8"));
        runTest(in.readLine(), in.readLine(), in.readLine(), in.readLine());
        in.close();
    }

    private void runTest(String exchangeName, String queueName, String routingKey, String data) throws Exception
    {
        Connection con =  getConnection();
        Session sess = con.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        final Destination receivingDestination = getReceivingDestination(exchangeName, routingKey, queueName, sess);
        final Destination sendingDestination = getSendingDestination(exchangeName, routingKey, queueName, sess);

        final MessageConsumer msgCons = sess.createConsumer(receivingDestination);
        con.start();

        // Send data
        MessageProducer msgProd = sess.createProducer(sendingDestination);
        TextMessage message = sess.createTextMessage(data);
        message.setStringProperty("stringProperty", data);
        msgProd.send(message);

        // consume data
        TextMessage m = (TextMessage) msgCons.receive(RECEIVE_TIMEOUT);
        assertNotNull(m);
        assertEquals(data, m.getText());
        assertEquals(data, message.getStringProperty("stringProperty"));
    }

    private Destination getReceivingDestination(String exch, String routkey, String qname, final Session session) throws Exception
    {
        Properties props = new Properties();
        if(isBroker10())
        {
            props.setProperty("queue.recvDest", qname);
            createTestQueue(session, qname);
        }
        else
        {
            props.setProperty("destination.recvDest",
                              "direct://" + exch + "//" + qname + "?autodelete='false'&durable='false'"
                              + "&routingkey='" + routkey + "'");
        }
        // Get our connection context
        InitialContext ctx = new InitialContext(props);
        return (Destination) ctx.lookup("recvDest");
    }

    private Destination getSendingDestination(String exch, String routkey, String qname, final Session session) throws Exception
    {
        Properties props = new Properties();
        if(isBroker10())
        {
            props.setProperty("topic.sendDest", exch +"/" + routkey);
            createEntityUsingAmqpManagement(exch, session, "org.apache.qpid.DirectExchange");
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put("queue",qname);
            arguments.put("bindingKey", routkey);
            performOperationUsingAmqpManagement(exch, "bind", session, "org.apache.qpid.DirectExchange", arguments);
        }
        else
        {
            props.setProperty("destination.sendDest",
                              "direct://" + exch + "//" + qname + "?autodelete='false'&durable='false'"
                              + "&routingkey='" + routkey + "'");
        }
        // Get our connection context
        InitialContext ctx = new InitialContext(props);
        return (Destination) ctx.lookup("sendDest");
    }
}
