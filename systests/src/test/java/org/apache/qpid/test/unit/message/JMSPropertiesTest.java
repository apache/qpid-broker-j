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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.client.message.QpidMessageProperties;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * JMS AMQP 0-x specific tests
 */
public class JMSPropertiesTest extends QpidBrokerTestCase
{

    /**
     * Test Goal : Test if custom message properties can be set and retrieved properly with out an error.
     *             Also test if unsupported properties are filtered out. See QPID-2930.
     */
    public void testQpidExtensionProperties() throws Exception
    {
        Connection con = getConnection();
        Session ssn = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        con.start();

        Topic topic = createTopic(con, "test");
        MessageConsumer consumer = ssn.createConsumer(topic);
        MessageProducer prod = ssn.createProducer(topic);
        Message m = ssn.createMessage();
        m.setObjectProperty("foo-bar", "foobar".getBytes());
        m.setObjectProperty(QpidMessageProperties.AMQP_0_10_APP_ID, "my-app-id");
        prod.send(m);

        Message msg = consumer.receive(getReceiveTimeout());
        assertNotNull(msg);

    	Enumeration<String> enu = msg.getPropertyNames();
    	Map<String,String> map = new HashMap<String,String>();
    	while (enu.hasMoreElements())
    	{
    	    String name = enu.nextElement();
    	    String value = msg.getStringProperty(name);
    		map.put(name, value);
       }

       assertFalse("Property 'foo-bar' should have been filtered out",map.containsKey("foo-bar"));
       assertEquals("Property "+ QpidMessageProperties.AMQP_0_10_APP_ID + " should be present","my-app-id",msg.getStringProperty(QpidMessageProperties.AMQP_0_10_APP_ID));
       assertEquals("Property "+ QpidMessageProperties.AMQP_0_10_ROUTING_KEY + " should be present","test",msg.getStringProperty(QpidMessageProperties.AMQP_0_10_ROUTING_KEY));
       
    }
}
