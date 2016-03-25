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

package org.apache.qpid.example;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.configuration.ClientProperties;


public class Hello 
{

    public Hello() 
    {
    }

    public static void main(String[] args)
    {
        System.setProperty(ClientProperties.DEST_SYNTAX, AMQDestination.DestSyntax.BURL.name());
        System.setProperty(ClientProperties.AMQP_VERSION, "0-9");
        Hello hello = new Hello();
        for(int i =0; i<100;i++)
        {

            hello.runTest("test-" + i);
        }
    }

    private void runTest(String queueName)
    {
        try (InputStream resourceAsStream = this.getClass().getResourceAsStream("hello.properties"))
        {
            Properties properties = new Properties();
            //properties.load(resourceAsStream);
            properties.put("java.naming.factory.initial", "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
            properties.put("connectionfactory.qpidConnectionfactory", "amqp://guest:guest@clientid/?brokerlist='tcp://localhost:5672'");
            Context context = new InitialContext(properties);

            ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);

            MessageProducer messageProducer = session.createProducer(destination);
            MessageConsumer messageConsumer = session.createConsumer(destination);

            Random rn = new Random();
            for (int i=0;i< rn.nextInt(10);i++)
            {
                TextMessage message = session.createTextMessage("Hello world! from " + queueName);
                messageProducer.send(message);
            }

            //message = (TextMessage)messageConsumer.receive();
           // System.out.println(message.getText());

            connection.close();
            context.close();
        }
        catch (Exception exp) 
        {
            exp.printStackTrace();
        }
    }
}
