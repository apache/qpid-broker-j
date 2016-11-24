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
 *
 *
 */
package org.apache.qpid.jndi;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.ConfigurationException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.test.utils.QpidTestCase;

public class PropertiesFileInitialContextFactoryTest extends QpidTestCase
{
    private static final String CONNECTION_URL = "amqp://username:password@clientid/test?brokerlist='tcp://testContextFromProviderURL:5672'";

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        setTestSystemProperty("java.naming.provider.url", null);
    }

    public void testQueueNamesWithTrailingSpaces() throws Exception
    {
        Context ctx = prepareContext();
        Queue queue = (Queue)ctx.lookup("QueueNameWithSpace");
        assertEquals("QueueNameWithSpace",queue.getQueueName());
    }

    public void testTopicNamesWithTrailingSpaces() throws Exception
    {
        Context ctx = prepareContext();
        Topic topic = (Topic)ctx.lookup("TopicNameWithSpace");
        assertEquals("TopicNameWithSpace",topic.getTopicName());
    }

    public void testMultipleTopicNamesWithTrailingSpaces() throws Exception
    {
        Context ctx = prepareContext();
        Topic topic = (Topic)ctx.lookup("MultipleTopicNamesWithSpace");
        int i = 0;
        for (String bindingKey: ((AMQDestination)topic).getBindingKeys())
        {
            i++;
            assertEquals("Topic" + i + "WithSpace", bindingKey);
        }
    }

    public void testConfigurationErrors() throws Exception
    {
        Properties properties = new Properties();
        properties.put("java.naming.factory.initial", "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        properties.put("destination.my-queue","amq.topic/test;create:always}");

        try
        {
            new InitialContext(properties);
            fail("A configuration exception should be thrown with details about the address syntax error");
        }
        catch(ConfigurationException e)
        {
            assertTrue("Incorrect exception", e.getMessage().contains("Failed to parse entry: amq.topic/test;create:always}"));
        }
    }

    private InitialContext prepareContext() throws IOException, NamingException
    {
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("JNDITest.properties"));

        return new InitialContext(properties);
    }

    /**
     * Test loading of a JNDI properties file through use of a file:// URL
     * supplied via the InitialContext.PROVIDER_URL system property.
     */
    public void testContextFromProviderFileURL() throws Exception
    {
        Properties properties = new Properties();
        properties.put("connectionfactory.qpidConnectionfactory", CONNECTION_URL);
        properties.put("destination.topicExchange", "ADDR:destName");

        File f = File.createTempFile(getTestName(), ".properties");
        try
        {
            createJndiPropertiesFile(properties, f);

            setTestSystemProperty(InitialContext.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
            setTestSystemProperty(InitialContext.PROVIDER_URL,  f.toURI().toURL().toString());

            InitialContext context = new InitialContext();
            Destination dest = (Destination) context.lookup("topicExchange");
            assertNotNull("Lookup from URI based context should not be null", dest);
            assertTrue("Unexpected value from lookup", dest.toString().contains("destName"));

            ConnectionFactory factory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
            assertTrue("ConnectionFactory was not an instance of AMQConnectionFactory", factory instanceof AMQConnectionFactory);
            assertEquals("Unexpected ConnectionURL value", CONNECTION_URL.replaceAll("password", "********"),
                         ((AMQConnectionFactory) factory).getConnectionURLString());

            context.close();
        }
        finally
        {
            f.delete();
        }
    }

    public void testContextFromFileOnFileSystem() throws Exception
    {
        Properties properties = new Properties();
        properties.put("connectionfactory.qpidConnectionfactory", CONNECTION_URL);

        File f = File.createTempFile(getTestName(), ".properties");
        try
        {
            createJndiPropertiesFile(properties, f);

            setTestSystemProperty(InitialContext.INITIAL_CONTEXT_FACTORY,
                                  "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
            setTestSystemProperty(InitialContext.PROVIDER_URL, f.getAbsolutePath());

            InitialContext context = new InitialContext();
            ConnectionFactory factory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
            assertNotNull("Connection factory not present in loaded configuration", factory);

            context.close();
        }
        finally
        {
            f.delete();
        }
    }

    public void testNonExistentFileHandling() throws Exception
    {
        Hashtable environment = new Hashtable();
        // Make sure the filename does not exist
        File f = File.createTempFile(getTestName(), ".properties");
        f.delete();

        environment.put(Context.PROVIDER_URL, f.getAbsolutePath());
        environment.put(InitialContext.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");

        try
        {
            new InitialContext(environment);
            fail("Expected exception not thrown.");
        }
        catch (NamingException e)
        {
            assertTrue("No IOException chained", e.getCause() instanceof IOException);
        }

    }

    private void createJndiPropertiesFile(final Properties properties, final File f) throws IOException
    {
        try(FileOutputStream fos = new FileOutputStream(f))
        {
            properties.store(fos, null);
        }
    }

}
