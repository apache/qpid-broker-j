/*
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

package org.apache.qpid.systests;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class QpidJmsClientProvider implements JmsProvider
{
    private final AmqpManagementFacade _managementFacade;

    public QpidJmsClientProvider(AmqpManagementFacade managementFacade)
    {
        _managementFacade = managementFacade;
    }

    @Override
    public Connection getConnection(String urlString) throws Exception
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Queue getTestQueue(final String testQueueName) throws NamingException
    {
        return (Queue) getDestination("queue", testQueueName);
    }

    @Override
    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return session.createQueue(name);
    }

    @Override
    public Queue createQueue(Session session, String queueName) throws JMSException
    {
        _managementFacade.createEntityUsingAmqpManagement(queueName, session, "org.apache.qpid.Queue");

        return session.createQueue(queueName);
    }

    @Override
    public Topic getTestTopic(final String testTopicName) throws NamingException
    {
        return (Topic) getDestination("topic", testTopicName);
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _managementFacade.createEntityUsingAmqpManagement(topicName, session, "org.apache.qpid.TopicExchange");

        return session.createTopic(topicName);
    }

    @Override
    public Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createTopic("amq.direct/" + topicName);
    }

    @Override
    public Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createTopic("amq.fanout/" + topicName);
    }

    @Override
    public ConnectionBuilder getConnectionBuilder()
    {
        return new QpidJmsClientConnectionBuilder();
    }

    private Destination getDestination(String type, String name) throws NamingException
    {
        final String jndiName = "test";
        final Properties initialContextProperties = new Properties();
        initialContextProperties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        initialContextProperties.put(type + "." + jndiName, name);

        InitialContext initialContext = new InitialContext(initialContextProperties);
        try
        {
            return (Destination) initialContext.lookup(jndiName);
        }
        finally
        {
            initialContext.close();
        }
    }

    @Override
    public void addGenericConnectionListener(final Connection connection, final GenericConnectionListener listener)
    {
        try
        {
            final Class<?> iface = Class.forName("org.apache.qpid.jms.JmsConnectionListener");
            final Object listenerProxy = Proxy.newProxyInstance(iface.getClassLoader(),
                                                                       new Class[]{iface},
                                                                       (proxy, method, args) -> {
                                                                           final String methodName = method.getName();
                                                                           switch (methodName)
                                                                           {
                                                                               case "onConnectionRestored":
                                                                                   listener.onConnectionRestored(

                                                                                           ((URI) args[0]));
                                                                                   break;
                                                                               case "onConnectionInterrupted":
                                                                                   listener.onConnectionInterrupted(

                                                                                           ((URI) args[0]));
                                                                                   break;
                                                                               case "toString":
                                                                                   return String.format("[Proxy %s]",
                                                                                                        listener.toString());
                                                                               case "equals":
                                                                                   Object other = args[0];
                                                                                   return Objects.equals(this, other);
                                                                               case "hashCode":
                                                                                   return Objects.hashCode(this);
                                                                           }
                                                                           return null;
                                                                       });

            final Method addConnectionListener = connection.getClass().getMethod("addConnectionListener", iface);
            addConnectionListener.invoke(connection, listenerProxy);
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException("Unable to reflectively add listener", e);
        }
    }

    @Override
    public URI getConnectedURI(final Connection connection)
    {
        final Method connectedURI;
        try
        {
            connectedURI = connection.getClass().getMethod("getConnectedURI", new Class[] {});
            return (URI) connectedURI.invoke(connection, null);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException("Unable to reflectively get connected URI", e);
        }
    }

}
