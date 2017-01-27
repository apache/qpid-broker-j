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

package org.apache.qpid.test.utils;

import java.net.URISyntaxException;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;


public interface JmsProvider
{

    ConnectionFactory getConnectionFactory() throws NamingException;

    ConnectionFactory getConnectionFactory(Map<String, String> options) throws NamingException;

    ConnectionFactory getConnectionFactory(String factoryName) throws NamingException;

    ConnectionFactory getConnectionFactory(String factoryName, String vhost, String clientId) throws NamingException;

    ConnectionFactory getConnectionFactory(String factoryName,
                                           String vhost,
                                           String clientId,
                                           Map<String, String> options)
                            throws NamingException;

    Connection getConnection() throws JMSException, NamingException;

    Connection getConnectionWithPrefetch(int prefetch) throws Exception;

    Connection getConnectionWithOptions(Map<String, String> options) throws Exception;

    Connection getConnectionWithOptions(String vhost, Map<String, String> options) throws Exception;

    Connection getConnectionForVHost(String vhost)
                                    throws Exception;

    Connection getConnectionForVHost(String vhost, String username, String password)
                                            throws Exception;

    Connection getConnection(String urlString) throws Exception;

    Connection getConnection(String username, String password) throws JMSException, NamingException;

    Connection getConnectionWithSyncPublishing() throws Exception;

    Connection getClientConnection(String username, String password, String id) throws Exception;

    Queue getTestQueue(String testQueueName);

    Queue getQueueFromName(Session session, String name) throws JMSException;

    Queue createTestQueue(Session session, String queueName) throws JMSException;

    Topic getTestTopic(String testQueueName);

    Topic createTopic(Connection con, String topicName) throws JMSException;

    Topic createTopicOnDirect(Connection con, String topicName) throws JMSException, URISyntaxException;

    Topic createTopicOnFanout(Connection con, String topicName) throws JMSException, URISyntaxException;

    long getQueueDepth(Connection con, Queue destination) throws Exception;

    boolean isQueueExist(Connection con, Queue destination) throws Exception;

    String getBrokerDetailsFromDefaultConnectionUrl();
}
