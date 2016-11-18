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
 */

package org.apache.qpid.client;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TemporaryQueue;


public class TemporaryQueuePrefixTest extends QpidBrokerTestCase
{
    @Override
    public void startDefaultBroker() throws Exception
    {
        // deliberately don't start broker
    }

    public void testNoPrefixSet() throws Exception
    {
        super.startDefaultBroker();
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        assertTrue(queue.getQueueName() + " does not start with \"TempQueue\".", queue.getQueueName().startsWith("TempQueue"));
        connection.close();
    }

    public void testEmptyPrefix() throws Exception
    {
        String prefix = "[]";
        setTestSystemProperty("qpid.globalAddressDomains", prefix);
        super.startDefaultBroker();
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        assertTrue(queue.getQueueName() + " does not start with \"TempQueue\".", queue.getQueueName().startsWith("TempQueue"));
        connection.close();
    }

    public void testTwoDomains() throws Exception
    {
        final String primaryPrefix = "/testPrefix";
        String prefix = "[ \\\"" + primaryPrefix + "\\\", \\\"/foo\\\" ]";
        setTestSystemProperty("qpid.globalAddressDomains", prefix);
        super.startDefaultBroker();
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        assertFalse(queue.getQueueName() + " has superfluous slash in prefix.", queue.getQueueName().startsWith(prefix + "/"));
        assertTrue(queue.getQueueName() + " does not start with expected prefix \"" + primaryPrefix + "\".", queue.getQueueName().startsWith(primaryPrefix));
        connection.close();
    }

    public void testPrefix() throws Exception
    {
        String prefix = "/testPrefix";
        setTestSystemProperty("qpid.globalAddressDomains", prefix);
        super.startDefaultBroker();
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();

        assertTrue(queue.getQueueName() + " does not start with expected prefix \"" + prefix + "/\".", queue.getQueueName().startsWith(prefix + "/"));
        connection.close();
    }
}
