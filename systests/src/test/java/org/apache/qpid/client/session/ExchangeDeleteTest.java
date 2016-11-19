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

package org.apache.qpid.client.session;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ExchangeDeleteTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private AMQSession<?, ?> _session;
    protected void setUp() throws Exception
    {
        super.setUp();

        // Turn off queue declare side effect of creating consumer
        setTestClientSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");
        setTestClientSystemProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "false");
        setTestClientSystemProperty(ClientProperties.QPID_BIND_QUEUES_PROP_NAME, "false");

        _connection = getConnection();
        _connection.start();
        _session = (AMQSession<?, ?>) _connection.createSession(true, Session.SESSION_TRANSACTED);
    }

    public void testDeleteExchange() throws Exception
    {
        String exchangeName = getTestName();

        _session.declareExchange(exchangeName, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

        _session.deleteExchange(exchangeName);
    }

    public void testDeleteNonExistentExchange() throws Exception
    {
        String exchangeName = getTestName();

        try
        {
            _session.deleteExchange(exchangeName);
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
            assertEquals("Expecting exchange not found",
                         String.valueOf(ErrorCodes.NOT_FOUND),
                         e.getErrorCode());
        }

        assertTrue("Session expected to be closed", _session.isClosed());

    }

}
