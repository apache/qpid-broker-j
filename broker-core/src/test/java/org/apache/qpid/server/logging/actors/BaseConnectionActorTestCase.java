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
package org.apache.qpid.server.logging.actors;

import org.junit.After;
import org.junit.Before;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.transport.AMQPConnection;

public abstract class BaseConnectionActorTestCase extends BaseActorTestCase
{
    private AMQPConnection<?> _connection;
    private VirtualHost<?> _virtualHost;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        BrokerTestHelper.setUp();
        _connection = BrokerTestHelper.createConnection();
        _virtualHost = BrokerTestHelper.createVirtualHost("test", this);
    }

    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if(_virtualHost != null)
            {
                _virtualHost.close();
            }
            if (_connection != null)
            {
                _connection.sendConnectionCloseAsync(AMQPConnection.CloseReason.MANAGEMENT, "");
            }
        }
        finally
        {
            BrokerTestHelper.tearDown();
            super.tearDown();
        }
    }

    public AMQPConnection<?> getConnection()
    {
        return _connection;
    }

}
