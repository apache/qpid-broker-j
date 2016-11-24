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
package org.apache.qpid.test.utils;

import java.util.concurrent.CountDownLatch;

import javax.jms.ConnectionFactory;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.jms.ConnectionListener;

public class FailoverBaseCase extends QpidBrokerTestCase implements ConnectionListener
{
    protected static final Logger _logger = LoggerFactory.getLogger(FailoverBaseCase.class);

    public static final long DEFAULT_FAILOVER_TIME = Long.getLong("FailoverBaseCase.defaultFailoverTime", 10000L);

    protected CountDownLatch _failoverStarted;
    protected CountDownLatch _failoverComplete;
    protected BrokerHolder _alternativeBroker;
    protected int _port;
    protected int _alternativePort;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _failoverComplete = new CountDownLatch(1);
        _failoverStarted = new CountDownLatch(1);

        _alternativeBroker = createSpawnedBroker();
        _alternativeBroker.start();
        _alternativePort = _alternativeBroker.getAmqpPort();

        _port = getDefaultBroker().getAmqpPort();
        setTestSystemProperty("test.port.alt", String.valueOf(_alternativePort));
        setTestSystemProperty("test.port", String.valueOf(_port));
    }

    /**
     * We are using failover factories
     *
     * @return a connection 
     * @throws Exception
     */
    @Override
    public ConnectionFactory getConnectionFactory() throws NamingException
    {
        _logger.info("get ConnectionFactory");
        if (_connectionFactory == null)
        {
            if (Boolean.getBoolean("profile.use_ssl"))
            {
                _connectionFactory = getConnectionFactory("failover.ssl");
            }
            else
            {
                _connectionFactory = getConnectionFactory("failover");
            }
        }
        return _connectionFactory;
    }

    public void failDefaultBroker()
    {
        failBroker(getDefaultBroker());
    }

    public void failBroker(BrokerHolder broker)
    {
        try
        {
            //TODO: use killBroker instead
            broker.shutdown();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void restartBroker(BrokerHolder broker) throws Exception
    {
        broker.restart();
    }

    @Override
    public void bytesSent(long count)
    {
    }

    @Override
    public void bytesReceived(long count)
    {
    }

    @Override
    public boolean preFailover(boolean redirect)
    {
        _failoverStarted.countDown();
        return true;
    }

    @Override
    public boolean preResubscribe()
    {
        return true;
    }

    @Override
    public void failoverComplete()
    {
        _failoverComplete.countDown();
    }
}
