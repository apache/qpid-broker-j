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
package org.apache.qpid.server.exchange;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.virtualhost.ExchangeIsAlternateException;
import org.apache.qpid.server.virtualhost.ReservedExchangeNameException;
import org.apache.qpid.test.utils.QpidTestCase;

public class DirectExchangeTest extends QpidTestCase
{
    private DirectExchangeImpl _exchange;
    private VirtualHost<?> _vhost;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        BrokerTestHelper.setUp();
        _vhost = BrokerTestHelper.createVirtualHost(getName());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "test");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
        _exchange.open();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_vhost != null)
            {
                _vhost.close();
            }
        }
        finally
        {
            BrokerTestHelper.tearDown();
            super.tearDown();
        }
    }

    public void testCreationOfExchangeWithReservedExchangePrefixRejected() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "amq.wibble");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
            _exchange.open();
            fail("Exception not thrown");
        }
        catch (ReservedExchangeNameException rene)
        {
            // PASS
        }
    }

    public void testAmqpDirectRecreationRejected() throws Exception
    {
        DirectExchangeImpl ampqDirect = (DirectExchangeImpl) _vhost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        assertNotNull(ampqDirect);

        assertSame(ampqDirect, _vhost.getChildById(Exchange.class, ampqDirect.getId()));
        assertSame(ampqDirect, _vhost.getChildByName(Exchange.class, ampqDirect.getName()));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        attributes.put(Exchange.DURABLE, true);
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
            _exchange.open();
            fail("Exception not thrown");
        }
        catch (ReservedExchangeNameException rene)
        {
            // PASS
        }

        // QPID-6599, defect would mean that the existing exchange was removed from the in memory model.
        assertSame(ampqDirect, _vhost.getChildById(Exchange.class, ampqDirect.getId()));
        assertSame(ampqDirect, _vhost.getChildByName(Exchange.class, ampqDirect.getName()));

    }

    public void testDeleteOfExchangeSetAsAlternate() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.DURABLE, false);
        attributes.put(Queue.ALTERNATE_EXCHANGE, _exchange.getName());

        Queue queue = (Queue) _vhost.createChild(Queue.class, attributes);
        queue.open();

        assertEquals("Unexpected alternate exchange on queue", _exchange, queue.getAlternateExchange());

        try
        {
            _exchange.delete();
            fail("Exchange deletion should fail with ExchangeIsAlternateException");
        }
        catch(ExchangeIsAlternateException e)
        {
            // pass
        }

        assertEquals("Unexpected effective exchange state", State.ACTIVE, _exchange.getState());
        assertEquals("Unexpected desired exchange state", State.ACTIVE, _exchange.getDesiredState());
    }

}
