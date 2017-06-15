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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
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
        DirectExchangeImpl amqpDirect = (DirectExchangeImpl) _vhost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        assertNotNull(amqpDirect);

        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));

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
        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));

    }

    public void testDeleteOfExchangeSetAsAlternate() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.DURABLE, false);
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName()));

        Queue queue = (Queue) _vhost.createChild(Queue.class, attributes);
        queue.open();

        assertEquals("Unexpected alternate exchange on queue", _exchange, queue.getAlternateBindingDestination());

        try
        {
            _exchange.delete();
            fail("Exchange deletion should fail with MessageDestinationIsAlternateException");
        }
        catch(MessageDestinationIsAlternateException e)
        {
            // pass
        }

        assertEquals("Unexpected effective exchange state", State.ACTIVE, _exchange.getState());
        assertEquals("Unexpected desired exchange state", State.ACTIVE, _exchange.getDesiredState());
    }

    public void testAlternateBindingValidationRejectsNonExistingDestination()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        attributes.put(Exchange.ALTERNATE_BINDING,
                       Collections.singletonMap(AlternateBinding.DESTINATION, "nonExisting"));

        try
        {
            _vhost.createChild(Exchange.class, attributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testAlternateBindingValidationRejectsSelf()
    {
        Map<String, String> alternateBinding = Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName());
        Map<String, Object> newAttributes = Collections.singletonMap(Exchange.ALTERNATE_BINDING, alternateBinding);
        try
        {
            _exchange.setAttributes(newAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testDurableExchangeRejectsNonDurableAlternateBinding()
    {
        Map<String, Object> dlqAttributes = new HashMap<>();
        String dlqName = getTestName() + "_DLQ";
        dlqAttributes.put(Queue.NAME, dlqName);
        dlqAttributes.put(Queue.DURABLE, false);
        _vhost.createChild(Queue.class, dlqAttributes);

        Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put(Exchange.NAME, getTestName());
        exchangeAttributes.put(Exchange.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, dlqName));
        exchangeAttributes.put(Exchange.DURABLE, true);
        exchangeAttributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _vhost.createChild(Exchange.class, exchangeAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testAlternateBinding()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        attributes.put(Exchange.ALTERNATE_BINDING,
                       Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName()));
        attributes.put(Exchange.DURABLE, false);

        Exchange newExchange = _vhost.createChild(Exchange.class, attributes);

        assertEquals("Unexpected alternate binding",
                     _exchange.getName(),
                     newExchange.getAlternateBinding().getDestination());
    }
}
