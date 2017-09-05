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
package org.apache.qpid.server.routing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Session;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ExchangeRoutingTest extends QpidBrokerTestCase
{

    private static final String AMQP_MNG_QPID_EXCHANGE_DIRECT = "org.apache.qpid.DirectExchange";
    private static final String AMQP_MNG_QPID_EXCHANGE_FANOUT = "org.apache.qpid.FanoutExchange";
    private static final String AMQP_MNG_QPID_EXCHANGE_TOPIC = "org.apache.qpid.FanoutExchange";
    private static final String AMQP_MNG_QPID_QUEUE_STANDARD = "org.apache.qpid.StandardQueue";
    private String _queueName;
    private String _exchName1;
    private String _exchName2;
    private Connection _connection;
    private Session _session;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _queueName = getTestQueueName() + "_queue";
        _exchName1 = getTestQueueName() + "_exch1";
        _exchName2 = getTestQueueName() + "_exch2";

        _connection = getConnection();
        _connection.start();
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);

        createEntityUsingAmqpManagement(_queueName, _session, AMQP_MNG_QPID_QUEUE_STANDARD);

    }

    public void testExchangeToQueueRouting() throws Exception
    {
        String routingKey = "key";

        createEntityUsingAmqpManagement(_exchName1, _session, AMQP_MNG_QPID_EXCHANGE_DIRECT);

        final Map<String, Object> bindingArguments = new HashMap<>();
        bindingArguments.put("destination", _queueName);
        bindingArguments.put("bindingKey", routingKey);

        performOperationUsingAmqpManagement(_exchName1,
                                            "bind",
                                            _session,
                                            "org.apache.qpid.Exchange",
                                            bindingArguments);

        routeTest(_exchName1, _queueName, "unboundKey", 0, 0);
        routeTest(_exchName1, _queueName, routingKey, 0, 1);
    }

    public void testExchangeToExchangeToQueueRouting() throws Exception
    {
        String bindingKey = "key";

        createEntityUsingAmqpManagement(_exchName1, _session, AMQP_MNG_QPID_EXCHANGE_DIRECT);
        createEntityUsingAmqpManagement(_exchName2, _session, AMQP_MNG_QPID_EXCHANGE_DIRECT);

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", _exchName2);
        binding1Arguments.put("bindingKey", bindingKey);

        performOperationUsingAmqpManagement(_exchName1,
                                            "bind",
                                            _session,
                                            "org.apache.qpid.Exchange",
                                            binding1Arguments);

        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", _queueName);
        binding2Arguments.put("bindingKey", bindingKey);

        performOperationUsingAmqpManagement(_exchName2,
                                            "bind",
                                            _session,
                                            "org.apache.qpid.Exchange",
                                            binding2Arguments);

        routeTest(_exchName1, _queueName, bindingKey, 0, 1);
    }

    public void testExchangeToExchangeToQueueRoutingWithReplacementRoutingKey() throws Exception
    {
        String bindingKey1 = "key1";
        String bindingKey2 = "key2";

        createEntityUsingAmqpManagement(_exchName1, _session, AMQP_MNG_QPID_EXCHANGE_DIRECT);
        createEntityUsingAmqpManagement(_exchName2, _session, AMQP_MNG_QPID_EXCHANGE_DIRECT);

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", _exchName2);
        binding1Arguments.put("bindingKey", bindingKey1);
        binding1Arguments.put("arguments",
                              new ObjectMapper().writeValueAsString(Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                                             bindingKey2)));

        performOperationUsingAmqpManagement(_exchName1,
                                            "bind",
                                            _session,
                                            "org.apache.qpid.Exchange",
                                            binding1Arguments);


        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", _queueName);
        binding2Arguments.put("bindingKey", bindingKey2);

        performOperationUsingAmqpManagement(_exchName2,
                                            "bind",
                                            _session,
                                            "org.apache.qpid.Exchange",
                                            binding2Arguments);

        routeTest(_exchName1, _queueName, bindingKey1, 0, 1);
    }

    private void routeTest(final String fromExchangeName,
                           final String queueName,
                           final String routingKey,
                           final int expectedDepthBefore,
                           final int expectedDepthAfter) throws Exception
    {
        Destination ingressExchangeDest = _session.createQueue(getDestinationAddress(fromExchangeName, routingKey));
        Queue queueDest = _session.createQueue(queueName);

        assertEquals(String.format("Unexpected number of messages on queue '%s'", queueName),
                     expectedDepthBefore, getQueueDepth(_connection, queueDest));

        sendMessage(_session, ingressExchangeDest, 1);

        assertEquals(String.format("Unexpected number of messages on queue '%s", queueName),
                     expectedDepthAfter, getQueueDepth(_connection, queueDest));
    }

    private String getDestinationAddress(final String exchangeName, final String routingKey)
    {
        return isBroker10() ? String.format("%s/%s", exchangeName,routingKey): String.format("ADDR:%s/%s", exchangeName,routingKey);
    }
}
