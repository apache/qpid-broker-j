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
package org.apache.qpid.server.protocol.v0_8.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.test.utils.UnitTestBase;

public class MessagePublishInfoImplTest extends UnitTestBase
{
    private MessagePublishInfo _mpi;
    private final AMQShortString _exchange = AMQShortString.createAMQShortString("exchange");
    private final AMQShortString _routingKey = AMQShortString.createAMQShortString("routingKey");

    @BeforeEach
    public void setUp() throws Exception
    {
        _mpi = new MessagePublishInfo(_exchange, true, true, _routingKey);
    }

    /** Test that we can update the exchange value. */
    @Test
    public void testExchange()
    {
        assertEquals(_exchange, _mpi.getExchange());
        AMQShortString newExchange = AMQShortString.createAMQShortString("newExchange");
        //Check we can update the exchange
        _mpi.setExchange(newExchange);
        assertEquals(newExchange, _mpi.getExchange());
        //Ensure that the new exchange doesn't equal the old one
        assertFalse(_exchange.equals(_mpi.getExchange()));
    }

    /**
     * Check that the immedate value is set correctly and defaulted correctly
     */
    @Test
    public void testIsImmediate()
    {
        //Check that the set value is correct
        assertTrue(_mpi.isImmediate(), "Set value for immediate not as expected");

        MessagePublishInfo mpi = new MessagePublishInfo();

        assertFalse(mpi.isImmediate(), "Default value for immediate should be false");

        mpi.setImmediate(true);

        assertTrue(mpi.isImmediate(), "Updated value for immediate not as expected");
    }

    /**
     * Check that the mandatory value is set correctly and defaulted correctly
     */
    @Test
    public void testIsMandatory()
    {
        assertTrue(_mpi.isMandatory(), "Set value for mandatory not as expected");

        MessagePublishInfo mpi = new MessagePublishInfo();

        assertFalse(mpi.isMandatory(), "Default value for mandatory should be false");

        mpi.setMandatory(true);

        assertTrue(mpi.isMandatory(), "Updated value for mandatory not as expected");
    }

    /**
     * Check that the routingKey value is perserved
     */
    @Test
    public void testRoutingKey()
    {
        assertEquals(_routingKey, _mpi.getRoutingKey());
        AMQShortString newRoutingKey = AMQShortString.createAMQShortString("newRoutingKey");

        //Check we can update the routingKey
        _mpi.setRoutingKey(newRoutingKey);
        assertEquals(newRoutingKey, _mpi.getRoutingKey());
        //Ensure that the new routingKey doesn't equal the old one
        assertFalse(_routingKey.equals(_mpi.getRoutingKey()));
    }
}
