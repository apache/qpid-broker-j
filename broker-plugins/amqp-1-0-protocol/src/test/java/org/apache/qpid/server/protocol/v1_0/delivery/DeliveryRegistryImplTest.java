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
package org.apache.qpid.server.protocol.v1_0.delivery;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.LinkEndpoint;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.test.utils.UnitTestBase;

public class DeliveryRegistryImplTest extends UnitTestBase
{
    private static final UnsignedInteger DELIVERY_ID = UnsignedInteger.ZERO;
    private static final UnsignedInteger DELIVERY_ID_2 = UnsignedInteger.ONE;
    private static final Binary DELIVERY_TAG = new Binary(new byte[]{(byte) 32, (byte) 33});
    private static final Binary DELIVERY_TAG_2 = new Binary(new byte[]{(byte) 32});

    private DeliveryRegistryImpl _registry;
    private UnsettledDelivery _unsettledDelivery;

    @Before
    public void setUp()
    {
        _registry = new DeliveryRegistryImpl();
        _unsettledDelivery = new UnsettledDelivery(DELIVERY_TAG, mock(LinkEndpoint.class));
    }

    @Test
    public void addDelivery()
    {
        assertThat(_registry.size(), is(equalTo(0)));

        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);

        assertThat(_registry.size(), is(equalTo(1)));
    }

    @Test
    public void removeDelivery()
    {
        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);
        assertThat(_registry.size(), is(equalTo(1)));
        _registry.removeDelivery(DELIVERY_ID);
        assertThat(_registry.size(), is(equalTo(0)));
        assertThat(_registry.getDelivery(UnsignedInteger.ZERO), is(nullValue()));
    }

    @Test
    public void getDelivery()
    {
        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);

        assertThat(_registry.size(), is(equalTo(1)));
        final UnsettledDelivery expected =
                new UnsettledDelivery(_unsettledDelivery.getDeliveryTag(), _unsettledDelivery.getLinkEndpoint());
        assertThat(_registry.getDelivery(UnsignedInteger.ZERO), is(equalTo(expected)));
    }

    @Test
    public void removeDeliveriesForLinkEndpoint()
    {
        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);
        _registry.addDelivery(DELIVERY_ID_2, new UnsettledDelivery(DELIVERY_TAG_2, _unsettledDelivery.getLinkEndpoint()));
        _registry.addDelivery(UnsignedInteger.valueOf(2), new UnsettledDelivery(DELIVERY_TAG, mock(LinkEndpoint.class)));

        assertThat(_registry.size(), is(equalTo(3)));

        _registry.removeDeliveriesForLinkEndpoint(_unsettledDelivery.getLinkEndpoint());

        assertThat(_registry.size(), is(equalTo(1)));
    }

    @Test
    public void getDeliveryId()
    {
        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);
        _registry.addDelivery(DELIVERY_ID_2, new UnsettledDelivery(DELIVERY_TAG, mock(LinkEndpoint.class)));

        final UnsignedInteger deliveryId = _registry.getDeliveryId(DELIVERY_TAG, _unsettledDelivery.getLinkEndpoint());

        assertThat(deliveryId, is(equalTo(DELIVERY_ID)));
    }

    @Test
    public void size()
    {
        assertThat(_registry.size(), is(equalTo(0)));

        _registry.addDelivery(DELIVERY_ID, _unsettledDelivery);

        assertThat(_registry.size(), is(equalTo(1)));

        _registry.removeDelivery(DELIVERY_ID);

        assertThat(_registry.size(), is(equalTo(0)));
    }
}
