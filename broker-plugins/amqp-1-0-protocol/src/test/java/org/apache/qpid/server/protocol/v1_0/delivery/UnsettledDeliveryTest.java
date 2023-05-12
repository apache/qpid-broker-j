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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Objects;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.LinkEndpoint;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.test.utils.UnitTestBase;

class UnsettledDeliveryTest extends UnitTestBase
{
    private static final byte[] DATA = new byte[]{(byte) 32, (byte) 33, (byte) 34};

    private Binary _deliveryTag;
    private LinkEndpoint<?, ?> _linkEndpoint;
    private UnsettledDelivery _unsettledDelivery;

    @BeforeAll
    void setUp()
    {
        _deliveryTag = new Binary(DATA);
        _linkEndpoint = mock(LinkEndpoint.class);
        _unsettledDelivery = new UnsettledDelivery(_deliveryTag, _linkEndpoint);
    }

    @Test
    void getDeliveryTag()
    {
        assertThat(_unsettledDelivery.getDeliveryTag(), is(equalTo(_deliveryTag)));
    }

    @Test
    void getLinkEndpoint()
    {
        assertThat(_unsettledDelivery.getLinkEndpoint(), is(equalTo(_linkEndpoint)));
    }

    @Test
    void equalsToNewUnsettledDeliveryWithTheSameFields()
    {
        assertThat(_unsettledDelivery.equals(new UnsettledDelivery(_deliveryTag, _linkEndpoint)), is(equalTo(true)));
    }

    @Test
    void equalsToNewUnsettledDeliveryWithEqualsFields()
    {
        assertThat(_unsettledDelivery.equals(new UnsettledDelivery(new Binary(DATA), _linkEndpoint)),
                is(equalTo(true)));
    }

    @Test
    void notEqualsWhenDeliveryTagIsDifferent()
    {
        assertThat(_unsettledDelivery.equals(new UnsettledDelivery(new Binary(new byte[]{(byte) 32, (byte) 33}),
                _linkEndpoint)), is(equalTo(false)));
    }

    @Test
    void notEqualsWhenLinkEndpointIsDifferent()
    {
        final LinkEndpoint<?, ?> linkEndpoint = mock(LinkEndpoint.class);
        assertThat(_unsettledDelivery.equals(new UnsettledDelivery(new Binary(new byte[]{(byte) 32, (byte) 33}),
                linkEndpoint)), is(equalTo(false)));
    }

    @Test
    void hashCodes()
    {
        int expected = Objects.hash(_deliveryTag, _linkEndpoint);
        assertThat(_unsettledDelivery.hashCode(), is(equalTo(expected)));
    }
}
