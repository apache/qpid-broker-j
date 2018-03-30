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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.junit.Test;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class UnacknowledgedMessageMapTest extends UnitTestBase
{
    public static final Function<MessageConsumerAssociation, MessageInstance>
            MESSAGE_INSTANCE_FUNCTION = new Function<MessageConsumerAssociation, MessageInstance>()

    {
        @Override
        public MessageInstance apply(final MessageConsumerAssociation input)
        {
            return input.getMessageInstance();
        }
    };
    private final MessageInstanceConsumer _consumer = mock(MessageInstanceConsumer.class);

    @Test
    public void testDeletedMessagesCantBeAcknowledged()
    {
        UnacknowledgedMessageMap map = new UnacknowledgedMessageMapImpl(100, mock(CreditRestorer.class));
        final int expectedSize = 5;
        MessageInstance[] msgs = populateMap(map,expectedSize);
        assertEquals((long) expectedSize, (long) map.size());
        Collection<MessageConsumerAssociation> acknowledged = map.acknowledge(100, true);
        Collection<MessageInstance> acknowledgedMessages = Collections2.transform(acknowledged, MESSAGE_INSTANCE_FUNCTION);
        assertEquals((long) expectedSize, (long) acknowledged.size());
        assertEquals((long) 0, (long) map.size());
        for(int i = 0; i < expectedSize; i++)
        {
            assertTrue("Message " + i + " is missing", acknowledgedMessages.contains(msgs[i]));
        }

        map = new UnacknowledgedMessageMapImpl(100, mock(CreditRestorer.class));
        msgs = populateMap(map,expectedSize);
        // simulate some messages being ttl expired
        when(msgs[2].makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.FALSE);
        when(msgs[4].makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.FALSE);

        assertEquals((long) expectedSize, (long) map.size());

        acknowledged = map.acknowledge(100, true);
        acknowledgedMessages = Collections2.transform(acknowledged, MESSAGE_INSTANCE_FUNCTION);
        assertEquals((long) (expectedSize - 2), (long) acknowledged.size());
        assertEquals((long) 0, (long) map.size());
        for(int i = 0; i < expectedSize; i++)
        {
            assertEquals(i != 2 && i != 4, acknowledgedMessages.contains(msgs[i]));
        }

    }

    public MessageInstance[] populateMap(final UnacknowledgedMessageMap map, int size)
    {
        MessageInstance[] msgs = new MessageInstance[size];
        for(int i = 0; i < size; i++)
        {
            msgs[i] = createMessageInstance(i);
            map.add((long)i, msgs[i], _consumer, true);
        }
        return msgs;
    }

    private MessageInstance createMessageInstance(final int id)
    {
        MessageInstance instance = mock(MessageInstance.class);
        when(instance.makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.TRUE);
        when(instance.getAcquiringConsumer()).thenReturn(_consumer);
        ServerMessage message = mock(ServerMessage.class);
        when(message.getSize()).thenReturn(0L);
        when(instance.getMessage()).thenReturn(message);
        return instance;
    }
}
