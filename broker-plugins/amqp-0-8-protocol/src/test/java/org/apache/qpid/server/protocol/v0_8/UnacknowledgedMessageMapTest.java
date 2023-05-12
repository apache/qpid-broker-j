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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class UnacknowledgedMessageMapTest extends UnitTestBase
{
    public static final Function<MessageConsumerAssociation, MessageInstance>
            MESSAGE_INSTANCE_FUNCTION = MessageConsumerAssociation::getMessageInstance;
    private final MessageInstanceConsumer _consumer = mock(MessageInstanceConsumer.class);

    @Test
    void deletedMessagesCantBeAcknowledged()
    {
        UnacknowledgedMessageMap map = new UnacknowledgedMessageMapImpl(100, mock(CreditRestorer.class));
        final int expectedSize = 5;
        MessageInstance[] msgs = populateMap(map,expectedSize);
        assertEquals(expectedSize, (long) map.size());
        Collection<MessageConsumerAssociation> acknowledged = map.acknowledge(100, true);
        Collection<MessageInstance> acknowledgedMessages = acknowledged.stream().map(MESSAGE_INSTANCE_FUNCTION)
                .collect(Collectors.toList());
        assertEquals(expectedSize, (long) acknowledged.size());
        assertEquals(0, (long) map.size());
        for (int i = 0; i < expectedSize; i++)
        {
            assertTrue(acknowledgedMessages.contains(msgs[i]), "Message " + i + " is missing");
        }

        map = new UnacknowledgedMessageMapImpl(100, mock(CreditRestorer.class));
        msgs = populateMap(map,expectedSize);
        // simulate some messages being ttl expired
        when(msgs[2].makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.FALSE);
        when(msgs[4].makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.FALSE);

        assertEquals(expectedSize, (long) map.size());

        acknowledged = map.acknowledge(100, true);
        acknowledgedMessages = acknowledged.stream().map(MESSAGE_INSTANCE_FUNCTION)
                .collect(Collectors.toList());
        assertEquals(expectedSize - 2, (long) acknowledged.size());
        assertEquals(0, (long) map.size());
        for (int i = 0; i < expectedSize; i++)
        {
            assertEquals(i != 2 && i != 4, acknowledgedMessages.contains(msgs[i]));
        }
    }

    MessageInstance[] populateMap(final UnacknowledgedMessageMap map, final int size)
    {
        final MessageInstance[] msgs = new MessageInstance[size];
        for (int i = 0; i < size; i++)
        {
            msgs[i] = createMessageInstance();
            map.add(i, msgs[i], _consumer, true);
        }
        return msgs;
    }

    private MessageInstance createMessageInstance()
    {
        final MessageInstance instance = mock(MessageInstance.class);
        when(instance.makeAcquisitionUnstealable(_consumer)).thenReturn(Boolean.TRUE);
        when(instance.getAcquiringConsumer()).thenReturn(_consumer);
        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.getSize()).thenReturn(0L);
        when(instance.getMessage()).thenReturn(message);
        return instance;
    }
}
