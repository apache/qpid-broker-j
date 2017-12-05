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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class UnacknowledgedMessageMapImpl implements UnacknowledgedMessageMap
{
    private static final class MessageConsumerAssociationImpl implements MessageConsumerAssociation
    {
        private final MessageInstance _messageInstance;
        private final MessageInstanceConsumer _consumer;
        private final boolean _usesCredit;

        private MessageConsumerAssociationImpl(final MessageInstance messageInstance, final MessageInstanceConsumer consumer, final boolean usesCredit)
        {
            _messageInstance = messageInstance;
            _consumer = consumer;
            _usesCredit = usesCredit;
        }

        @Override
        public MessageInstance getMessageInstance()
        {
            return _messageInstance;
        }

        @Override
        public MessageInstanceConsumer getConsumer()
        {
            return _consumer;
        }

        @Override
        public long getSize()
        {
            return _messageInstance.getMessage().getSize();
        }

        public boolean isUsesCredit()
        {
            return _usesCredit;
        }
    }
    private final Map<Long, MessageConsumerAssociationImpl> _map;
    // we keep this separately as it is accessed by the management thread
    private volatile int _size;

    private final CreditRestorer _creditRestorer;

    UnacknowledgedMessageMapImpl(int prefetchLimit, CreditRestorer creditRestorer)
    {
        _map = new LinkedHashMap<>(prefetchLimit);
        _creditRestorer = creditRestorer;
    }

    @Override
    public void collect(long deliveryTag, boolean multiple, Map<Long, MessageConsumerAssociation> msgs)
    {
        if (multiple)
        {
            collect(deliveryTag, msgs);
        }
        else
        {
            final MessageConsumerAssociation messageConsumerAssociation = _map.get(deliveryTag);
            if(messageConsumerAssociation != null)
            {
                msgs.put(deliveryTag, messageConsumerAssociation);
            }
        }

    }

    private void remove(Collection<Long> msgs)
    {
        for (Long deliveryTag : msgs)
        {
            remove(deliveryTag, true);
        }
    }

    @Override
    public MessageConsumerAssociation remove(long deliveryTag, final boolean restoreCredit)
    {
        MessageConsumerAssociationImpl entry = _map.remove(deliveryTag);
        if(entry != null)
        {
            _size--;
            if(restoreCredit && entry.isUsesCredit())
            {
                _creditRestorer.restoreCredit(entry.getConsumer().getTarget(), 1, entry.getSize());
            }
        }
        return entry;
    }

    @Override
    public void visit(Visitor visitor)
    {
        for (Map.Entry<Long, MessageConsumerAssociationImpl> entry : _map.entrySet())
        {
            visitor.callback(entry.getKey(), entry.getValue());
        }
        visitor.visitComplete();
    }

    @Override
    public void add(long deliveryTag, MessageInstance message, final MessageInstanceConsumer consumer, final boolean usesCredit)
    {
        if(_map.put(deliveryTag, new MessageConsumerAssociationImpl(message, consumer, usesCredit)) == null)
        {
            _size++;
        }
        else
        {
            throw new ConnectionScopedRuntimeException("Unexpected duplicate delivery tag created");
        }
    }

    @Override
    public int size()
    {
        return _size;
    }

    @Override
    public MessageInstance get(long key)
    {
        MessageConsumerAssociation association = _map.get(key);
        return association == null ? null : association.getMessageInstance();
    }

    @Override
    public Collection<MessageConsumerAssociation> acknowledge(long deliveryTag, boolean multiple)
    {
        if(multiple)
        {
            Map<Long, MessageConsumerAssociation> ackedMessageMap = new LinkedHashMap<>();
            collect(deliveryTag, multiple, ackedMessageMap);
            remove(ackedMessageMap.keySet());
            List<MessageConsumerAssociation> acknowledged = new ArrayList<>();
            for (MessageConsumerAssociation messageConsumerAssociation : ackedMessageMap.values())
            {
                MessageInstance instance = messageConsumerAssociation.getMessageInstance();
                if (instance.makeAcquisitionUnstealable(messageConsumerAssociation.getConsumer()))
                {
                    acknowledged.add(messageConsumerAssociation);
                }
            }
            return acknowledged;
        }
        else
        {
            final MessageConsumerAssociation association = remove(deliveryTag, true);
            if (association != null)
            {
                final MessageInstance messageInstance = association.getMessageInstance();
                if (messageInstance != null && messageInstance.makeAcquisitionUnstealable(association.getConsumer()))
                {
                    return Collections.singleton(association);
                }
            }
            return Collections.emptySet();
        }
    }

    private void collect(long key, Map<Long, MessageConsumerAssociation> msgs)
    {
        for (Map.Entry<Long, MessageConsumerAssociationImpl> entry : _map.entrySet())
        {
            msgs.put(entry.getKey(), entry.getValue());
            if (entry.getKey() == key)
            {
                break;
            }
        }
    }

}
