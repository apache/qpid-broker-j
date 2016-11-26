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

import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class UnacknowledgedMessageMapImpl implements UnacknowledgedMessageMap
{
    private static final class EntryImpl implements Entry
    {
        private final MessageInstance _messageInstance;
        private final ConsumerImpl _consumer;
        private final boolean _usesCredit;

        private EntryImpl(final MessageInstance messageInstance, final ConsumerImpl consumer, final boolean usesCredit)
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
        public ConsumerImpl getConsumer()
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
    private final Map<Long, EntryImpl> _map;
    // we keep this separately as it is accessed by the management thread
    private volatile int _size;

    private CreditRestorer _creditRestorer;

    UnacknowledgedMessageMapImpl(int prefetchLimit, CreditRestorer creditRestorer)
    {
        _map = new LinkedHashMap<>(prefetchLimit);
        _creditRestorer = creditRestorer;
    }

    public void collect(long deliveryTag, boolean multiple, Map<Long, Entry> msgs)
    {
        if (multiple)
        {
            collect(deliveryTag, msgs);
        }
        else
        {
            final Entry entry = _map.get(deliveryTag);
            if(entry != null)
            {
                msgs.put(deliveryTag, entry);
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

    public Entry remove(long deliveryTag, final boolean restoreCredit)
    {
        EntryImpl entry = _map.remove(deliveryTag);
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

    public void visit(Visitor visitor)
    {
        for (Map.Entry<Long, EntryImpl> entry : _map.entrySet())
        {
            visitor.callback(entry.getKey(), entry.getValue().getMessageInstance());
        }
        visitor.visitComplete();
    }

    public void add(long deliveryTag, MessageInstance message, final ConsumerImpl consumer, final boolean usesCredit)
    {
        if(_map.put(deliveryTag, new EntryImpl(message, consumer, usesCredit)) == null)
        {
            _size++;
        }
        else
        {
            throw new ConnectionScopedRuntimeException("Unexpected duplicate delivery tag created");
        }
    }

    public int size()
    {
        return _size;
    }

    public MessageInstance get(long key)
    {
        Entry messageTargetPair = _map.get(key);
        return messageTargetPair == null ? null : messageTargetPair.getMessageInstance();
    }

    public Collection<MessageInstance> acknowledge(long deliveryTag, boolean multiple)
    {
        if(multiple)
        {
            Map<Long, Entry> ackedMessageMap = new LinkedHashMap<>();
            collect(deliveryTag, multiple, ackedMessageMap);
            remove(ackedMessageMap.keySet());
            List<MessageInstance> acknowledged = new ArrayList<>();
            for (Entry entry : ackedMessageMap.values())
            {
                MessageInstance instance = entry.getMessageInstance();
                if (instance.makeAcquisitionUnstealable(entry.getConsumer()))
                {
                    acknowledged.add(instance);
                }
            }
            return acknowledged;
        }
        else
        {
            MessageInstance instance;
            final Entry entry = remove(deliveryTag, true);
            if(entry != null && (instance = entry.getMessageInstance()).makeAcquisitionUnstealable(entry.getConsumer()))
            {
                return Collections.singleton(instance);
            }
            else
            {
                return Collections.emptySet();
            }

        }
    }

    private void collect(long key, Map<Long, Entry> msgs)
    {
        for (Map.Entry<Long, EntryImpl> entry : _map.entrySet())
        {
            msgs.put(entry.getKey(), entry.getValue());
            if (entry.getKey() == key)
            {
                break;
            }
        }
    }

}
