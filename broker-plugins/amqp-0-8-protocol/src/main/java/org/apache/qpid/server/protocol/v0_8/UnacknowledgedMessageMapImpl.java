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

import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class UnacknowledgedMessageMapImpl implements UnacknowledgedMessageMap
{
    private static final class MessageTargetPair
    {
        private final MessageInstance _messageInstance;
        private final ConsumerTarget _target;

        private MessageTargetPair(final MessageInstance messageInstance, final ConsumerTarget target)
        {
            _messageInstance = messageInstance;
            _target = target;
        }

        public MessageInstance getMessageInstance()
        {
            return _messageInstance;
        }

        public ConsumerTarget getTarget()
        {
            return _target;
        }

        public long getSize()
        {
            return _messageInstance.getMessage().getSize();
        }
    }
    private final Map<Long, MessageTargetPair> _map;
    // we keep this separately as it is accessed by the management thread
    private volatile int _size;

    private CreditRestorer _creditRestorer;

    UnacknowledgedMessageMapImpl(int prefetchLimit, CreditRestorer creditRestorer)
    {
        _map = new LinkedHashMap<>(prefetchLimit);
        _creditRestorer = creditRestorer;
    }

    public void collect(long deliveryTag, boolean multiple, Map<Long, MessageInstance> msgs)
    {
        if (multiple)
        {
            collect(deliveryTag, msgs);
        }
        else
        {
            final MessageInstance entry = get(deliveryTag);
            if(entry != null)
            {
                msgs.put(deliveryTag, entry);
            }
        }

    }

    public void remove(Map<Long,MessageInstance> msgs)
    {
        for (Long deliveryTag : msgs.keySet())
        {
            remove(deliveryTag, true);
        }
    }

    public MessageInstance remove(long deliveryTag, final boolean restoreCredit)
    {
        MessageTargetPair message = _map.remove(deliveryTag);
        if(message != null)
        {
            _size--;
            if(restoreCredit)
            {
                _creditRestorer.restoreCredit(message.getTarget(), 1, message.getSize());
            }
        }
        return message.getMessageInstance();
    }

    public void visit(Visitor visitor)
    {
        for (Map.Entry<Long, MessageTargetPair> entry : _map.entrySet())
        {
            visitor.callback(entry.getKey(), entry.getValue().getMessageInstance());
        }
        visitor.visitComplete();
    }

    public void add(long deliveryTag, MessageInstance message, final ConsumerTarget target)
    {
        if(_map.put(deliveryTag, new MessageTargetPair(message,target)) == null)
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
        MessageTargetPair messageTargetPair = _map.get(key);
        return messageTargetPair == null ? null : messageTargetPair.getMessageInstance();
    }

    public Collection<MessageInstance> acknowledge(long deliveryTag, boolean multiple)
    {
        if(multiple)
        {
            Map<Long, MessageInstance> ackedMessageMap = new LinkedHashMap<>();
            collect(deliveryTag, multiple, ackedMessageMap);
            remove(ackedMessageMap);
            List<MessageInstance> acknowledged = new ArrayList<>();
            for (MessageInstance instance : ackedMessageMap.values())
            {
                if (instance.makeAcquisitionUnstealable(instance.getAcquiringConsumer()))
                {
                    acknowledged.add(instance);
                }
            }
            return acknowledged;
        }
        else
        {
            MessageInstance instance;
            instance = remove(deliveryTag, true);
            if(instance != null && instance.makeAcquisitionUnstealable(instance.getAcquiringConsumer()))
            {
                return Collections.singleton(instance);
            }
            else
            {
                return Collections.emptySet();
            }

        }
    }

    private void collect(long key, Map<Long, MessageInstance> msgs)
    {
        for (Map.Entry<Long, MessageTargetPair> entry : _map.entrySet())
        {
            msgs.put(entry.getKey(), entry.getValue().getMessageInstance());
            if (entry.getKey() == key)
            {
                break;
            }
        }
    }

}
