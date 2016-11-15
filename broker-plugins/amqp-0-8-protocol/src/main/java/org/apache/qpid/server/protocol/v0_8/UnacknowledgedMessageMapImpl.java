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

class UnacknowledgedMessageMapImpl implements UnacknowledgedMessageMap
{
    private Map<Long, MessageInstance> _map;
    private volatile int _size;

    private final int _prefetchLimit;

    UnacknowledgedMessageMapImpl(int prefetchLimit)
    {
        _prefetchLimit = prefetchLimit;
        _map = new LinkedHashMap<>(prefetchLimit);
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
            remove(deliveryTag);
        }
    }

    public MessageInstance remove(long deliveryTag)
    {
        MessageInstance message = _map.remove(deliveryTag);
        if(message != null)
        {
            _size--;
        }
        return message;
    }

    public void visit(Visitor visitor)
    {
        for (Map.Entry<Long, MessageInstance> entry : _map.entrySet())
        {
            visitor.callback(entry.getKey(), entry.getValue());
        }
        visitor.visitComplete();
    }

    public void add(long deliveryTag, MessageInstance message)
    {
        if(_map.put(deliveryTag, message) == null)
        {
            _size++;
        }
    }

    public Collection<MessageInstance> cancelAllMessages()
    {
        Collection<MessageInstance> currentEntries = _map.values();
        _map = new LinkedHashMap<>(_prefetchLimit);
        _size = 0;
        return currentEntries;
    }

    public int size()
    {
        return _size;
    }

    public void clear()
    {
        _map.clear();
        _size = 0;
    }

    public MessageInstance get(long key)
    {
        return _map.get(key);
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
            instance = remove(deliveryTag);
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
        for (Map.Entry<Long, MessageInstance> entry : _map.entrySet())
        {
            msgs.put(entry.getKey(), entry.getValue());
            if (entry.getKey() == key)
            {
                break;
            }
        }
    }

}
