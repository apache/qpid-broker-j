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
package org.apache.qpid.server.consumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ScheduledConsumerTargetSet<C extends AbstractConsumerTarget> implements Set<C>
{
    private final ConcurrentLinkedQueue<C> _underlying = new ConcurrentLinkedQueue<>();

    @Override
    public boolean add(final C c)
    {
        if(c.setScheduled())
        {
            return _underlying.add(c);
        }
        else
        {
            return false;
        }
    }

    @Override
    public boolean isEmpty()
    {
        return _underlying.isEmpty();
    }

    @Override
    public int size()
    {
        return _underlying.size();
    }

    @Override
    public boolean contains(final Object o)
    {
        return _underlying.contains(o);
    }

    @Override
    public boolean remove(final Object o)
    {
        ((C)o).clearScheduled();
        return _underlying.remove(o);
    }

    @Override
    public boolean addAll(final Collection<? extends C> c)
    {
        boolean result = false;
        for(C consumer : c)
        {
            result = _underlying.add(consumer) || result;
        }
        return result;
    }

    @Override
    public Object[] toArray()
    {
        return _underlying.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a)
    {
        return _underlying.toArray(a);
    }

    @Override
    public Iterator<C> iterator()
    {
        return new ScheduledConsumerIterator();
    }

    @Override
    public void clear()
    {
        for(C consumer : _underlying)
        {
            remove(consumer);
        }
    }

    @Override
    public boolean containsAll(final Collection<?> c)
    {
        return _underlying.containsAll(c);
    }

    @Override
    public boolean removeAll(final Collection<?> c)
    {
        boolean result = false;
        for(Object consumer : c)
        {
            result = _underlying.remove((C)consumer) || result;
        }
        return result;
    }

    @Override
    public boolean retainAll(final Collection<?> c)
    {
        boolean modified = false;
        Iterator<C> iterator = iterator();
        while (iterator.hasNext())
        {
            if (!c.contains(iterator.next()))
            {
                iterator.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public String toString()
    {
        return _underlying.toString();
    }

    @Override
    public boolean equals(final Object o)
    {
        return _underlying.equals(o);
    }

    @Override
    public int hashCode()
    {
        return _underlying.hashCode();
    }

    private class ScheduledConsumerIterator implements Iterator<C>
    {
        private final Iterator<C> _underlyingIterator;
        private C _current;

        public ScheduledConsumerIterator()
        {
            _underlyingIterator = _underlying.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return _underlyingIterator.hasNext();
        }

        @Override
        public C next()
        {
            _current = _underlyingIterator.next();
            return _current;
        }

        @Override
        public void remove()
        {
            _underlyingIterator.remove();
            _current.clearScheduled();
        }
    }
}
