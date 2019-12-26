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
package org.apache.qpid.server.util;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class StateChangeListenerEntry<T, E>
{
    private static final AtomicReferenceFieldUpdater<StateChangeListenerEntry, StateChangeListenerEntry> NEXT =
            AtomicReferenceFieldUpdater.newUpdater(StateChangeListenerEntry.class, StateChangeListenerEntry.class, "_next");

    private volatile StateChangeListenerEntry<T, E> _next;
    private volatile StateChangeListener<T,E> _listener;

    public StateChangeListenerEntry(final StateChangeListener<T, E> listener)
    {
        _listener = listener;
    }

    public StateChangeListener<T, E> getListener()
    {
        return _listener;
    }

    public StateChangeListenerEntry<T, E> next()
    {
        return (StateChangeListenerEntry<T, E>) NEXT.get(this);
    }

    private boolean append(StateChangeListenerEntry<T, E> entry)
    {
        return NEXT.compareAndSet(this, null, entry);
    }

    public void add(StateChangeListener<T,E> listener)
    {
        add(new StateChangeListenerEntry<>(listener));
    }

    public void add(final StateChangeListenerEntry<T, E> entry)
    {
        StateChangeListenerEntry<T, E> tail = this;
        while(!entry.getListener().equals(tail.getListener()) && !tail.append(entry))
        {
            tail = tail.next();
        }
    }

    public boolean remove(final StateChangeListener<T, E> listener)
    {
        if(listener.equals(_listener))
        {
            _listener = null;
            return true;
        }
        else
        {
            final StateChangeListenerEntry<T, E> next = next();
            if(next != null)
            {
                boolean returnVal = next.remove(listener);
                StateChangeListenerEntry<T,E> nextButOne;
                if(next._listener == null && (nextButOne = next.next()) != null)
                {
                    NEXT.compareAndSet(this, next, nextButOne);
                }
                return returnVal;
            }
            else
            {
                return false;
            }
        }
    }
}
