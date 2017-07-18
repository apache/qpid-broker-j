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

package org.apache.qpid.server.filter;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.queue.QueueConsumer;

public class FilterSupport
{
    private static final Map<String, WeakReference<JMSSelectorFilter>> _selectorCache =
            Collections.synchronizedMap(new WeakHashMap<String, WeakReference<JMSSelectorFilter>>());

    static MessageFilter createJMSSelectorFilter(Map<String, Object> args) throws AMQInvalidArgumentException
    {
        final String selectorString = (String) args.get(AMQPFilterTypes.JMS_SELECTOR.toString());
        return getMessageFilter(selectorString);
    }


    private static MessageFilter getMessageFilter(String selectorString) throws AMQInvalidArgumentException
    {
        WeakReference<JMSSelectorFilter> selectorRef = _selectorCache.get(selectorString);
        JMSSelectorFilter selector = null;

        if(selectorRef == null || (selector = selectorRef.get())==null)
        {
            try
            {
                selector = new JMSSelectorFilter(selectorString);
            }
            catch (ParseException | SelectorParsingException | TokenMgrError e)
            {
                throw new AMQInvalidArgumentException("Cannot parse JMS selector \"" + selectorString + "\"", e);
            }
            _selectorCache.put(selectorString, new WeakReference<JMSSelectorFilter>(selector));
        }
        return selector;
    }

    public static boolean argumentsContainFilter(final Map<String, Object> args)
    {
        return argumentsContainNoLocal(args) || argumentsContainJMSSelector(args);
    }


    public static void removeFilters(final Map<String, Object> args)
    {
        args.remove(AMQPFilterTypes.JMS_SELECTOR.toString());
        args.remove(AMQPFilterTypes.NO_LOCAL.toString());
    }



    static boolean argumentsContainNoLocal(final Map<String, Object> args)
    {
        return args != null
                && args.containsKey(AMQPFilterTypes.NO_LOCAL.toString())
                && Boolean.TRUE.equals(args.get(AMQPFilterTypes.NO_LOCAL.toString()));
    }

    static boolean argumentsContainJMSSelector(final Map<String,Object> args)
    {
        return args != null && (args.get(AMQPFilterTypes.JMS_SELECTOR.toString()) instanceof String)
                       && ((String)args.get(AMQPFilterTypes.JMS_SELECTOR.toString())).trim().length() != 0;
    }

    public static FilterManager createMessageFilter(final Map<String,Object> args, MessageDestination queue) throws AMQInvalidArgumentException
    {
        FilterManager filterManager = null;
        if(argumentsContainNoLocal(args) && queue instanceof Queue)
        {
            filterManager = new FilterManager();
            filterManager.add(AMQPFilterTypes.NO_LOCAL.toString(), new NoLocalFilter((Queue<?>) queue));
        }

        if(argumentsContainJMSSelector(args))
        {
            if(filterManager == null)
            {
                filterManager = new FilterManager();
            }
            filterManager.add(AMQPFilterTypes.JMS_SELECTOR.toString(),createJMSSelectorFilter(args));
        }
        return filterManager;

    }

    @PluggableService
    public static final class NoLocalFilter implements MessageFilter
    {
        private final Queue<?> _queue;

        private NoLocalFilter(Queue<?> queue)
        {
            _queue = queue;
        }

        @Override
        public String getName()
        {
            return AMQPFilterTypes.NO_LOCAL.toString();
        }

        @Override
        public boolean matches(Filterable message)
        {

            final Collection<QueueConsumer<?,?>> consumers = _queue.getConsumers();
            for(QueueConsumer<?,?> c : consumers)
            {
                if(c.getSession().getConnectionReference() == message.getConnectionReference())
                {
                    return false;
                }
            }
            return !consumers.isEmpty();
        }

        @Override
        public boolean startAtTail()
        {
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            NoLocalFilter that = (NoLocalFilter) o;

            return _queue == null ? that._queue == null : _queue.equals(that._queue);
        }

        @Override
        public int hashCode()
        {
            return _queue != null ? _queue.hashCode() : 0;
        }

        @Override
        public String toString()
        {
            return "NoLocalFilter[]";
        }
    }

}
