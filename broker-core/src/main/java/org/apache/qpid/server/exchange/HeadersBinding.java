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
package org.apache.qpid.server.exchange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.FilterSupport;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.model.Binding;

/**
 * Defines binding and matching based on a set of headers.
 */
class HeadersBinding
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HeadersBinding.class);

    private final Map<String,Object> _mappings;
    private final AbstractExchange.BindingIdentifier _binding;
    private final Set<String> required = new HashSet<>();
    private final Map<String,Object> matches = new HashMap<>();
    private final String _replacementRoutingKey;
    private boolean matchAny;
    private FilterManager _filter;

    /**
     * Creates a header binding for a set of mappings. Those mappings whose value is
     * null or the empty string are assumed only to be required headers, with
     * no constraint on the value. Those with a non-null value are assumed to
     * define a required match of value.
     *
     * @param binding the binding to create a header binding using
     */
    public HeadersBinding(AbstractExchange.BindingIdentifier binding, Map<String,Object> arguments)
            throws AMQInvalidArgumentException
    {
        _binding = binding;
        arguments = arguments == null ? Map.of() : arguments;
        if(_binding != null)
        {
            _mappings = arguments;
            initMappings();
        }
        else
        {
            _mappings = null;
        }
        Object key = arguments.get(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY);
        _replacementRoutingKey = key == null ? null : String.valueOf(key);
    }

    private void initMappings() throws AMQInvalidArgumentException
    {
        if(FilterSupport.argumentsContainFilter(_mappings))
        {
            _filter = FilterSupport.createMessageFilter(_mappings, _binding.getDestination());
        }
        for(Map.Entry<String, Object> entry : _mappings.entrySet())
        {
            String propertyName = entry.getKey();
            Object value = entry.getValue();
            if (isSpecial(propertyName))
            {
                processSpecial(propertyName, value);
            }
            else if (value == null || value.equals(""))
            {
                required.add(propertyName);
            }
            else
            {
                matches.put(propertyName,value);
            }
        }
    }

    public AbstractExchange.BindingIdentifier getBinding()
    {
        return _binding;
    }

    /**
     * Checks whether the supplied headers match the requirements of this binding
     * @param headers the headers to check
     * @return true if the headers define any required keys and match any required
     * values
     */
    public boolean matches(AMQMessageHeader headers)
    {
        if(headers == null)
        {
            return required.isEmpty() && matches.isEmpty();
        }
        else
        {
            return matchAny ? or(headers) : and(headers);
        }
    }

    public boolean matches(Filterable message)
    {
        return matches(message.getMessageHeader()) && (_filter == null || _filter.allAllow(message));
    }

    private boolean and(AMQMessageHeader headers)
    {
        if(headers.containsHeaders(required))
        {
            for(Map.Entry<String, Object> e : matches.entrySet())
            {
                if(!e.getValue().equals(headers.getHeader(e.getKey())))
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }


    private boolean or(final AMQMessageHeader headers)
    {
        if(required.isEmpty())
        {
            return  matches.isEmpty() || passesMatchesOr(headers);
        }
        else
        {
            if(!passesRequiredOr(headers))
            {
                return !matches.isEmpty() && passesMatchesOr(headers);
            }
            else
            {
                return true;
            }

        }
    }

    private boolean passesMatchesOr(AMQMessageHeader headers)
    {
        for(Map.Entry<String,Object> entry : matches.entrySet())
        {
            if(headers.containsHeader(entry.getKey())
               && ((entry.getValue() == null && headers.getHeader(entry.getKey()) == null)
                   || (entry.getValue().equals(headers.getHeader(entry.getKey())))))
            {
                return true;
            }
        }
        return false;
    }

    private boolean passesRequiredOr(AMQMessageHeader headers)
    {
        for(String name : required)
        {
            if(headers.containsHeader(name))
            {
                return true;
            }
        }
        return false;
    }

    private void processSpecial(String key, Object value)
    {
        if("X-match".equalsIgnoreCase(key))
        {
            matchAny = isAny(value);
        }
        else
        {
            LOGGER.warn("Ignoring special header: " + key);
        }
    }

    private boolean isAny(Object value)
    {
        if(value instanceof String)
        {
            if("any".equalsIgnoreCase((String) value))
            {
                return true;
            }
            if("all".equalsIgnoreCase((String) value))
            {
                return false;
            }
        }
        LOGGER.warn("Ignoring unrecognised match type: " + value);
        return false;//default to all
    }

    static boolean isSpecial(Object key)
    {
        return key instanceof String && isSpecial((String) key);
    }

    static boolean isSpecial(String key)
    {
        return key.startsWith("X-") || key.startsWith("x-");
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final HeadersBinding hb = (HeadersBinding) o;

        if(_binding == null)
        {
            if(hb.getBinding() != null)
            {
                return false;
            }
        }
        else if (!_binding.equals(hb.getBinding()))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return _binding == null ? 0 : _binding.hashCode();
    }

    public String getReplacementRoutingKey()
    {
        return _replacementRoutingKey;
    }

    private static class ExcludeAllFilter implements MessageFilter
    {
        @Override
        public String getName()
        {
            return "";
        }

        @Override
        public boolean startAtTail()
        {
            return false;
        }

        @Override
        public boolean matches(Filterable message)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "ExcludeAllFilter[]";
        }
    }
}
