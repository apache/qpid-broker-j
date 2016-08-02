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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class FixedKeyMapCreator
{
    private final String[] _keys;
    private final int[] _keyHashCodes;
    private final Set<String> _keySet = new AbstractSet<String>()
    {
        @Override
        public Iterator<String> iterator()
        {
            return new KeysIterator();
        }

        @Override
        public int size()
        {
            return _keys.length;
        }
    };

    public FixedKeyMapCreator(final String... keys)
    {
        _keys = keys;
        _keyHashCodes = new int[keys.length];

        Set<String> uniqueKeys = new HashSet<>(Arrays.asList(keys));
        if(uniqueKeys.size() != keys.length)
        {
            List<String> duplicateKeys = new ArrayList<>(Arrays.asList(keys));
            duplicateKeys.removeAll(uniqueKeys);
            throw new IllegalArgumentException("The supplied keys must be unique, but the following keys are duplicated: " + duplicateKeys);
        }
        for(int i = 0; i < keys.length; i++)
        {
            _keyHashCodes[i] = keys[i].hashCode();
        }

    }

    public Map<String,Object> createMap(Object... values)
    {
        if(values.length != _keys.length)
        {
            throw new IllegalArgumentException("There are " + _keys.length + " keys, so that many values must be supplied");
        }
        return new FixedKeyMap(values);
    }

    private final class FixedKeyMap extends AbstractMap<String,Object>
    {
        private final Object[] _values;

        private FixedKeyMap(final Object[] values)
        {
            _values = values;
        }

        @Override
        public Object get(final Object key)
        {
            int keyHashCode = key.hashCode();
            for(int i = 0; i < _keys.length; i++)
            {
                if(_keyHashCodes[i] == keyHashCode && _keys[i].equals(key))
                {
                    return _values[i];
                }
            }
            return null;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public boolean containsValue(final Object value)
        {
            if(value == null)
            {
                for(Object o : _values)
                {
                    if(o == null)
                    {
                        return true;
                    }
                }
            }
            else
            {
                for (Object o : _values)
                {
                    if (value.equals(o))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public boolean containsKey(final Object key)
        {
            int keyHashCode = key.hashCode();
            for(int i = 0; i < _keys.length; i++)
            {
                if(_keyHashCodes[i] == keyHashCode && _keys[i].equals(key))
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Object put(final String key, final Object value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove(final Object key)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(final Map<? extends String, ?> m)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet()
        {
            return _keySet;
        }

        @Override
        public Collection<Object> values()
        {
            return Collections.unmodifiableCollection(Arrays.asList(_values));
        }

        @Override
        public int size()
        {
            return _values.length;
        }

        @Override
        public Set<Entry<String, Object>> entrySet()
        {
            return new EntrySet();
        }

        private class EntrySet extends AbstractSet<Entry<String,Object>>
        {
            @Override
            public Iterator<Entry<String, Object>> iterator()
            {
                return new EntrySetIterator();
            }

            @Override
            public int size()
            {
                return _keys.length;
            }
        }

        private class EntrySetIterator implements Iterator<Entry<String, Object>>
        {
            private int _position = 0;
            @Override
            public boolean hasNext()
            {
                return _position < _keys.length;
            }

            @Override
            public Entry<String, Object> next()
            {
                try
                {
                    final String key = _keys[_position];
                    final Object value = _values[_position++];
                    return new FixedKeyEntry(key, value);
                }
                catch (ArrayIndexOutOfBoundsException e)
                {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }


        }
    }
    private static class FixedKeyEntry implements Map.Entry<String, Object>
    {
        private final String _key;
        private final Object _value;

        private FixedKeyEntry(final String key, final Object value)
        {
            _key = key;
            _value = value;
        }

        @Override
        public String getKey()
        {
            return _key;
        }

        @Override
        public Object getValue()
        {
            return _value;
        }

        @Override
        public Object setValue(final Object value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(final Object o)
        {
            if(this == o)
            {
                return true;
            }
            else if(o instanceof Map.Entry)
            {
                Map.Entry e2 = (Map.Entry) o;
                return _key.equals(e2.getKey())
                       && (_value == null ? e2.getValue() == null
                        : _value.equals(e2.getValue()));
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return _key.hashCode() ^ (_value == null ? 0 : _value.hashCode());
        }
    }

    private class KeysIterator implements Iterator<String>
    {
        private int _position = 0;
        @Override
        public boolean hasNext()
        {
            return _position < _keys.length;
        }

        @Override
        public String next()
        {
            try
            {
                return _keys[_position++];
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
