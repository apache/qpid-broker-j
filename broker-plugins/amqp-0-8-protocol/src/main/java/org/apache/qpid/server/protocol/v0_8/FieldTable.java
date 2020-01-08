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

import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class FieldTable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldTable.class);
    private static final String STRICT_AMQP_NAME = "_strictAMQP";
    static boolean _strictAMQP = Boolean.valueOf(System.getProperty(STRICT_AMQP_NAME, "false"));

    private final FieldTableSupport _fieldTableSupport;

    FieldTable(QpidByteBuffer input, int len)
    {
        final QpidByteBuffer encodedForm = input.view(0, len);
        input.position(input.position() + len);
        _fieldTableSupport = new ByteBufferFieldTableSupport(encodedForm);
    }

    FieldTable(QpidByteBuffer buffer)
    {
        _fieldTableSupport = new ByteBufferFieldTableSupport(buffer.duplicate());
    }

    FieldTable(Map<String, Object> properties)
    {
        final Map<String, AMQTypedValue> m;
        if (properties != null && !properties.isEmpty())
        {
            m = properties.entrySet()
                          .stream()
                          .peek(e -> checkPropertyName(e.getKey()))
                          .collect(Collectors.toMap(Map.Entry::getKey,
                                                    e -> getAMQTypeValue(e.getValue()),
                                                    (x, y) -> y,
                                                    LinkedHashMap::new));
        }
        else
        {
            m = Collections.emptyMap();
        }

        _fieldTableSupport = new MapFieldTableSupport(m);
    }

    FieldTable(FieldTableSupport fieldTableSupport)
    {
        _fieldTableSupport = new MapFieldTableSupport(fieldTableSupport.getAsMap());
    }

    private static AMQTypedValue getAMQTypeValue(final Object object) throws AMQPInvalidClassException
    {
        if (object == null)
        {
            return AMQType.VOID.asTypedValue(null);
        }
        else if (object instanceof Boolean)
        {
            return AMQType.BOOLEAN.asTypedValue(object);
        }
        else if (object instanceof Byte)
        {
            return AMQType.BYTE.asTypedValue(object);
        }
        else if (object instanceof Short)
        {
            return AMQType.SHORT.asTypedValue(object);
        }
        else if (object instanceof Integer)
        {
            return AMQTypedValue.createAMQTypedValue((int) object);
        }
        else if (object instanceof Long)
        {
            return AMQTypedValue.createAMQTypedValue((long) object);
        }
        else if (object instanceof Float)
        {
            return AMQType.FLOAT.asTypedValue(object);
        }
        else if (object instanceof Double)
        {
            return AMQType.DOUBLE.asTypedValue(object);
        }
        else if (object instanceof String)
        {
            return AMQType.LONG_STRING.asTypedValue(object);
        }
        else if (object instanceof Character)
        {
            return AMQType.ASCII_CHARACTER.asTypedValue(object);
        }
        else if (object instanceof FieldTable)
        {
            return AMQType.FIELD_TABLE.asTypedValue(object);
        }
        else if (object instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            return AMQType.FIELD_TABLE.asTypedValue(FieldTable.convertToFieldTable(map));
        }
        else if (object instanceof Collection)
        {
            return AMQType.FIELD_ARRAY.asTypedValue(object);
        }
        else if (object instanceof Date)
        {
            return AMQType.TIMESTAMP.asTypedValue(((Date) object).getTime());
        }
        else if (object instanceof BigDecimal)
        {
            final BigDecimal decimal = (BigDecimal) object;
            if (decimal.longValue() > Integer.MAX_VALUE)
            {
                throw new UnsupportedOperationException(String.format("AMQP does not support decimals larger than %d",
                                                                      Integer.MAX_VALUE));
            }

            if (decimal.scale() > Byte.MAX_VALUE)
            {
                throw new UnsupportedOperationException(String.format(
                        "AMQP does not support decimal scales larger than %d",
                        Byte.MAX_VALUE));
            }

            return AMQType.DECIMAL.asTypedValue(decimal);
        }
        else if (object instanceof byte[])
        {
            return AMQType.BINARY.asTypedValue(object);
        }
        else if (object instanceof UUID)
        {
            return AMQType.LONG_STRING.asTypedValue(object.toString());
        }

        throw new AMQPInvalidClassException(AMQPInvalidClassException.INVALID_OBJECT_MSG + object.getClass());
    }

    @Override
    public String toString()
    {
        return _fieldTableSupport.toString();
    }

    private static void checkPropertyName(String propertyName)
    {
        if (propertyName == null)
        {
            throw new IllegalArgumentException("Property name must not be null");
        }
        else if (propertyName.length() == 0)
        {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        if (_strictAMQP)
        {
            checkIdentiferFormat(propertyName);
        }
    }

    private static void checkIdentiferFormat(String propertyName)
    {
        // AMQP Spec: 4.2.5.5 Field Tables
        // Guidelines for implementers:
        // * Field names MUST start with a letter, '$' or '#' and may continue with
        // letters, '$' or '#', digits, or underlines, to a maximum length of 128
        // characters.
        // * The server SHOULD validate field names and upon receiving an invalid
        // field name, it SHOULD signal a connection exception with reply code
        // 503 (syntax error). Conformance test: amq_wlp_table_01.
        // * A peer MUST handle duplicate fields by using only the first instance.

        // AMQP length limit
        if (propertyName.length() > 128)
        {
            throw new IllegalArgumentException("AMQP limits property names to 128 characters");
        }

        // AMQ start character
        if (!(Character.isLetter(propertyName.charAt(0)) || (propertyName.charAt(0) == '$')
              || (propertyName.charAt(0) == '#') || (propertyName.charAt(0)
                                                     == '_'))) // Not official AMQP added for JMS.
        {
            throw new IllegalArgumentException("Identifier '" + propertyName
                                               + "' does not start with a valid AMQP start character");
        }
    }

    public void writeToBuffer(QpidByteBuffer buffer)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("FieldTable::writeToBuffer: Writing encoded length of " + getEncodedSize() + "...");
            if (_fieldTableSupport instanceof MapFieldTableSupport)
            {
                LOGGER.debug(_fieldTableSupport.toString());
            }
        }

        buffer.putUnsignedInt(getEncodedSize());

        _fieldTableSupport.writeToBuffer(buffer);
    }

    public byte[] getDataAsBytes()
    {
        return _fieldTableSupport.getAsBytes();
    }

    public long getEncodedSize()
    {
        return _fieldTableSupport.getEncodedSize();
    }

    private static long recalculateEncodedSize(final Map<String, AMQTypedValue> properties)
    {
        long size = 0L;
        for (Map.Entry<String, AMQTypedValue> e : properties.entrySet())
        {
            String key = e.getKey();
            AMQTypedValue value = e.getValue();
            size += EncodingUtils.encodedShortStringLength(key) + 1 + value.getEncodingSize();
        }
        return size;
    }

    public static Map<String, Object> convertToMap(final FieldTable fieldTable)
    {
        if (fieldTable != null)
        {
            final Map<String, Object> map = new LinkedHashMap<>();
            Map<String, AMQTypedValue> properties = fieldTable.getProperties();
            if (properties != null)
            {
                for (Map.Entry<String, AMQTypedValue> e : properties.entrySet())
                {
                    Object val = e.getValue().getValue();
                    if (val instanceof AMQShortString)
                    {
                        val = val.toString();
                    }
                    else if (val instanceof FieldTable)
                    {
                        val = FieldTable.convertToMap((FieldTable) val);
                    }
                    map.put(e.getKey(), val);
                }
            }
            return map;
        }
        return Collections.emptyMap();
    }

    public void dispose()
    {
        _fieldTableSupport.dispose();
    }

    public int size()
    {
        return getProperties().size();
    }

    public boolean isEmpty()
    {
        return getEncodedSize() > 0;
    }

    public boolean containsKey(String key)
    {
        return _fieldTableSupport.containsKey(key);
    }

    public Set<String> keys()
    {
        return new LinkedHashSet<>(getProperties().keySet());
    }

    public Object get(String key)
    {
        checkPropertyName(key);
        return _fieldTableSupport.get(key);
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

        final FieldTable that = (FieldTable) o;

        return _fieldTableSupport.equals(that._fieldTableSupport);
    }

    @Override
    public int hashCode()
    {
        return _fieldTableSupport.hashCode();
    }

    private Map<String, AMQTypedValue> getProperties()
    {
        return _fieldTableSupport.getAsMap();
    }

    public static FieldTable convertToFieldTable(Map<String, Object> map)
    {
        if (map != null)
        {
            return new FieldTable(map);
        }
        else
        {
            return null;
        }
    }

    public static FieldTable convertToDecodedFieldTable(final FieldTable fieldTable)
    {
        if (fieldTable == null)
        {
            return null;
        }

        return new FieldTable(fieldTable._fieldTableSupport);
    }

    public void validate()
    {
        _fieldTableSupport.validate();
    }

    interface FieldTableSupport
    {
        Object get(String key);

        boolean containsKey(String key);

        long getEncodedSize();

        void writeToBuffer(QpidByteBuffer buffer);

        byte[] getAsBytes();

        Map<String, AMQTypedValue> getAsMap();

        void dispose();

        void validate();
    }

    static class ByteBufferFieldTableSupport implements FieldTableSupport
    {
        private static final AMQTypedValue NOT_PRESENT = AMQType.VOID.asTypedValue(null);

        private final QpidByteBuffer _encodedForm;
        private volatile SoftReference<Map<String, AMQTypedValue>> _cache;

        ByteBufferFieldTableSupport(final QpidByteBuffer encodedForm)
        {
            _encodedForm = encodedForm;
            _cache = new SoftReference<>(new LinkedHashMap<>());
        }

        @Override
        public synchronized long getEncodedSize()
        {
            return _encodedForm.remaining();
        }

        @Override
        public synchronized Object get(final String key)
        {
            final AMQTypedValue value = getValue(key);
            if (value != null && value != NOT_PRESENT)
            {
                return value.getValue();
            }
            else
            {
                return null;
            }
        }

        @Override
        public boolean containsKey(final String key)
        {
            final AMQTypedValue value = getValue(key);
            return value != null && value != NOT_PRESENT;
        }

        @Override
        public synchronized void writeToBuffer(final QpidByteBuffer buffer)
        {
            byte[] encodedCopy = new byte[_encodedForm.remaining()];
            _encodedForm.copyTo(encodedCopy);
            buffer.put(encodedCopy);
        }

        @Override
        public synchronized byte[] getAsBytes()
        {
            byte[] encodedCopy = new byte[_encodedForm.remaining()];
            _encodedForm.copyTo(encodedCopy);
            return encodedCopy;
        }

        @Override
        public Map<String, AMQTypedValue> getAsMap()
        {
            return decode();
        }

        @Override
        public synchronized void dispose()
        {
            if (_encodedForm != null)
            {
                _encodedForm.dispose();
               _cache.clear();
            }
        }

        @Override
        public void validate()
        {
            decode();
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

            final ByteBufferFieldTableSupport that = (ByteBufferFieldTableSupport) o;

            return _encodedForm.equals(that._encodedForm);
        }

        @Override
        public int hashCode()
        {
            return _encodedForm.hashCode();
        }

        @Override
        public String toString()
        {
            return getAsMap().toString();
        }


        private synchronized AMQTypedValue getValue(final String key)
        {
            AMQTypedValue value = null;
            Map<String, AMQTypedValue> properties = _cache.get();
            if (properties == null)
            {
                _cache = new SoftReference<>(new LinkedHashMap<>());
                properties = _cache.get();
            }
            if (properties != null)
            {
                value = properties.get(key);
            }
            if (value == null)
            {
                value = findValueForKey(key);
                if (value == null)
                {
                    value = NOT_PRESENT;
                }
                if (properties != null)
                {
                    properties.put(key, value);
                }
            }
            return value;
        }

        private AMQTypedValue findValueForKey(String key)
        {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            _encodedForm.mark();
            try
            {
                while (_encodedForm.hasRemaining())
                {
                    final byte[] bytes = AMQShortString.readAMQShortStringAsBytes(_encodedForm);
                    if (Arrays.equals(keyBytes, bytes))
                    {
                        return AMQTypedValue.readFromBuffer(_encodedForm);
                    }
                    else
                    {
                        AMQType type = AMQTypeMap.getType(_encodedForm.get());
                        type.skip(_encodedForm);
                    }
                }
            }
            finally
            {
                _encodedForm.reset();
            }
            return null;
        }

        private synchronized Map<String, AMQTypedValue> decode()
        {
            final Map<String, AMQTypedValue> properties = new HashMap<>();
            final long encodedSize = getEncodedSize();
            if (encodedSize > 0)
            {
                _encodedForm.mark();
                try
                {
                    do
                    {
                        final String key = AMQShortString.readAMQShortStringAsString(_encodedForm);

                        checkPropertyName(key);
                        AMQTypedValue value = AMQTypedValue.readFromBuffer(_encodedForm);
                        properties.put(key, value);
                    }
                    while (_encodedForm.hasRemaining());
                }
                finally
                {
                    _encodedForm.reset();
                }

                final long recalculateEncodedSize = recalculateEncodedSize(properties);
                if (encodedSize != recalculateEncodedSize)
                {
                    throw new IllegalStateException(String.format(
                            "Malformed field table detected: provided encoded size '%d' does not equal calculated size '%d'",
                            encodedSize,
                            recalculateEncodedSize));
                }
            }
            return properties;
        }
    }

    static class MapFieldTableSupport implements FieldTableSupport
    {
        private final Map<String, AMQTypedValue> _properties;
        private final long _encodedSize;

        MapFieldTableSupport(final Map<String, AMQTypedValue> properties)
        {
            _properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
            _encodedSize = recalculateEncodedSize(properties);
        }

        @Override
        public long getEncodedSize()
        {
            return _encodedSize;
        }

        @Override
        public Object get(final String key)
        {
            final AMQTypedValue value = _properties.get(key);
            if (value == null)
            {
                return null;
            }
            return value.getValue();
        }

        @Override
        public boolean containsKey(final String key)
        {
            return _properties.containsKey(key);
        }

        @Override
        public void writeToBuffer(final QpidByteBuffer buffer)
        {
            for (final Map.Entry<String, AMQTypedValue> me : _properties.entrySet())
            {
                EncodingUtils.writeShortStringBytes(buffer, me.getKey());
                me.getValue().writeToBuffer(buffer);
            }
        }

        @Override
        public byte[] getAsBytes()
        {
            byte[] data = new byte[(int) getEncodedSize()];
            QpidByteBuffer buf = QpidByteBuffer.wrap(data);
            writeToBuffer(buf);
            return data;
        }

        @Override
        public Map<String, AMQTypedValue> getAsMap()
        {
            return _properties;
        }

        @Override
        public void dispose()
        {
            // noop
        }

        @Override
        public void validate()
        {
            // noop
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

            final MapFieldTableSupport that = (MapFieldTableSupport) o;

            if (_encodedSize != that._encodedSize)
            {
                return false;
            }
            return _properties.equals(that._properties);
        }

        @Override
        public int hashCode()
        {
            int result = _properties.hashCode();
            result = 31 * result + (int) (_encodedSize ^ (_encodedSize >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return _properties.toString();
        }
    }

}
