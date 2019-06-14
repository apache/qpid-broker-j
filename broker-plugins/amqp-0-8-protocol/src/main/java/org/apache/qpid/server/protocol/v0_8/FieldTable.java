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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class FieldTable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldTable.class);
    private static final String STRICT_AMQP_NAME = "STRICT_AMQP";
    private static final boolean STRICT_AMQP = Boolean.valueOf(System.getProperty(STRICT_AMQP_NAME, "false"));
    private static final AMQTypedValue NOT_PRESENT = AMQType.VOID.asTypedValue(null);

    public static final FieldTable EMPTY = FieldTable.convertToFieldTable(Collections.emptyMap());

    private QpidByteBuffer _encodedForm;
    private boolean _decoded;
    private final Map<String, AMQTypedValue> _properties;
    private final long _encodedSize;
    private final boolean _strictAMQP;

    FieldTable(QpidByteBuffer input, int len)
    {
        _strictAMQP = STRICT_AMQP;
        _encodedForm = input.view(0,len);
        input.position(input.position()+len);
        _encodedSize = len;
        _properties = new LinkedHashMap<>();
    }

    FieldTable(QpidByteBuffer buffer)
    {
        _strictAMQP = STRICT_AMQP;
        _encodedForm = buffer.duplicate();
        _encodedSize = buffer.remaining();
        _properties = new LinkedHashMap<>();
    }

    FieldTable(Map<String, Object> properties)
    {
        this(properties, STRICT_AMQP);
    }

    FieldTable(Map<String, Object> properties, boolean strictAMQP)
    {
        _strictAMQP = strictAMQP;
        long size = 0;
        Map<String, AMQTypedValue> m = new LinkedHashMap<>();
        if (properties != null && !properties.isEmpty())
        {
            m = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : properties.entrySet())
            {
                String key = e.getKey();
                Object val = e.getValue();
                checkPropertyName(key);
                AMQTypedValue value = getAMQTypeValue(val);
                size += EncodingUtils.encodedShortStringLength(e.getKey()) + 1 + value.getEncodingSize();
                m.put(e.getKey(), value);
            }
        }
        _properties = m;
        _encodedSize = size;
        _decoded = true;
    }

    private synchronized AMQTypedValue getProperty(String key)
    {
        AMQTypedValue value = _properties.get(key);
        if (value == null && !_decoded)
        {
            value = findValueForKey(key);
            _properties.put(key, value);
        }
        return value;
    }

    private Map<String, AMQTypedValue> decode()
    {
        final Map<String, AMQTypedValue> properties = new HashMap<>();
        if (_encodedSize > 0 && _encodedForm != null)
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
            if (_encodedSize != recalculateEncodedSize)
            {
                throw new IllegalStateException(String.format(
                        "Malformed field table detected: provided encoded size '%d' does not equal calculated size '%d'",
                        _encodedSize,
                        recalculateEncodedSize));
            }
        }
        return properties;
    }

    private void decodeIfNecessary()
    {
        if (!_decoded)
        {
            try
            {
                final Map<String, AMQTypedValue> properties = decode();
                if (!_properties.isEmpty())
                {
                    _properties.clear();
                }
                _properties.putAll(properties);
            }
            finally
            {
                _decoded = true;
            }
        }
    }

    private AMQTypedValue getAMQTypeValue(final Object object) throws AMQPInvalidClassException
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

    // ***** Methods

    @Override
    public String toString()
    {
        return getProperties().toString();
    }

    private void checkPropertyName(String propertyName)
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

    // *************************  Byte Buffer Processing

    public synchronized void writeToBuffer(QpidByteBuffer buffer)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("FieldTable::writeToBuffer: Writing encoded length of " + getEncodedSize() + "...");
            if (_decoded)
            {
                LOGGER.debug(getProperties().toString());
            }
        }

        buffer.putUnsignedInt(getEncodedSize());

        putDataInBuffer(buffer);
    }

    public synchronized byte[] getDataAsBytes()
    {
        if (_encodedForm == null)
        {
            byte[] data = new byte[(int) getEncodedSize()];
            QpidByteBuffer buf = QpidByteBuffer.wrap(data);
            putDataInBuffer(buf);
            return data;
        }
        else
        {
            byte[] encodedCopy = new byte[_encodedForm.remaining()];
            _encodedForm.copyTo(encodedCopy);
            return encodedCopy;
        }
    }

    public long getEncodedSize()
    {
        return _encodedSize;
    }

    private synchronized long recalculateEncodedSize(final Map<String, AMQTypedValue> properties)
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
        final Map<String, Object> map = new HashMap<>();

        if (fieldTable != null)
        {
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
        }
        return map;
    }

    public synchronized void clearEncodedForm()
    {
        try
        {
            decodeIfNecessary();
        }
        finally
        {
            if (_encodedForm != null)
            {
                _encodedForm.dispose();
                _encodedForm = null;
            }
        }
    }

    public synchronized void dispose()
    {
        if (_encodedForm != null)
        {
            _encodedForm.dispose();
            _encodedForm = null;
        }
        _properties.clear();
    }

    public int size()
    {
        return getProperties().size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean containsKey(String key)
    {
        return getProperties().containsKey(key);
    }

    public Set<String> keys()
    {
        return new LinkedHashSet<>(getProperties().keySet());
    }

    public Object get(String key)
    {
        checkPropertyName(key);
        AMQTypedValue value = getProperty(key);
        if (value != null && value != NOT_PRESENT)
        {
            return value.getValue();
        }
        else
        {
            return null;
        }
    }

    private void putDataInBuffer(QpidByteBuffer buffer)
    {
        if (_encodedForm != null)
        {
            byte[] encodedCopy = new byte[_encodedForm.remaining()];
            _encodedForm.copyTo(encodedCopy);

            buffer.put(encodedCopy);
        }
        else if (!_properties.isEmpty())
        {
            for (final Map.Entry<String, AMQTypedValue> me : _properties.entrySet())
            {
                EncodingUtils.writeShortStringBytes(buffer, me.getKey());
                me.getValue().writeToBuffer(buffer);
            }
        }
    }


    @Override
    public int hashCode()
    {
        return getProperties().hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        FieldTable f = (FieldTable) o;
        return getProperties().equals(f.getProperties());
    }

    private synchronized Map<String, AMQTypedValue> getProperties()
    {
        decodeIfNecessary();
        return _properties;
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
        return NOT_PRESENT;
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

    public synchronized void validate()
    {
        if (!_decoded)
        {
            decode();
        }
    }
}
