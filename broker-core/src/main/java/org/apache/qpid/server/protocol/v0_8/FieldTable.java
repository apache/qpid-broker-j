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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
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
    private static final String STRICT_AMQP_NAME = "STRICT_AMQP";
    private static final boolean STRICT_AMQP = Boolean.valueOf(System.getProperty(STRICT_AMQP_NAME, "false"));

    private QpidByteBuffer _encodedForm;
    private Map<String, AMQTypedValue> _properties = null;
    private long _encodedSize;
    private static final int INITIAL_HASHMAP_CAPACITY = 16;
    private final boolean _strictAMQP;

    public FieldTable()
    {
        this(STRICT_AMQP);
    }


    public FieldTable(boolean strictAMQP)
    {
        super();
        _strictAMQP = strictAMQP;
    }

    public FieldTable(FieldTable other)
    {
        _encodedForm = other._encodedForm;
        _encodedSize = other._encodedSize;
        _strictAMQP = other._strictAMQP;
        if(other._properties != null)
        {
            _properties = new LinkedHashMap<>(other._properties);
        }
    }

    public FieldTable(QpidByteBuffer input, int len)
    {
        this();
        _encodedForm = input.view(0,len);
        input.position(input.position()+len);
        _encodedSize = len;
    }

    public FieldTable(QpidByteBuffer buffer)
    {
        this();
        _encodedForm = buffer.duplicate();
        _encodedSize = buffer.remaining();
    }

    public synchronized boolean isClean()
    {
        return _encodedForm != null;
    }

    public AMQTypedValue getProperty(AMQShortString string)
    {
        return getProperty(AMQShortString.toString(string));
    }

    private AMQTypedValue getProperty(String string)
    {
        checkPropertyName(string);

        synchronized (this)
        {
            if (_properties == null)
            {
                if (_encodedForm == null)
                {
                    return null;
                }
                else
                {
                    populateFromBuffer();
                }
            }
        }

        if (_properties == null)
        {
            return null;
        }
        else
        {
            return _properties.get(string);
        }
    }

    private void populateFromBuffer()
    {
        if (_encodedSize > 0)
        {
            _properties = decode();
        }
    }

    private Map<String, AMQTypedValue> decode()
    {
        final Map<String, AMQTypedValue> properties = new LinkedHashMap<>(INITIAL_HASHMAP_CAPACITY);

        _encodedForm.mark();
        try
        {
            do
            {
                final String key = AMQShortString.readAMQShortStringAsString(_encodedForm);
                AMQTypedValue value = AMQTypedValue.readFromBuffer(_encodedForm);
                properties.put(key, value);
            }
            while (_encodedForm.hasRemaining());
        }
        finally
        {
            _encodedForm.reset();
        }

        final long recalculateEncodedSize = calculateEncodedSize(properties);
        if (_encodedSize != recalculateEncodedSize)
        {
            throw new IllegalStateException(String.format(
                    "Malformed field table detected: provided encoded size '%d' does not equal calculated size '%d'",
                    _encodedSize,
                    recalculateEncodedSize));
        }
        return properties;
    }

    private AMQTypedValue setProperty(String key, AMQTypedValue val)
    {
        checkPropertyName(key);

        synchronized (this)
        {
            initMapIfNecessary();
            if (_properties.containsKey(key))
            {
                _encodedForm = null;

                if (val == null)
                {
                    return removeKey(key);
                }
            }
            else if ((_encodedForm != null) && (val != null))
            {
                // We have updated data to store in the buffer
                // So clear the _encodedForm to allow it to be rebuilt later
                // this is safer than simply appending to any existing buffer.
                _encodedForm = null;
            }
            else if (val == null)
            {
                return null;
            }
        }

        AMQTypedValue oldVal = _properties.put(key, val);
        if (oldVal != null)
        {
            _encodedSize -= oldVal.getEncodingSize();
        }
        else
        {
            _encodedSize += EncodingUtils.encodedShortStringLength(key) + 1;
        }

        _encodedSize += val.getEncodingSize();

        return oldVal;
    }

    private void initMapIfNecessary()
    {
        synchronized (this)
        {
            if (_properties == null)
            {
                if ((_encodedForm == null) || (_encodedSize == 0))
                {
                    _properties = new LinkedHashMap<>();
                }
                else
                {
                    populateFromBuffer();
                }
            }

        }
    }

    public Boolean getBoolean(AMQShortString string)
    {
        return getBoolean(AMQShortString.toString(string));
    }

    public Boolean getBoolean(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.BOOLEAN))
        {
            return (Boolean) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Byte getByte(AMQShortString string)
    {
        return getByte(AMQShortString.toString(string));
    }

    public Byte getByte(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.BYTE))
        {
            return (Byte) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Short getShort(AMQShortString string)
    {
        return getShort(AMQShortString.toString(string));
    }

    public Short getShort(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.SHORT))
        {
            return (Short) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Integer getInteger(AMQShortString string)
    {
        return getInteger(AMQShortString.toString(string));
    }

    public Integer getInteger(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.INT))
        {
            return (Integer) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Long getLong(AMQShortString string)
    {
        return getLong(AMQShortString.toString(string));
    }

    public Long getLong(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.LONG))
        {
            return (Long) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Float getFloat(AMQShortString string)
    {
        return getFloat(AMQShortString.toString(string));
    }

    public Float getFloat(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.FLOAT))
        {
            return (Float) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Double getDouble(AMQShortString string)
    {
        return getDouble(AMQShortString.toString(string));
    }

    public Double getDouble(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.DOUBLE))
        {
            return (Double) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public String getString(AMQShortString string)
    {
        return getString(AMQShortString.toString(string));
    }

    public String getString(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && ((value.getType() == AMQType.WIDE_STRING) || (value.getType() == AMQType.ASCII_STRING)))
        {
            return (String) value.getValue();
        }
        else if ((value != null) && (value.getValue() != null) && !(value.getValue() instanceof byte[]))
        {
            return String.valueOf(value.getValue());
        }
        else
        {
            return null;
        }

    }

    public Character getCharacter(AMQShortString string)
    {
        return getCharacter(AMQShortString.toString(string));
    }

    public Character getCharacter(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.ASCII_CHARACTER))
        {
            return (Character) value.getValue();
        }
        else
        {
            return null;
        }
    }

     public byte[] getBytes(AMQShortString string)
    {
        return getBytes(AMQShortString.toString(string));
    }

    public byte[] getBytes(String string)
    {
        AMQTypedValue value = getProperty(string);
        if ((value != null) && (value.getType() == AMQType.BINARY))
        {
            return (byte[]) value.getValue();
        }
        else
        {
            return null;
        }
    }

    /**
     * Extracts a value from the field table that is itself a FieldTable associated with the specified parameter name.
     *
     * @param string The name of the parameter to get the associated FieldTable value for.
     *
     * @return The associated FieldTable value, or <tt>null</tt> if the associated value is not of FieldTable type or
     *         not present in the field table at all.
     */
    public FieldTable getFieldTable(AMQShortString string)
    {
        return getFieldTable(AMQShortString.toString(string));
    }

    /**
     * Extracts a value from the field table that is itself a FieldTable associated with the specified parameter name.
     *
     * @param string The name of the parameter to get the associated FieldTable value for.
     *
     * @return The associated FieldTable value, or <tt>null</tt> if the associated value is not of FieldTable type or
     *         not present in the field table at all.
     */
    public FieldTable getFieldTable(String string)
    {
        AMQTypedValue value = getProperty(string);

        if ((value != null) && (value.getType() == AMQType.FIELD_TABLE))
        {
            return (FieldTable) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public Object getObject(AMQShortString string)
    {
        return getObject(AMQShortString.toString(string));
    }

    public Object getObject(String string)
    {
        AMQTypedValue value = getProperty(string);
        if (value != null)
        {
            return value.getValue();
        }
        else
        {
            return null;
        }

    }

    public Long getTimestamp(AMQShortString name)
    {
        return getTimestamp(AMQShortString.toString(name));
    }

    public Long getTimestamp(String name)
    {
        AMQTypedValue value = getProperty(name);
        if ((value != null) && (value.getType() == AMQType.TIMESTAMP))
        {
            return (Long) value.getValue();
        }
        else
        {
            return null;
        }
    }

    public BigDecimal getDecimal(AMQShortString propertyName)
    {
        return getDecimal(AMQShortString.toString(propertyName));
    }

    public BigDecimal getDecimal(String propertyName)
    {
        AMQTypedValue value = getProperty(propertyName);
        if ((value != null) && (value.getType() == AMQType.DECIMAL))
        {
            return (BigDecimal) value.getValue();
        }
        else
        {
            return null;
        }
    }

    // ************  Setters

    public Object setBoolean(AMQShortString string, Boolean b)
    {
        return setBoolean(AMQShortString.toString(string), b);
    }

    public Object setBoolean(String string, Boolean b)
    {
        return setProperty(string, AMQType.BOOLEAN.asTypedValue(b));
    }

    public Object setByte(AMQShortString string, Byte b)
    {
        return setByte(AMQShortString.toString(string), b);
    }

    public Object setByte(String string, Byte b)
    {
        return setProperty(string, AMQType.BYTE.asTypedValue(b));
    }

    public Object setShort(AMQShortString string, Short i)
    {
        return setShort(AMQShortString.toString(string), i);
    }

    public Object setShort(String string, Short i)
    {
        return setProperty(string, AMQType.SHORT.asTypedValue(i));
    }

    public Object setInteger(AMQShortString string, int i)
    {
        return setInteger(AMQShortString.toString(string), i);
    }

    public Object setInteger(String string, int i)
    {
        return setProperty(string, AMQTypedValue.createAMQTypedValue(i));
    }

    public Object setLong(AMQShortString string, long l)
    {
        return setLong(AMQShortString.toString(string), l);
    }

    public Object setLong(String string, long l)
    {
        return setProperty(string, AMQTypedValue.createAMQTypedValue(l));
    }

    public Object setFloat(AMQShortString string, Float v)
    {
        return setFloat(AMQShortString.toString(string), v);
    }

    public Object setFloat(String string, Float f)
    {
        return setProperty(string, AMQType.FLOAT.asTypedValue(f));
    }

    public Object setDouble(AMQShortString string, Double d)
    {
        return setDouble(AMQShortString.toString(string), d);
    }

    public Object setDouble(String string, Double v)
    {
        return setProperty(string, AMQType.DOUBLE.asTypedValue(v));
    }

    public Object setString(AMQShortString string, String value)
    {
        return setString(AMQShortString.toString(string), value);
    }

    public Object setString(String string, String value)
    {
        if (value == null)
        {
            return setProperty(string, AMQType.VOID.asTypedValue(null));
        }
        else
        {
            return setProperty(string, AMQType.LONG_STRING.asTypedValue(value));
        }
    }

    public Object setAsciiString(AMQShortString string, String value)
    {
        return setAsciiString(AMQShortString.toString(string), value);
    }

    public Object setAsciiString(String string, String value)
    {
        if (value == null)
        {
            return setProperty(string, AMQType.VOID.asTypedValue(null));
        }
        else
        {
            return setProperty(string, AMQType.ASCII_STRING.asTypedValue(value));
        }
    }

    public Object setChar(AMQShortString string, char c)
    {
        return setChar(AMQShortString.toString(string), c);
    }

    public Object setChar(String string, char c)
    {
        return setProperty(string, AMQType.ASCII_CHARACTER.asTypedValue(c));
    }

    public Object setFieldArray(AMQShortString string, Collection<?> collection)
    {
        return setFieldArray(AMQShortString.toString(string), collection);
    }

    public Object setFieldArray(String string, Collection<?> collection)
    {
        return setProperty(string, AMQType.FIELD_ARRAY.asTypedValue(collection));
    }

     public Object setBytes(AMQShortString string, byte[] bytes)
    {
        return setBytes(AMQShortString.toString(string), bytes);
    }

    public Object setBytes(String string, byte[] bytes)
    {
        return setProperty(string, AMQType.BINARY.asTypedValue(bytes));
    }

    public Object setBytes(AMQShortString string, byte[] bytes, int start, int length)
    {
        return setBytes(AMQShortString.toString(string), bytes, start, length);
    }

    public Object setBytes(String string, byte[] bytes, int start, int length)
    {
        byte[] newBytes = new byte[length];
        System.arraycopy(bytes, start, newBytes, 0, length);

        return setBytes(string, newBytes);
    }

    public Object setTimestamp(AMQShortString string, long datetime)
    {
        return setTimestamp(AMQShortString.toString(string), datetime);
    }

    public Object setTimestamp(String string, long datetime)
    {
        return setProperty(string, AMQType.TIMESTAMP.asTypedValue(datetime));
    }

    public Object setDecimal(AMQShortString string, BigDecimal decimal)
    {
        return setDecimal(AMQShortString.toString(string), decimal);
    }

    public Object setDecimal(String string, BigDecimal decimal)
    {
        if (decimal.longValue() > Integer.MAX_VALUE)
        {
            throw new UnsupportedOperationException("AMQP does not support decimals larger than " + Integer.MAX_VALUE);
        }

        if (decimal.scale() > Byte.MAX_VALUE)
        {
            throw new UnsupportedOperationException("AMQP does not support decimal scales larger than " + Byte.MAX_VALUE);
        }

        return setProperty(string, AMQType.DECIMAL.asTypedValue(decimal));
    }

    public Object setVoid(AMQShortString string)
    {
        return setVoid(AMQShortString.toString(string));
    }

    public Object setVoid(String string)
    {
        return setProperty(string, AMQType.VOID.asTypedValue(null));
    }

    /**
     * Associates a nested field table with the specified parameter name.
     *
     * @param string  The name of the parameter to store in the table.
     * @param ftValue The field table value to associate with the parameter name.
     *
     * @return The stored value.
     */
    public Object setFieldTable(AMQShortString string, FieldTable ftValue)
    {
        return setFieldTable(AMQShortString.toString(string), ftValue);
    }

    /**
     * Associates a nested field table with the specified parameter name.
     *
     * @param string  The name of the parameter to store in the table.
     * @param ftValue The field table value to associate with the parameter name.
     *
     * @return The stored value.
     */
    public Object setFieldTable(String string, FieldTable ftValue)
    {
        return setProperty(string, AMQType.FIELD_TABLE.asTypedValue(ftValue));
    }

    public Object setObject(AMQShortString string, Object object)
    {
        return setObject(AMQShortString.toString(string), object);
    }

    public Object setObject(String string, Object object)
    {
        if (object instanceof Boolean)
        {
            return setBoolean(string, (Boolean) object);
        }
        else if (object instanceof Byte)
        {
            return setByte(string, (Byte) object);
        }
        else if (object instanceof Short)
        {
            return setShort(string, (Short) object);
        }
        else if (object instanceof Integer)
        {
            return setInteger(string, (Integer) object);
        }
        else if (object instanceof Long)
        {
            return setLong(string, (Long) object);
        }
        else if (object instanceof Float)
        {
            return setFloat(string, (Float) object);
        }
        else if (object instanceof Double)
        {
            return setDouble(string, (Double) object);
        }
        else if (object instanceof String)
        {
            return setString(string, (String) object);
        }
        else if (object instanceof Character)
        {
            return setChar(string, (Character) object);
        }
        else if (object instanceof FieldTable)
        {
            return setFieldTable(string, (FieldTable) object);
        }
        else if (object instanceof Map)
        {
            return setFieldTable(string, FieldTable.convertToFieldTable((Map<String,Object>) object));
        }
        else if (object instanceof Collection)
        {
            return setFieldArray(string, (Collection)object);
        }
        else if (object instanceof Date)
        {
            return setTimestamp(string, ((Date) object).getTime());
        }
        else if (object instanceof BigDecimal)
        {
            return setDecimal(string, (BigDecimal) object);
        }
        else if (object instanceof byte[])
        {
            return setBytes(string, (byte[]) object);
        }
        else if (object instanceof UUID)
        {
            return setString(string, object.toString());
        }

        throw new AMQPInvalidClassException(AMQPInvalidClassException.INVALID_OBJECT_MSG + (object == null ? "null" : object.getClass()));
    }

    public boolean isNullStringValue(String name)
    {
        AMQTypedValue value = getProperty(name);

        return (value != null) && (value.getType() == AMQType.VOID);
    }

    // ***** Methods

    private Enumeration getPropertyNames()
    {
        return Collections.enumeration(keys());
    }

    public boolean propertyExists(AMQShortString propertyName)
    {
        return itemExists(AMQShortString.toString(propertyName));
    }

    public boolean propertyExists(String propertyName)
    {
        return itemExists(propertyName);
    }

    public boolean itemExists(AMQShortString propertyName)
    {
        return itemExists(AMQShortString.toString(propertyName));
    }

    private boolean itemExists(String propertyName)
    {
        checkPropertyName(propertyName);
        initMapIfNecessary();

        return _properties.containsKey(propertyName);
    }

    @Override
    public String toString()
    {
        initMapIfNecessary();

        return _properties.toString();
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

    protected static void checkIdentiferFormat(AMQShortString propertyName)
    {
        checkIdentiferFormat(AMQShortString.toString(propertyName));
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
                    || (propertyName.charAt(0) == '#') || (propertyName.charAt(0) == '_'))) // Not official AMQP added for JMS.
        {
            throw new IllegalArgumentException("Identifier '" + propertyName
                + "' does not start with a valid AMQP start character");
        }
    }

    // *************************  Byte Buffer Processing

    public void writeToBuffer(QpidByteBuffer buffer)
    {
        final boolean trace = LOGGER.isDebugEnabled();

        if (trace)
        {
            LOGGER.debug("FieldTable::writeToBuffer: Writing encoded length of " + getEncodedSize() + "...");
            if (_properties != null)
            {
                LOGGER.debug(_properties.toString());
            }
        }

        buffer.putUnsignedInt(getEncodedSize());

        putDataInBuffer(buffer);
    }


    public synchronized byte[] getDataAsBytes()
    {
        if(_encodedForm == null)
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

    private void recalculateEncodedSize()
    {

        int encodedSize = calculateEncodedSize(_properties);

        _encodedSize = encodedSize;
    }

    private int calculateEncodedSize(final Map<String, AMQTypedValue> properties)
    {
        int encodedSize = 0;
        if (properties != null)
        {
            for (Map.Entry<String, AMQTypedValue> e : properties.entrySet())
            {
                encodedSize += EncodingUtils.encodedShortStringLength(e.getKey());
                encodedSize++; // the byte for the encoding Type
                encodedSize += e.getValue().getEncodingSize();

            }
        }
        return encodedSize;
    }

    public synchronized void addAll(FieldTable fieldTable)
    {
        initMapIfNecessary();
        fieldTable.initMapIfNecessary();
        if (fieldTable._properties != null)
        {
            _encodedForm = null;
            _properties.putAll(fieldTable._properties);
            recalculateEncodedSize();
        }
    }

    public static Map<String, Object> convertToMap(final FieldTable fieldTable)
    {
        final Map<String, Object> map = new HashMap<>();

        if(fieldTable != null)
        {
            fieldTable.processOverElements(
                    new FieldTableElementProcessor()
                    {

                        @Override
                        public boolean processElement(String propertyName, AMQTypedValue value)
                        {
                            Object val = value.getValue();
                            if (val instanceof AMQShortString)
                            {
                                val = val.toString();
                            }
                            else if (val instanceof FieldTable)
                            {
                                val = FieldTable.convertToMap((FieldTable) val);
                            }
                            map.put(propertyName, val);
                            return true;
                        }

                        @Override
                        public Object getResult()
                        {
                            return map;
                        }
                    });
        }
        return map;
    }

    public void clearEncodedForm()
    {
        synchronized (this)
        {
            try
            {
                if (_properties == null)
                {
                    if (_encodedForm != null)
                    {
                        populateFromBuffer();
                    }
                }
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
    }

    public void dispose()
    {
        synchronized (this)
        {
            if (_properties == null)
            {
                if (_encodedForm != null)
                {
                    _properties = Collections.emptyMap();
                }
            }

            if (_encodedForm != null)
            {
                _encodedForm.dispose();
                _encodedForm = null;
            }
        }
    }

    public synchronized void reallocate()
    {
        _encodedForm = QpidByteBuffer.reallocateIfNecessary(_encodedForm);
    }


    public static interface FieldTableElementProcessor
    {
        public boolean processElement(String propertyName, AMQTypedValue value);

        public Object getResult();
    }

    public Object processOverElements(FieldTableElementProcessor processor)
    {
        initMapIfNecessary();
        if (_properties != null)
        {
            for (Map.Entry<String, AMQTypedValue> e : _properties.entrySet())
            {
                boolean result = processor.processElement(e.getKey(), e.getValue());
                if (!result)
                {
                    break;
                }
            }
        }

        return processor.getResult();

    }

    public int size()
    {
        initMapIfNecessary();

        return _properties.size();

    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean containsKey(AMQShortString key)
    {
        return containsKey(AMQShortString.toString(key));
    }

    public boolean containsKey(String key)
    {
        initMapIfNecessary();

        return _properties.containsKey(key);
    }

    public Set<String> keys()
    {
        initMapIfNecessary();
        return new LinkedHashSet<>(_properties.keySet());
    }

    public Iterator<Map.Entry<AMQShortString, AMQTypedValue>> iterator()
    {
        initMapIfNecessary();
        return _properties.entrySet()
                          .stream()
                          .collect(Collectors.toMap(e-> AMQShortString.valueOf(e.getKey()), Map.Entry::getValue))
                          .entrySet()
                          .iterator();
    }

    public Object get(String key)
    {
        return getObject(key);
    }

    public Object put(AMQShortString key, Object value)
    {
        return setObject(AMQShortString.toString(key), value);
    }

    public Object remove(String key)
    {
        AMQTypedValue val = removeKey(key);

        return (val == null) ? null : val.getValue();

    }

    public Object remove(AMQShortString key)
    {
        return remove(AMQShortString.toString(key));
    }

    public AMQTypedValue removeKey(AMQShortString key)
    {
        return removeKey(AMQShortString.toString(key));
    }

    private AMQTypedValue removeKey(String key)
    {
        synchronized (this)
        {
            initMapIfNecessary();
            _encodedForm = null;
        }
        AMQTypedValue value = _properties.remove(key);
        if (value == null)
        {
            return null;
        }
        else
        {
            _encodedSize -= EncodingUtils.encodedShortStringLength(key);
            _encodedSize--;
            _encodedSize -= value.getEncodingSize();

            return value;
        }

    }

    public synchronized void clear()
    {
        initMapIfNecessary();
        if (_encodedForm != null)
        {
            _encodedForm.dispose();
            _encodedForm = null;
        }
        _properties.clear();
        _encodedSize = 0;
    }



    public Set<AMQShortString> keySet()
    {
        initMapIfNecessary();

        return _properties.keySet().stream().map(k->AMQShortString.valueOf(k)).collect(Collectors.toSet());
    }

    private synchronized void putDataInBuffer(QpidByteBuffer buffer)
    {
        if (_encodedForm != null)
        {
            byte[] encodedCopy = new byte[_encodedForm.remaining()];
            _encodedForm.copyTo(encodedCopy);

            buffer.put(encodedCopy);
        }
        else if (_properties != null)
        {
            final Iterator<Map.Entry<String, AMQTypedValue>> it = _properties.entrySet().iterator();

            // If there are values then write out the encoded Size... could check _encodedSize != 0
            // write out the total length, which we have kept up to date as data is added

            while (it.hasNext())
            {
                final Map.Entry<String, AMQTypedValue> me = it.next();
                try
                {
                    // Write the actual parameter name
                    EncodingUtils.writeShortStringBytes(buffer, me.getKey());
                    me.getValue().writeToBuffer(buffer);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    @Override
    public int hashCode()
    {
        initMapIfNecessary();

        return _properties.hashCode();
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

        initMapIfNecessary();

        FieldTable f = (FieldTable) o;
        f.initMapIfNecessary();

        return _properties.equals(f._properties);
    }


    public static FieldTable convertToFieldTable(Map<String, Object> map)
    {
        if (map != null)
        {
            FieldTable table = new FieldTable();
            for(Map.Entry<String,Object> entry : map.entrySet())
            {
                table.setObject(entry.getKey(), entry.getValue());
            }

            return table;
        }
        else
        {
            return null;
        }
    }


    public synchronized void validate()
    {
        if (_properties == null && _encodedForm != null && _encodedSize > 0)
        {
            decode();
        }
    }

}
