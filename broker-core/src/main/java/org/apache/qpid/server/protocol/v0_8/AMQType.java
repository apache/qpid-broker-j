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
import java.util.Date;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

/**
 * AMQType is a type that represents the different possible AMQP field table types. It provides operations for each
 * of the types to perform tasks such as calculating the size of an instance of the type, converting types between AMQP
 * and Java native types, and reading and writing instances of AMQP types in binary formats to and from byte buffers.
 */
public enum AMQType
{
    LONG_STRING('S')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedLongStringLength((String) value);
        }

        @Override
        public String toNativeValue(Object value)
        {
            if (value != null)
            {
                return value.toString();
            }
            else
            {
                throw new NullPointerException("Cannot convert: null to String.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            EncodingUtils.writeLongStringBytes(buffer, (String) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return EncodingUtils.readLongString(buffer);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            EncodingUtils.skipLongString(buffer);
        }
    },

    INTEGER('i')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.unsignedIntegerLength();
        }

        @Override
        public Long toNativeValue(Object value)
        {
            if (value instanceof Long)
            {
                return (Long) value;
            }
            else if (value instanceof Integer)
            {
                return ((Integer) value).longValue();
            }
            else if (value instanceof Short)
            {
                return ((Short) value).longValue();
            }
            else if (value instanceof Byte)
            {
                return ((Byte) value).longValue();
            }
            else if ((value instanceof String) || (value == null))
            {
                return Long.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName() + ") to int.");
            }
        }


        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putLong( (Long) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getUnsignedInt();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Integer.BYTES);
        }
    },

    DECIMAL('D')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedByteLength() + EncodingUtils.encodedIntegerLength();
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if (value instanceof BigDecimal)
            {
                return (BigDecimal) value;
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to BigDecimal.");
            }
        }


        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            BigDecimal bd = (BigDecimal) value;

            byte places = new Integer(bd.scale()).byteValue();

            int unscaled = bd.intValue();

            buffer.put(places);

            buffer.putInt(unscaled);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            byte places = buffer.get();

            int unscaled = buffer.getInt();

            BigDecimal bd = new BigDecimal(unscaled);

            return bd.setScale(places);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Integer.BYTES + Byte.BYTES);
        }
    },

    TIMESTAMP('T')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedLongLength();
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if (value instanceof Long)
            {
                return value;
            }
            else if (value instanceof Date)
            {
                return ((Date) value).getTime();
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to timestamp.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putLong ((Long) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getLong();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Long.BYTES);
        }
    },

    /**
     * Implements the field table type. The native value of a field table type will be an instance of
     * {@link FieldTable}, which itself may contain name/value pairs encoded as {@link AMQTypedValue}s.
     */
    FIELD_TABLE('F')
    {
        /**
         * Calculates the size of an instance of the type in bytes.
         *
         * @param value An instance of the type.
         *
         * @return The size of the instance of the type in bytes.
         */
        @Override
        public int getEncodingSize(Object value)
        {
            // Ensure that the value is a FieldTable.
            if (!(value instanceof FieldTable))
            {
                throw new IllegalArgumentException("Value is not a FieldTable.");
            }

            FieldTable ftValue = (FieldTable) value;

            // Loop over all name/value pairs adding up size of each. FieldTable itself keeps track of its encoded
            // size as entries are added, so no need to loop over all explicitly.
            // EncodingUtils calculation of the encoded field table lenth, will include 4 bytes for its 'size' field.
            return EncodingUtils.encodedFieldTableLength(ftValue);
        }

        /**
         * Converts an instance of the type to an equivalent Java native representation.
         *
         * @param value An instance of the type.
         *
         * @return An equivalent Java native representation.
         */
        @Override
        public Object toNativeValue(Object value)
        {
            // Ensure that the value is a FieldTable.
            if (!(value instanceof FieldTable))
            {
                throw new IllegalArgumentException("Value is not a FieldTable.");
            }

            return (FieldTable) value;
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            // Ensure that the value is a FieldTable.
            if (!(value instanceof FieldTable))
            {
                throw new IllegalArgumentException("Value is not a FieldTable.");
            }

            FieldTable ftValue = (FieldTable) value;

            // Loop over all name/values writing out into buffer.
            ftValue.writeToBuffer(buffer);
        }
        /**
         * Reads an instance of the type from a specified byte buffer.
         *
         * @param buffer The byte buffer to write it to.
         *
         * @return An instance of the type.
         */
        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return EncodingUtils.readFieldTable(buffer);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            EncodingUtils.skipFieldTable(buffer);
        }
    },
    /**
     * Implements the field table type. The native value of a field table type will be an instance of
     * {@link FieldTable}, which itself may contain name/value pairs encoded as {@link AMQTypedValue}s.
     */
    FIELD_ARRAY('A')
            {
                @Override
                public int getEncodingSize(Object value)
                {
                    if (!(value instanceof Collection))
                    {
                        throw new IllegalArgumentException("Value is not a Collection.");
                    }

                    FieldArray fieldArrayValue = FieldArray.asFieldArray((Collection)value);

                    return 4 + fieldArrayValue.getEncodingSize();
                }

                @Override
                public Object toNativeValue(Object value)
                {
                    // Ensure that the value is a FieldTable.
                    if (!(value instanceof Collection))
                    {
                        throw new IllegalArgumentException("Value cannot be converted to a FieldArray.");
                    }

                    return FieldArray.asFieldArray((Collection)value);
                }

                @Override
                public void writeValueImpl(Object value, QpidByteBuffer buffer)
                {

                    if (!(value instanceof FieldArray))
                    {
                        throw new IllegalArgumentException("Value is not a FieldArray.");
                    }

                    FieldArray fieldArrayValue = (FieldArray) value;

                    // Loop over all name/values writing out into buffer.
                    fieldArrayValue.writeToBuffer(buffer);
                }

                /**
                 * Reads an instance of the type from a specified byte buffer.
                 *
                 * @param buffer The byte buffer to write it to.
                 *
                 * @return An instance of the type.
                 */
                @Override
                public Object readValueFromBuffer(QpidByteBuffer buffer)
                {
                    // Read size of field table then all name/value pairs.
                    return FieldArray.readFromBuffer(buffer);

                }

                @Override
                void skip(final QpidByteBuffer buffer)
                {
                    FieldArray.skipFieldArray(buffer);
                }
            },
    VOID('V')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return 0;
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if (value == null)
            {
                return null;
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to null String.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        { }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return null;
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            // no-op
        }
    },

    BINARY('x')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedLongstrLength((byte[]) value);
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if ((value instanceof byte[]) || (value == null))
            {
                return value;
            }
            else
            {
                throw new IllegalArgumentException("Value: " + value + " (" + value.getClass().getName()
                    + ") cannot be converted to byte[]");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            EncodingUtils.writeLongstr(buffer, (byte[]) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return EncodingUtils.readLongstr(buffer);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            EncodingUtils.skipLongString(buffer);
        }
    },

    ASCII_STRING('c')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedLongStringLength((String) value);
        }

        @Override
        public String toNativeValue(Object value)
        {
            if (value != null)
            {
                return value.toString();
            }
            else
            {
                throw new NullPointerException("Cannot convert: null to String.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            EncodingUtils.writeLongStringBytes(buffer, (String) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return EncodingUtils.readLongString(buffer);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            EncodingUtils.skipLongString(buffer);
        }
    },

    WIDE_STRING('C')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            // FIXME: use proper charset encoder
            return EncodingUtils.encodedLongStringLength((String) value);
        }

        @Override
        public String toNativeValue(Object value)
        {
            if (value != null)
            {
                return value.toString();
            }
            else
            {
                throw new NullPointerException("Cannot convert: null to String.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            EncodingUtils.writeLongStringBytes(buffer, (String) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return EncodingUtils.readLongString(buffer);
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            EncodingUtils.skipLongString(buffer);
        }
    },

    BOOLEAN('t')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedBooleanLength();
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if (value instanceof Boolean)
            {
                return (Boolean) value;
            }
            else if ((value instanceof String) || (value == null))
            {
                return Boolean.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to boolean.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            EncodingUtils.writeBoolean(buffer, (Boolean) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.get() == 1;
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Byte.BYTES);
        }
    },

    ASCII_CHARACTER('k')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedCharLength();
        }

        @Override
        public Character toNativeValue(Object value)
        {
            if (value instanceof Character)
            {
                return (Character) value;
            }
            else if (value == null)
            {
                throw new NullPointerException("Cannot convert null into char");
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to char.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            char charVal = (Character) value;
            buffer.put((byte) charVal);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return (char) buffer.get();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Byte.BYTES);
        }
    },

    BYTE('b')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedByteLength();
        }

        @Override
        public Byte toNativeValue(Object value)
        {
            if (value instanceof Byte)
            {
                return (Byte) value;
            }
            else if ((value instanceof String) || (value == null))
            {
                return Byte.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to byte.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.put((Byte) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.get();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Byte.BYTES);
        }
    },

    UNSIGNED_BYTE('B')
            {
                @Override
                public int getEncodingSize(Object value)
                {
                    return EncodingUtils.encodedByteLength();
                }

                @Override
                public Short toNativeValue(Object value)
                {
                    if (value == null)
                    {
                        throw new NullPointerException("Cannot convert null into unsigned byte");
                    }
                    else if (value instanceof Short)
                    {
                        return (Short) value;
                    }
                    else if (value instanceof Number)
                    {
                        return ((Number) value).shortValue();
                    }
                    else if (value instanceof String)
                    {
                        return Short.valueOf((String) value);
                    }
                    else
                    {
                        throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                                                        + ") to unsigned byte.");
                    }
                }

                @Override
                public void writeValueImpl(Object value, QpidByteBuffer buffer)
                {
                    buffer.putUnsignedByte((Short) value);
                }

                @Override
                public Object readValueFromBuffer(QpidByteBuffer buffer)
                {
                    return buffer.getUnsignedByte();
                }

                @Override
                void skip(final QpidByteBuffer buffer)
                {
                    buffer.position(buffer.position() + Byte.BYTES);
                }
    },

    SHORT('s')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedShortLength();
        }

        @Override
        public Short toNativeValue(Object value)
        {
            if (value instanceof Short)
            {
                return (Short) value;
            }
            else if (value instanceof Byte)
            {
                return ((Byte) value).shortValue();
            }
            else if ((value instanceof String) || (value == null))
            {
                return Short.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to short.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putShort((Short) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getShort();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Short.BYTES);
        }
    },

    UNSIGNED_SHORT('u')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedShortLength();
        }

        @Override
        public Integer toNativeValue(Object value)
        {
            if (value == null)
            {
                throw new NullPointerException("Cannot convert null into unsigned short");
            }
            else if (value instanceof Integer)
            {
                return (Integer) value;
            }
            else if (value instanceof Number)
            {
                return ((Number) value).intValue();
            }
            else if (value instanceof String)
            {
                return Integer.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to unsigned short.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putUnsignedShort((Integer) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getUnsignedShort();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Short.BYTES);
        }
    },

    INT('I')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedIntegerLength();
        }

        @Override
        public Integer toNativeValue(Object value)
        {
            if (value instanceof Integer)
            {
                return (Integer) value;
            }
            else if (value instanceof Short)
            {
                return ((Short) value).intValue();
            }
            else if (value instanceof Byte)
            {
                return ((Byte) value).intValue();
            }
            else if ((value instanceof String) || (value == null))
            {
                return Integer.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName() + ") to int.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putInt((Integer) value);
        }
        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getInt();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Integer.BYTES);
        }
    },

    LONG('l')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedLongLength();
        }

        @Override
        public Object toNativeValue(Object value)
        {
            if (value instanceof Long)
            {
                return (Long) value;
            }
            else if (value instanceof Integer)
            {
                return ((Integer) value).longValue();
            }
            else if (value instanceof Short)
            {
                return ((Short) value).longValue();
            }
            else if (value instanceof Byte)
            {
                return ((Byte) value).longValue();
            }
            else if ((value instanceof String) || (value == null))
            {
                return Long.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to long.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putLong ((Long) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getLong();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Long.BYTES);
        }
    },

    FLOAT('f')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedFloatLength();
        }

        @Override
        public Float toNativeValue(Object value)
        {
            if (value instanceof Float)
            {
                return (Float) value;
            }
            else if ((value instanceof String) || (value == null))
            {
                return Float.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to float.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putFloat ((Float) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getFloat();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Float.BYTES);
        }
    },

    DOUBLE('d')
    {
        @Override
        public int getEncodingSize(Object value)
        {
            return EncodingUtils.encodedDoubleLength();
        }

        @Override
        public Double toNativeValue(Object value)
        {
            if (value instanceof Double)
            {
                return (Double) value;
            }
            else if (value instanceof Float)
            {
                return ((Float) value).doubleValue();
            }
            else if ((value instanceof String) || (value == null))
            {
                return Double.valueOf((String) value);
            }
            else
            {
                throw new NumberFormatException("Cannot convert: " + value + "(" + value.getClass().getName()
                    + ") to double.");
            }
        }

        @Override
        public void writeValueImpl(Object value, QpidByteBuffer buffer)
        {
            buffer.putDouble((Double) value);
        }

        @Override
        public Object readValueFromBuffer(QpidByteBuffer buffer)
        {
            return buffer.getDouble();
        }

        @Override
        void skip(final QpidByteBuffer buffer)
        {
            buffer.position(buffer.position() + Double.BYTES);
        }
    };

    /** Holds the defined one byte identifier for the type. */
    private final byte _identifier;

    /**
     * Creates an instance of an AMQP type from its defined one byte identifier.
     *
     * @param identifier The one byte identifier for the type.
     */
    AMQType(char identifier)
    {
        _identifier = (byte) identifier;
    }

    /**
     * Extracts the byte identifier for the typ.
     *
     * @return The byte identifier for the typ.
     */
    public final byte identifier()
    {
        return _identifier;
    }

    /**
     * Calculates the size of an instance of the type in bytes.
     *
     * @param value An instance of the type.
     *
     * @return The size of the instance of the type in bytes.
     */
    public abstract int getEncodingSize(Object value);

    /**
     * Converts an instance of the type to an equivalent Java native representation.
     *
     * @param value An instance of the type.
     *
     * @return An equivalent Java native representation.
     */
    public abstract Object toNativeValue(Object value);

    /**
     * Converts an instance of the type to an equivalent Java native representation, packaged as an
     * {@link AMQTypedValue} tagged with its AMQP type.
     *
     * @param value An instance of the type.
     *
     * @return An equivalent Java native representation, tagged with its AMQP type.
     */
    public AMQTypedValue asTypedValue(Object value)
    {
        return AMQTypedValue.createAMQTypedValue(this, toNativeValue(value));
    }

    public void writeToBuffer(Object value, QpidByteBuffer buffer)
    {
        buffer.put(identifier());
        writeValueImpl(value, buffer);
    }

    abstract void writeValueImpl(Object value, QpidByteBuffer buffer);

    /**
     * Reads an instance of the type from a specified byte buffer.
     *
     * @param buffer The byte buffer to write it to.
     *
     * @return An instance of the type.
     */
    abstract Object readValueFromBuffer(QpidByteBuffer buffer);

    abstract void skip(QpidByteBuffer buffer);
}
