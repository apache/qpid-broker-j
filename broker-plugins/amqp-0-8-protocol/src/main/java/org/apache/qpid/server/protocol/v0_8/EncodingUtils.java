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

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class EncodingUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EncodingUtils.class);

    private EncodingUtils()
    {
    }

    public static int encodedShortStringLength(String s)
    {
        if (s == null)
        {
            return 1;
        }
        else
        {
            int size =  1 + getUTF8Length(s);
            if(size > 256)
            {
                throw new IllegalArgumentException("String '"+s+"' is too long - over 255 octets to encode");
            }
            return (short) size;
        }
    }

    public static int encodedShortStringLength(long l)
    {
        if (l == 0)
        {
            return 2;
        }
        else if(l>=1000000000000L && l<10000000000000L)
        {
            // this covers the common case of timestamps between Sep 2010 and Nov 2286
            return 14;
        }
        else
        {
            return Long.toString(l).length()+1;
        }
    }

    public static int encodedShortStringLength(AMQShortString s)
    {
        if (s == null)
        {
            return 1;
        }
        else
        {
            return (1 + s.length());
        }
    }

    public static int encodedLongStringLength(String s)
    {
        if (s == null)
        {
            return 4;
        }
        else
        {
            return 4 + getUTF8Length(s);
        }
    }

    public static int encodedLongstrLength(byte[] bytes)
    {
        if (bytes == null)
        {
            return 4;
        }
        else
        {
            return 4 + bytes.length;
        }
    }

    public static int encodedFieldTableLength(FieldTable table)
    {
        if (table == null)
        {
            // length is encoded as 4 octets
            return 4;
        }
        else
        {
            // length of the table plus 4 octets for the length
            return (int) table.getEncodedSize() + 4;
        }
    }

    public static void writeLongAsShortString(QpidByteBuffer buffer, long l)
    {
        String s = Long.toString(l);
        byte[] encodedString = new byte[1+s.length()];
        char[] cha = s.toCharArray();
        encodedString[0] = (byte) s.length();
        for (int i = 0; i < cha.length; i++)
        {
            encodedString[i+1] = (byte) cha[i];
        }
        buffer.put(encodedString);

    }


    public static void writeShortStringBytes(QpidByteBuffer buffer, AMQShortString s)
    {
        if (s != null)
        {
            s.writeToBuffer(buffer);
        }
        else
        {
            // really writing out unsigned byte
            buffer.put((byte) 0);
        }
    }

    public static void writeShortStringBytes(QpidByteBuffer buffer, String s)
    {
        if (s != null)
        {
            AMQShortString.writeShortString(buffer, s);
        }
        else
        {
            buffer.put((byte) 0);
        }
    }

    public static void writeLongStringBytes(QpidByteBuffer buffer, String s)
    {
        if (s != null)
        {
            int len = getUTF8Length(s);
            buffer.putUnsignedInt((long) len);
            buffer.put(asUTF8Bytes(s));

        }
        else
        {
            buffer.putUnsignedInt((long) 0);
        }
    }

    public static int unsignedIntegerLength()
    {
        return 4;
    }

    public static void writeFieldTableBytes(QpidByteBuffer buffer, FieldTable table)
    {
        if (table != null)
        {
            table.writeToBuffer(buffer);
        }
        else
        {
            buffer.putUnsignedInt((long) 0);
        }
    }

    public static void writeLongstr(QpidByteBuffer buffer, byte[] data)
    {
        if (data != null)
        {
            buffer.putUnsignedInt((long) data.length);
            buffer.put(data);
        }
        else
        {
            buffer.putUnsignedInt((long) 0);
        }
    }

    public static FieldTable readFieldTable(QpidByteBuffer input)
    {
        long length = input.getUnsignedInt();
        if (length == 0)
        {
            return null;
        }
        else
        {
            return FieldTableFactory.createFieldTable(input, (int) length);
        }
    }

    public static void skipFieldTable(QpidByteBuffer buffer)
    {
        long length = buffer.getUnsignedInt();
        if (length > 0)
        {
            buffer.position(buffer.position() + (int)length);
        }
    }


    public static String readLongString(QpidByteBuffer buffer)
    {
        long length = ((long)(buffer.getInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return "";
        }
        else
        {
            byte[] stringBytes = new byte[(int) length];
            buffer.get(stringBytes, 0, (int) length);

            return new String(stringBytes, StandardCharsets.UTF_8);
        }
    }


    public static void skipLongString(final QpidByteBuffer buffer)
    {
        long length = buffer.getUnsignedInt();
        if (length > 0)
        {
            buffer.position(buffer.position() + (int)length);
        }
    }

    public static byte[] readLongstr(QpidByteBuffer buffer)
    {
        long length = ((long)(buffer.getInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return null;
        }
        else
        {
            byte[] result = new byte[(int) length];
            buffer.get(result);

            return result;
        }
    }

    // **** new methods

    // AMQP_BOOLEAN_PROPERTY_PREFIX

    public static void writeBoolean(QpidByteBuffer buffer, boolean aBoolean)
    {
        buffer.put(aBoolean ? (byte)1 : (byte)0);
    }

    public static int encodedBooleanLength()
    {
        return 1;
    }

    public static int encodedByteLength()
    {
        return 1;
    }

    public static int encodedShortLength()
    {
        return 2;
    }

    public static int encodedIntegerLength()
    {
        return 4;
    }

    public static int encodedLongLength()
    {
        return 8;
    }

    public static int encodedFloatLength()
    {
        return 4;
    }

    public static int encodedDoubleLength()
    {
        return 8;
    }

    public static byte[] readBytes(QpidByteBuffer buffer)
    {
        long length = buffer.getUnsignedInt();
        if (length == 0)
        {
            return null;
        }
        else
        {
            byte[] dataBytes = new byte[(int)length];
            buffer.get(dataBytes);

            return dataBytes;
        }
    }

    public static void writeBytes(QpidByteBuffer buffer, byte[] data)
    {
        if (data != null)
        {
            // TODO: check length fits in an unsigned byte
            buffer.putUnsignedInt((long)data.length);
            buffer.put(data);
        }
        else
        {
            buffer.putUnsignedInt(0L);
        }
    }

    // CHAR_PROPERTY
    public static int encodedCharLength()
    {
        return encodedByteLength();
    }

    public static long readLongAsShortString(QpidByteBuffer buffer) throws AMQFrameDecodingException
    {
        short length = buffer.getUnsignedByte();
        short pos = 0;
        if (length == 0)
        {
            return 0L;
        }

        byte digit = buffer.get();
        boolean isNegative;
        long result = 0;
        if (digit == (byte) '-')
        {
            isNegative = true;
            pos++;
            digit = buffer.get();
        }
        else
        {
            isNegative = false;
        }

        result = toNumber(digit);
        pos++;

        while (pos < length)
        {
            pos++;
            digit = buffer.get();
            result = (result << 3) + (result << 1);
            result += toNumber(digit);
        }

        return isNegative ? -result : result;
    }

    private static int toNumber(final byte digit) throws AMQFrameDecodingException
    {
        if (digit >= '0' && digit <= '9')
        {
            return digit - (byte) '0';
        }
        throw new AMQFrameDecodingException(String.format("Unexpected character '%c' in string representing long value",
                                                          digit));
    }

    public static byte[] asUTF8Bytes(CharSequence string)
    {
        byte[] bytes = new byte[getUTF8Length(string)];
        int j = 0;
        if(string != null)
        {
            final int length = string.length();
            int c;

            for (int i = 0; i < length; i++)
            {
                c = string.charAt(i);
                if ((c & 0xFF80) == 0)          /* U+0000..U+007F */
                {
                    bytes[j++] = (byte) c;
                }
                else if ((c & 0xF800) == 0)     /* U+0080..U+07FF */
                {
                    bytes[j++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                    bytes[j++] = (byte) (0x80 | (c & 0x3F));
                }
                else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF))     /* U+0800..U+FFFF - excluding surrogate pairs */
                {
                    bytes[j++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                    bytes[j++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                    bytes[j++] = (byte) (0x80 | (c & 0x3F));
                }
                else
                {
                    int low;

                    if ((++i == length) || ((low = string.charAt(i)) & 0xDC00) != 0xDC00)
                    {
                        throw new IllegalArgumentException("String contains invalid Unicode code points");
                    }

                    c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                    bytes[j++] = (byte) (0xF0 | ((c >> 18) & 0x07));
                    bytes[j++] = (byte) (0x80 | ((c >> 12) & 0x3F));
                    bytes[j++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                    bytes[j++] = (byte) (0x80 | (c & 0x3F));
                }
            }
        }
        return bytes;
    }

    public static int getUTF8Length(CharSequence string)
    {
        int size = 0;
        if(string != null)
        {
            int c;

            final int inputLength = string.length();
            for (int i = 0; i < inputLength; i++)
            {
                c = string.charAt(i);
                if ((c & 0xFF80) == 0)          /* U+0000..U+007F */
                {
                    size++;
                }
                else if ((c & 0xF800) == 0)     /* U+0080..U+07FF */
                {
                    size += 2;
                }
                else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF))     /* U+0800..U+FFFF - excluding surrogate pairs */
                {
                    size += 3;
                }
                else
                {
                    if ((++i == size) || (string.charAt(i) & 0xDC00) != 0xDC00)
                    {
                        throw new IllegalArgumentException("String contains invalid Unicode code points");
                    }

                    size += 4;
                }
            }

        }
        return size;
    }

}
