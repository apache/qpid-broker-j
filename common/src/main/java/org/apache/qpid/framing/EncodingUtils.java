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
package org.apache.qpid.framing;

import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.codec.MarkableDataInput;

public class EncodingUtils
{
    private static final Logger _logger = LoggerFactory.getLogger(EncodingUtils.class);

    private static final String STRING_ENCODING = "iso8859-15";

    private static final Charset _charset = Charset.forName("iso8859-15");

    public static final int SIZEOF_UNSIGNED_SHORT = 2;
    public static final int SIZEOF_UNSIGNED_INT = 4;
    private static final boolean[] ALL_FALSE_ARRAY = new boolean[8];

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

    public static int encodedShortStringLength(short s)
    {
        if (s == 0)
        {
            return 1 + 1;
        }

        int len = 0;
        if (s < 0)
        {
            len = 1;
            // sloppy - doesn't work of Integer.MIN_VALUE
            s = (short) -s;
        }

        if (s > 9999)
        {
            return 1 + 5;
        }
        else if (s > 999)
        {
            return 1 + 4;
        }
        else if (s > 99)
        {
            return 1 + 3;
        }
        else if (s > 9)
        {
            return 1 + 2;
        }
        else
        {
            return 1 + 1;
        }

    }

    public static int encodedShortStringLength(int i)
    {
        if (i == 0)
        {
            return 1 + 1;
        }

        int len = 0;
        if (i < 0)
        {
            len = 1;
            // sloppy - doesn't work of Integer.MIN_VALUE
            i = -i;
        }

        // range is now 1 - 2147483647
        if (i < Short.MAX_VALUE)
        {
            return len + encodedShortStringLength((short) i);
        }
        else if (i > 999999)
        {
            return len + 6 + encodedShortStringLength((short) (i / 1000000));
        }
        else // if i > 99999
        {
            return len + 5 + encodedShortStringLength((short) (i / 100000));
        }

    }

    public static int encodedShortStringLength(long l)
    {
        if (l == 0)
        {
            return 1 + 1;
        }

        int len = 0;
        if (l < 0)
        {
            len = 1;
            // sloppy - doesn't work of Long.MIN_VALUE
            l = -l;
        }

        if (l < Integer.MAX_VALUE)
        {
            return len + encodedShortStringLength((int) l);
        }
        else if (l > 9999999999L)
        {
            return len + 10 + encodedShortStringLength((int) (l / 10000000000L));
        }
        else
        {
            return len + 1 + encodedShortStringLength((int) (l / 10L));
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

    public static void writeLongStringBytes(QpidByteBuffer buffer, String s)
    {
        if (s != null)
        {
            int len = getUTF8Length(s);
            writeUnsignedInteger(buffer, len);
            buffer.put(asUTF8Bytes(s));

        }
        else
        {
            writeUnsignedInteger(buffer, 0);
        }
    }


    public static void writeUnsignedByte(QpidByteBuffer buffer, short b)
    {
        byte bv = (byte) b;
        buffer.put(bv);
    }

    public static void writeUnsignedShort(QpidByteBuffer buffer, int s)
    {
        // TODO: Is this comparison safe? Do I need to cast RHS to long?
        if (s < Short.MAX_VALUE)
        {
            buffer.putShort((short) s);
        }
        else
        {
            short sv = (short) s;
            buffer.put((byte) (0xFF & (sv >> 8)));
            buffer.put((byte) (0xFF & sv));
        }
    }


    public static int unsignedIntegerLength()
    {
        return 4;
    }

    public static void writeUnsignedInteger(QpidByteBuffer buffer, long l)
    {
        // TODO: Is this comparison safe? Do I need to cast RHS to long?
        if (l < Integer.MAX_VALUE)
        {
            buffer.putInt((int) l);
        }
        else
        {
            int iv = (int) l;

            // FIXME: This *may* go faster if we build this into a local 4-byte array and then
            // put the array in a single call.
            buffer.put((byte) (0xFF & (iv >> 24)));
            buffer.put((byte) (0xFF & (iv >> 16)));
            buffer.put((byte) (0xFF & (iv >> 8)));
            buffer.put((byte) (0xFF & iv));
        }
    }


    public static void writeFieldTableBytes(QpidByteBuffer buffer, FieldTable table)
    {
        if (table != null)
        {
            table.writeToBuffer(buffer);
        }
        else
        {
            EncodingUtils.writeUnsignedInteger(buffer, 0);
        }
    }

    public static void writeLongstr(QpidByteBuffer buffer, byte[] data)
    {
        if (data != null)
        {
            writeUnsignedInteger(buffer, data.length);
            buffer.put(data);
        }
        else
        {
            writeUnsignedInteger(buffer, 0);
        }
    }

    public static FieldTable readFieldTable(MarkableDataInput input) throws AMQFrameDecodingException, IOException
    {
        long length = ((long)(input.readInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return null;
        }
        else
        {
            return new FieldTable(input, (int) length);
        }
    }


    public static AMQShortString readAMQShortString(DataInput buffer) throws IOException
    {
        return AMQShortString.readFromBuffer(buffer);

    }

    public static String readLongString(DataInput buffer) throws IOException
    {
        long length = ((long)(buffer.readInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return "";
        }
        else
        {
            byte[] stringBytes = new byte[(int) length];
            buffer.readFully(stringBytes, 0, (int) length);

            return new String(stringBytes, StandardCharsets.UTF_8);
        }
    }

    public static byte[] readLongstr(DataInput buffer) throws IOException
    {
        long length = ((long)(buffer.readInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return null;
        }
        else
        {
            byte[] result = new byte[(int) length];
            buffer.readFully(result);

            return result;
        }
    }

    public static long readTimestamp(DataInput buffer) throws IOException
    {
        return buffer.readLong();
    }

    public static char[] convertToHexCharArray(byte[] from)
    {
        int length = from.length;
        char[] result_buff = new char[(length * 2) + 2];

        result_buff[0] = '0';
        result_buff[1] = 'x';

        int bite;
        int dest = 2;

        for (int i = 0; i < length; i++)
        {
            bite = from[i];

            if (bite < 0)
            {
                bite += 256;
            }

            result_buff[dest++] = hex_chars[bite >> 4];
            result_buff[dest++] = hex_chars[bite & 0x0f];
        }

        return (result_buff);
    }

    private static char[] hex_chars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    // **** new methods

    // AMQP_BOOLEAN_PROPERTY_PREFIX

    public static void writeBoolean(QpidByteBuffer buffer, boolean aBoolean)
    {
        buffer.put(aBoolean ? (byte)1 : (byte)0);
    }

    public static boolean readBoolean(DataInput buffer) throws IOException
    {
        byte packedValue = buffer.readByte();

        return (packedValue == 1);
    }

    public static int encodedBooleanLength()
    {
        return 1;
    }

    public static byte readByte(DataInput buffer) throws IOException
    {
        return buffer.readByte();
    }

    public static int encodedByteLength()
    {
        return 1;
    }

    public static short readShort(DataInput buffer) throws IOException
    {
        return buffer.readShort();
    }

    public static int encodedShortLength()
    {
        return 2;
    }

    public static int readInteger(DataInput buffer) throws IOException
    {
        return buffer.readInt();
    }

    public static int encodedIntegerLength()
    {
        return 4;
    }

    public static long readLong(DataInput buffer) throws IOException
    {
        return buffer.readLong();
    }

    public static int encodedLongLength()
    {
        return 8;
    }

    public static float readFloat(DataInput buffer) throws IOException
    {
        return buffer.readFloat();
    }

    public static int encodedFloatLength()
    {
        return 4;
    }

    public static double readDouble(DataInput buffer) throws IOException
    {
        return buffer.readDouble();
    }

    public static int encodedDoubleLength()
    {
        return 8;
    }

    public static byte[] readBytes(DataInput buffer) throws IOException
    {
        long length = ((long)(buffer.readInt())) & 0xFFFFFFFFL;
        if (length == 0)
        {
            return null;
        }
        else
        {
            byte[] dataBytes = new byte[(int)length];
            buffer.readFully(dataBytes, 0, (int) length);

            return dataBytes;
        }
    }

    public static void writeBytes(QpidByteBuffer buffer, byte[] data)
    {
        if (data != null)
        {
            // TODO: check length fits in an unsigned byte
            writeUnsignedInteger(buffer,  (long)data.length);
            buffer.put(data);
        }
        else
        {
            writeUnsignedInteger(buffer, 0L);
        }
    }

    // CHAR_PROPERTY
    public static int encodedCharLength()
    {
        return encodedByteLength();
    }

    public static char readChar(DataInput buffer) throws IOException
    {
        // This is valid as we know that the Character is ASCII 0..127
        return (char) buffer.readByte();
    }

    public static long readLongAsShortString(DataInput buffer) throws IOException
    {
        short length = (short) buffer.readUnsignedByte();
        short pos = 0;
        if (length == 0)
        {
            return 0L;
        }

        byte digit = buffer.readByte();
        boolean isNegative;
        long result = 0;
        if (digit == (byte) '-')
        {
            isNegative = true;
            pos++;
            digit = buffer.readByte();
        }
        else
        {
            isNegative = false;
        }

        result = digit - (byte) '0';
        pos++;

        while (pos < length)
        {
            pos++;
            digit = buffer.readByte();
            result = (result << 3) + (result << 1);
            result += digit - (byte) '0';
        }

        return result;
    }

    public static long readUnsignedInteger(DataInput buffer) throws IOException
    {
        long l = 0xFF & buffer.readByte();
        l <<= 8;
        l = l | (0xFF & buffer.readByte());
        l <<= 8;
        l = l | (0xFF & buffer.readByte());
        l <<= 8;
        l = l | (0xFF & buffer.readByte());

        return l;
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
