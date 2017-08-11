/*
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

package org.apache.qpid.server.protocol.v0_10.transport;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EncoderUtils
{
    private static final Map<Class<?>, Type> ENCODINGS = new HashMap<>();
    static
    {
        ENCODINGS.put(Boolean.class, Type.BOOLEAN);
        ENCODINGS.put(String.class, Type.STR16);
        ENCODINGS.put(Long.class, Type.INT64);
        ENCODINGS.put(Integer.class, Type.INT32);
        ENCODINGS.put(Short.class, Type.INT16);
        ENCODINGS.put(Byte.class, Type.INT8);
        ENCODINGS.put(Map.class, Type.MAP);
        ENCODINGS.put(List.class, Type.LIST);
        ENCODINGS.put(Float.class, Type.FLOAT);
        ENCODINGS.put(Double.class, Type.DOUBLE);
        ENCODINGS.put(Character.class, Type.CHAR);
        ENCODINGS.put(byte[].class, Type.VBIN32);
        ENCODINGS.put(UUID.class, Type.UUID);
        ENCODINGS.put(Xid.class, Type.STRUCT32);
    }

    public static Type getEncodingType(Object value)
    {
        if (value == null)
        {
            return Type.VOID;
        }

        Class klass = value.getClass();
        Type type = resolve(klass);

        if (type == null)
        {
            throw new IllegalArgumentException
                    ("unable to resolve type: " + klass + ", " + value);
        }
        else
        {
            return type;
        }
    }

    public static int getStruct32Length(Struct s)
    {
        if (s == null)
        {
            return 4;
        }
        else
        {
            int len = 0;
            len += 4; // size
            len += 2; // encoded type
            len += s.getEncodedLength();

            return len;
        }
    }

    public static int getArrayLength(List<Object> value)
    {
        int len = 0;
        len += 4; // size
        if (value != null && !value.isEmpty())
        {
            len += 1; // type
            len += 4; // array size

            final Type type = getEncodingType(value.get(0));
            for (Object v : value)
            {
                len += getTypeLength(type, v);
            }

        }

        return len;

    }

    public static int getListLength(List<Object> value)
    {
        int len = 0;
        len += 4; // size
        if (value != null && !value.isEmpty())
        {
            len += 4; // list size
            for (Object v : value)
            {
                final Type type = getEncodingType(v);
                len += 1; // type code
                len += getTypeLength(type, v);
            }
        }
        return len;
    }

    public static int getStr8Length(String s)
    {
        if (s == null)
        {
            return 1;
        }
        else
        {
            int length = s.getBytes(StandardCharsets.UTF_8).length;
            if (length > 255)
            {
                throw new IllegalArgumentException(String.format("String too long (%d) for str8", length));
            }
            return 1 + length;
        }
    }

    public static int getStr16Length(String s)
    {
        if (s == null)
        {
            return 2;
        }
        else
        {
            int length = s.getBytes(StandardCharsets.UTF_8).length;
            if (length > 65535)
            {
                throw new IllegalArgumentException(String.format("String too long (%d) for str16", length));
            }
            return 2 + length;
        }
    }

    public static int getVbin16Length(byte[] bytes)
    {
        if (bytes == null)
        {
            return 2;
        }
        else
        {
            return 2 + bytes.length;
        }
    }

    public static int getStructLength(int type, Struct s)
    {
        int len = 0;

        if (s == null)
        {
            s = Struct.create(type);
        }

        int width = s.getSizeWidth();

        if (width > 0)
        {
            switch (width)
            {
                case 1:
                case 2:
                case 4:
                    len += width;
                    break;
                default:
                    throw new IllegalStateException("illegal width: " + width);
            }
        }

        if (type > 0)
        {
            len += 2; // type
        }

        len += s.getEncodedLength();

        return len;


    }

    public static int getMapLength(Map<String, Object> map)
    {
        int len = 0;

        len += 4; // size
        if (map != null)
        {
            len += 4; // map size
            for (Map.Entry<String,Object> entry : map.entrySet())
            {
                String key = entry.getKey();
                Object value = entry.getValue();
                len += getStr8Length(key);
                len += 1; // type code
                Type type = getEncodingType(value);
                len += getTypeLength(type, value);
            }
        }

        return len;
    }

    private static Type resolve(Class klass)
    {
        Type type = ENCODINGS.get(klass);
        if (type != null)
        {
            return type;
        }

        Class sup = klass.getSuperclass();
        if (sup != null)
        {
            type = resolve(klass.getSuperclass());

            if (type != null)
            {
                return type;
            }
        }

        for (Class iface : klass.getInterfaces())
        {
            type = resolve(iface);
            if (type != null)
            {
                return type;
            }
        }

        return null;
    }

    private static int getTypeLength(Type t, Object value)
    {
        switch (t)
        {
            case VOID:
                return 0;

            case BIN8:
            case UINT8:
            case INT8:
            case CHAR:
            case BOOLEAN:
                return 1;

            case BIN16:
            case UINT16:
            case INT16:
                return 2;

            case BIN32:
            case UINT32:
            case CHAR_UTF32:
            case INT32:
            case FLOAT:
                return 4;

            case BIN64:
            case UINT64:
            case INT64:
            case DATETIME:
            case DOUBLE:
                return 8;

            case UUID:
                return 16;

            case STR8:
                return getStr8Length((String) value);

            case STR16:
                return getStr16Length((String) value);

            case STR8_LATIN:
            case STR8_UTF16:
            case STR16_LATIN:
            case STR16_UTF16:
                String str = (String) value;
                return t.getWidth() + (str == null ? 0 : str.getBytes(StandardCharsets.UTF_8).length);

            case MAP:
                return getMapLength((Map<String, Object>) value);

            case LIST:
                return getListLength((List<Object>) value);

            case ARRAY:
                return getArrayLength((List<Object>) value);

            case STRUCT32:
                return getStruct32Length((Struct) value);

            case BIN40:
            case DEC32:
            case BIN72:
            case DEC64:
                return t.getWidth() + (value == null ? 0 : ((byte[])value).length);

            default:
                if (!(value instanceof byte[]))
                {
                    throw new IllegalArgumentException("Expecting byte array was " + (value == null
                            ? "null"
                            : value.getClass()));
                }
                return t.getWidth() + (value == null ? 0 : ((byte[])value).length);
        }
    }

    public static boolean isEncodable(final Object value)
    {
        try
        {
            getEncodingType(value);
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }

        if (value instanceof Map)
        {
            for(Map.Entry<?,?> entry: ((Map<?,?>)value).entrySet())
            {
                Object key = entry.getKey();
                if (key instanceof String)
                {
                    String string = (String)key;
                    if (string.length() > 0xFF)
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
                if (!isEncodable(entry.getValue()))
                {
                    return false;
                }
            }
        }
        else if (value instanceof Collection)
        {
            Collection<?> collection = (Collection<?>) value;
            int index = 0;
            for (Object o: collection)
            {
                if (!isEncodable(o))
                {
                    return false;
                }
                index++;
            }
        }
        return true;
    }
}
