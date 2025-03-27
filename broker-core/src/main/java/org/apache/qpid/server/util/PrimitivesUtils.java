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

package org.apache.qpid.server.util;

public final class PrimitivesUtils
{
    public static final int BYTES = Short.SIZE / Byte.SIZE;
    private static final String ARRAY_TOO_SMALL = "array too small: %s < %s";

    private PrimitivesUtils() { }

    private static void checkArgument(boolean expression, int p1)
    {
        if (!expression)
        {
            throw new IllegalArgumentException(String.format(PrimitivesUtils.ARRAY_TOO_SMALL, p1, PrimitivesUtils.BYTES));
        }
    }

    public static byte[] toByteArray(short value)
    {
        return new byte[] {(byte) (value >> 8), (byte) value};
    }

    public static byte[] toByteArray(char value)
    {
        return new byte[] {(byte) (value >> 8), (byte) value};
    }

    public static byte[] toByteArray(int value)
    {
        return new byte[]
        {
            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    public static byte[] toByteArray(long value)
    {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--)
        {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    public static short shortFromByteArray(byte[] bytes)
    {
        checkArgument(bytes.length >= BYTES, bytes.length);
        return (short) ((bytes[0] << 8) | (bytes[1]) & 0xFF);
    }

    public static char charFromByteArray(byte[] bytes)
    {
        checkArgument(bytes.length >= BYTES, bytes.length);
        return (char) ((bytes[0] << 8) | (bytes[1] & 0xFF));
    }

    public static int intFromByteArray(byte[] bytes)
    {
        checkArgument(bytes.length >= BYTES, bytes.length);
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }

    public static long longFromByteArray(byte[] bytes)
    {
        checkArgument(bytes.length >= BYTES, bytes.length);
        return (bytes[0] & 0xFFL) << 56 | (bytes[1] & 0xFFL) << 48 | (bytes[2] & 0xFFL) << 40 |
               (bytes[3] & 0xFFL) << 32 | (bytes[4] & 0xFFL) << 24 | (bytes[5] & 0xFFL) << 16 |
               (bytes[6] & 0xFFL) << 8 | (bytes[7] & 0xFFL);
    }
}
