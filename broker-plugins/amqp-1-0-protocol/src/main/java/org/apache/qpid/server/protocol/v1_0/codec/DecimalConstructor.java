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
 */

package org.apache.qpid.server.protocol.v1_0.codec;

import java.math.BigDecimal;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public abstract class DecimalConstructor implements TypeConstructor<BigDecimal>
{

    private static final DecimalConstructor DECIMAL_32 = new DecimalConstructor()
    {

        @Override
        public BigDecimal construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {
            int val;

            if (in.hasRemaining(4))
            {
                val = in.getInt();
            }
            else
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct decimal32: insufficient input data");
            }

            return constructFrom32(val);

        }
    };


    private static final DecimalConstructor DECIMAL_64 = new DecimalConstructor()
    {

        @Override
        public BigDecimal construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {
            long val;

            if (in.hasRemaining(8))
            {
                val = in.getLong();
            }
            else
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct decimal64: insufficient input data");
            }

            return constructFrom64(val);

        }
    };


    private static final DecimalConstructor DECIMAL_128 = new DecimalConstructor()
    {

        @Override
        public BigDecimal construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {

            long high;
            long low;

            if (in.hasRemaining(16))
            {
                high = in.getLong();
                low = in.getLong();
            }
            else
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct decimal128: insufficient input data");
            }

            return constructFrom128(high, low);
        }
    };

    private static final BigDecimal TWO_TO_THE_SIXTY_FOUR = new BigDecimal(2).pow(64);

    private static BigDecimal constructFrom128(long high, long low)
    {
        int sign = ((high & 0x8000000000000000L) == 0) ? 1 : -1;

        int exponent = 0;
        long significand = high;

        if((high & 0x6000000000000000L) != 0x6000000000000000L)
        {
            exponent = ((int) ((high & 0x7FFE000000000000L) >> 49)) - 6176;
            significand = high & 0x0001ffffffffffffL;
        }
        else if((high & 0x7800000000000000L) != 0x7800000000000000L)
        {
            exponent = ((int)((high & 0x1fff800000000000L) >> 47)) - 6176;
            significand = (0x00007fffffffffffL & high) | 0x0004000000000000L;
        }
        else
        {
            // NaN or infinite
            return null;
        }


        BigDecimal bigDecimal = new BigDecimal(significand).multiply(TWO_TO_THE_SIXTY_FOUR);
        if(low >=0)
        {
            bigDecimal = bigDecimal.add(new BigDecimal(low));
        }
        else
        {
            bigDecimal = bigDecimal.add(TWO_TO_THE_SIXTY_FOUR.add(new BigDecimal(low)));
        }
        if(((high & 0x8000000000000000L) != 0))
        {
            bigDecimal = bigDecimal.negate();
        }
        bigDecimal = bigDecimal.scaleByPowerOfTen(exponent);
        return bigDecimal;
    }


    private static BigDecimal constructFrom64(final long val)
    {
        int sign = ((val & 0x8000000000000000L) == 0) ? 1 : -1;

        int exponent = 0;
        long significand = val;

        if((val & 0x6000000000000000L) != 0x6000000000000000L)
        {
            exponent = ((int) ((val & 0x7FE0000000000000L) >> 53)) - 398;
            significand = val & 0x001fffffffffffffL;
        }
        else if((val & 0x7800000000000000L) != 0x7800000000000000L)
        {
            exponent = ((int)((val & 0x1ff8000000000000L) >> 51)) - 398;
            significand = (0x0007ffffffffffffL & val) | 0x0020000000000000L;
        }
        else
        {
            // NaN or infinite
            return null;
        }

        BigDecimal bigDecimal = new BigDecimal(sign * significand);
        bigDecimal = bigDecimal.scaleByPowerOfTen(exponent);
        return bigDecimal;
    }

    private static BigDecimal constructFrom32(final int val)
    {
        int sign = ((val & 0x80000000) == 0) ? 1 : -1;

        int exponent = 0;
        int significand = val;

        if((val & 0x60000000) != 0x60000000)
        {
            exponent = ((val & 0x7F800000) >> 23) - 101;
            significand = val & 0x007fffffff;
        }
        else if((val &  0x78000000) != 0x78000000)
        {
            exponent = ((val & 0x1fe00000)>>21) - 101;
            significand = (0x001fffff & val) | 0x00800000;
        }
        else
        {
            // NaN or infinite
            return null;
        }

        BigDecimal bigDecimal = new BigDecimal(sign * significand);
        bigDecimal = bigDecimal.scaleByPowerOfTen(exponent);
        return bigDecimal;
    }

/*

    public static void main(String[] args)
    {
        System.out.println(constructFrom128(0l,0l));
        System.out.println(constructFrom128(0x3041ED09BEAD87C0l,0x378D8E63FFFFFFFFl));
        System.out.println(constructFrom64(0l));
        System.out.println(constructFrom64(0x5fe0000000000001l));
        System.out.println(constructFrom64(0xec7386F26FC0FFFFl));
        System.out.println(constructFrom32(0));
        System.out.println(constructFrom32(0x6cb8967f));

    }
*/

    public static TypeConstructor getDecimal32Instance()
    {
        return DECIMAL_32;
    }

    public static TypeConstructor getDecimal64Instance()
    {
        return DECIMAL_64;
    }

    public static TypeConstructor getDecimal128Instance()
    {
        return DECIMAL_128;
    }
}
