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

package org.apache.qpid.server.protocol.v1_0.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class UnsignedNumberTest extends UnitTestBase
{
    private static final String TWO_TO_THE_THIRTY_ONE_MINUS_ONE = "2147483647";
    private static final String TWO_TO_THE_THIRTY_ONE = "2147483648";
    private static final String TWO_TO_THE_THIRTY_TWO_MINUS_ONE = "4294967295";
    private static final String TWO_TO_THE_THIRTY_TWO = "4294967296";
    private static final String TWO_TO_THE_SIXTY_THREE_MINUS_ONE = "9223372036854775807";
    private static final String TWO_TO_THE_SIXTY_THREE = "9223372036854775808";
    private static final String TWO_TO_THE_SIXTY_FOUR_MINUS_ONE = "18446744073709551615";
    private static final String TWO_TO_THE_SIXTY_FOUR = "18446744073709551616";

    @Test
    void unsignedIntegerParsingAcceptsBoundaryValues()
    {
        assertUnsignedIntegerRoundTrip("0", 0);
        assertUnsignedIntegerRoundTrip(TWO_TO_THE_THIRTY_ONE_MINUS_ONE, Integer.MAX_VALUE);
        assertUnsignedIntegerRoundTrip(TWO_TO_THE_THIRTY_ONE, Integer.MIN_VALUE);
        assertUnsignedIntegerRoundTrip(TWO_TO_THE_THIRTY_TWO_MINUS_ONE, -1);
    }

    @Test
    void unsignedIntegerParsingRejectsOutOfRangeValues()
    {
        assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf(TWO_TO_THE_THIRTY_TWO));
        assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf("-1"));
    }

    @Test
    void unsignedLongParsingAcceptsBoundaryValues()
    {
        assertUnsignedLongRoundTrip("0", 0L);
        assertUnsignedLongRoundTrip(TWO_TO_THE_SIXTY_THREE_MINUS_ONE, Long.MAX_VALUE);
        assertUnsignedLongRoundTrip(TWO_TO_THE_SIXTY_THREE, Long.MIN_VALUE);
        assertUnsignedLongRoundTrip(TWO_TO_THE_SIXTY_FOUR_MINUS_ONE, -1L);
    }

    @Test
    void unsignedLongParsingRejectsOutOfRangeValues()
    {
        assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf(TWO_TO_THE_SIXTY_FOUR));
        assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf("-1"));
    }

    @Test
    void unsignedByteComparisonCrossesSignedBoundary()
    {
        assertUnsignedOrder(UnsignedByte.valueOf(Byte.MAX_VALUE), UnsignedByte.valueOf(Byte.MIN_VALUE));
    }

    @Test
    void unsignedShortComparisonCrossesSignedBoundary()
    {
        assertUnsignedOrder(UnsignedShort.valueOf(Short.MAX_VALUE), UnsignedShort.valueOf(Short.MIN_VALUE));
    }

    @Test
    void unsignedIntegerComparisonCrossesSignedBoundary()
    {
        assertUnsignedOrder(UnsignedInteger.valueOf(Integer.MAX_VALUE), UnsignedInteger.valueOf(Integer.MIN_VALUE));
    }

    @Test
    void unsignedLongComparisonCrossesSignedBoundary()
    {
        assertUnsignedOrder(UnsignedLong.valueOf(Long.MAX_VALUE), UnsignedLong.valueOf(Long.MIN_VALUE));
    }

    private static void assertUnsignedIntegerRoundTrip(final String encoded, final int underlying)
    {
        final UnsignedInteger value = UnsignedInteger.valueOf(encoded);

        assertEquals(underlying, value.intValue());
        assertEquals(encoded, value.toString());
    }

    private static void assertUnsignedLongRoundTrip(final String encoded, final long underlying)
    {
        final UnsignedLong value = UnsignedLong.valueOf(encoded);

        assertEquals(underlying, value.longValue());
        assertEquals(encoded, value.toString());
    }

    private static <T extends Comparable<T>> void assertUnsignedOrder(final T lower, final T higher)
    {
        assertTrue(lower.compareTo(higher) < 0);
        assertTrue(higher.compareTo(lower) > 0);
    }
}
