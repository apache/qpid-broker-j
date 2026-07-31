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

package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class SequenceNumberTest extends UnitTestBase
{
    @Test
    void compareToEqual()
    {
        assertEquals(0, new SequenceNumber(5).compareTo(new SequenceNumber(5)));
    }

    @Test
    void compareToLess()
    {
        assertTrue(new SequenceNumber(5).compareTo(new SequenceNumber(10)) < 0);
    }

    @Test
    void compareToGreater()
    {
        assertTrue(new SequenceNumber(10).compareTo(new SequenceNumber(5)) > 0);
    }

    @Test
    void compareToWrapAround()
    {
        final SequenceNumber unsignedMax = new SequenceNumber(-1);  // unsigned 4294967295
        final SequenceNumber zero = new SequenceNumber(0);
        assertTrue(zero.compareTo(unsignedMax) > 0, "0 should be 'after' 0xFFFFFFFF in serial arithmetic");
        assertTrue(unsignedMax.compareTo(zero) < 0, "0xFFFFFFFF should be 'before' 0 in serial arithmetic");
    }

    @Test
    void compareToReturnsNormalized()
    {
        // Verify compareTo returns -1, 0, or 1 (via Integer.signum)
        final SequenceNumber a = new SequenceNumber(100);
        final SequenceNumber b = new SequenceNumber(200);
        assertEquals(-1, a.compareTo(b));
        assertEquals(1, b.compareTo(a));
        assertEquals(0, a.compareTo(new SequenceNumber(100)));
    }

    @Test
    void longValueUnsigned()
    {
        assertEquals(0L, new SequenceNumber(0).longValue());
        assertEquals(4294967295L, new SequenceNumber(-1).longValue());
        assertEquals(2147483648L, new SequenceNumber(Integer.MIN_VALUE).longValue());
    }

    @Test
    void incrAndDecr()
    {
        final SequenceNumber sn = new SequenceNumber(5);
        sn.incr();
        assertEquals(6, sn.intValue());
        sn.decr();
        assertEquals(5, sn.intValue());
    }
}
