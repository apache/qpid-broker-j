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

package org.apache.qpid.server.protocol.v0_10.transport;

import static org.apache.qpid.server.util.Serial.COMPARATOR;
import static org.apache.qpid.server.util.Serial.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * RangeSetTest
 */
class RangeSetTest extends UnitTestBase
{
    private void check(RangeSet ranges)
    {
        final List<Integer> posts = new ArrayList<>();
        for (final Range range : ranges)
        {
            posts.add(range.getLower());
            posts.add(range.getUpper());
        }

        final List<Integer> sorted = new ArrayList<>(posts);
        sorted.sort(COMPARATOR);

        assertEquals(posts, sorted);

        int idx = 1;
        while (idx + 1 < posts.size())
        {
            final boolean condition = !eq(posts.get(idx) + 1, posts.get(idx+1));
            assertTrue(condition);
            idx += 2;
        }
    }

    @Test
    void test1()
    {
        final RangeSet ranges = RangeSetFactory.createRangeSet();
        ranges.add(5, 10);
        check(ranges);
        ranges.add(15, 20);
        check(ranges);
        ranges.add(23, 25);
        check(ranges);
        ranges.add(12, 14);
        check(ranges);
        ranges.add(0, 1);
        check(ranges);
        ranges.add(3, 11);
        check(ranges);
    }

    @Test
    void test2()
    {
        final RangeSet rs = RangeSetFactory.createRangeSet();
        check(rs);

        rs.add(1);
        assertTrue(rs.includes(1));
        final boolean condition10 = !rs.includes(2);
        assertTrue(condition10);
        final boolean condition9 = !rs.includes(0);
        assertTrue(condition9);
        check(rs);

        rs.add(2);
        final boolean condition8 = !rs.includes(0);
        assertTrue(condition8);
        assertTrue(rs.includes(1));
        assertTrue(rs.includes(2));
        final boolean condition7 = !rs.includes(3);
        assertTrue(condition7);
        check(rs);

        rs.add(0);

        final boolean condition6 = !rs.includes(-1);
        assertTrue(condition6);
        assertTrue(rs.includes(0));
        assertTrue(rs.includes(1));
        assertTrue(rs.includes(2));
        final boolean condition5 = !rs.includes(3);
        assertTrue(condition5);
        check(rs);

        rs.add(37);

        final boolean condition4 = !rs.includes(-1);
        assertTrue(condition4);
        assertTrue(rs.includes(0));
        assertTrue(rs.includes(1));
        assertTrue(rs.includes(2));
        final boolean condition3 = !rs.includes(3);
        assertTrue(condition3);
        final boolean condition2 = !rs.includes(36);
        assertTrue(condition2);
        assertTrue(rs.includes(37));
        final boolean condition1 = !rs.includes(38);
        assertTrue(condition1);
        check(rs);

        rs.add(-1);
        check(rs);

        rs.add(-3);
        check(rs);

        rs.add(1, 20);
        final boolean condition = !rs.includes(21);
        assertTrue(condition);
        assertTrue(rs.includes(20));
        check(rs);
    }

    @Test
    void addSelf()
    {
        final RangeSet a = RangeSetFactory.createRangeSet();
        a.add(0, 8);
        check(a);
        a.add(0, 8);
        check(a);
        assertEquals(a.size(), (long) 1);
        Range range = a.iterator().next();
        assertEquals(range.getLower(), (long) 0);
        assertEquals(range.getUpper(), (long) 8);
    }

    @Test
    void intersect1()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(9, 20);
        final Range i1 = a.intersect(b);
        final Range i2 = b.intersect(a);
        assertEquals(i1.getUpper(), (long) 10);
        assertEquals(i2.getUpper(), (long) 10);
        assertEquals(i1.getLower(), (long) 9);
        assertEquals(i2.getLower(), (long) 9);
    }

    @Test
    void intersect2()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(11, 20);
        assertNull(a.intersect(b));
        assertNull(b.intersect(a));
    }

    @Test
    void intersect3()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(3, 5);
        final Range i1 = a.intersect(b);
        final Range i2 = b.intersect(a);
        assertEquals(i1.getUpper(), (long) 5);
        assertEquals(i2.getUpper(), (long) 5);
        assertEquals(i1.getLower(), (long) 3);
        assertEquals(i2.getLower(), (long) 3);
    }

    @Test
    void subtract1()
    {
        final Range a = Range.newInstance(0, 10);
        assertTrue(a.subtract(a).isEmpty());
    }

    @Test
    void subtract2()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(20, 30);
        final List<Range> ranges = a.subtract(b);
        assertEquals(ranges.size(), (long) 1);
        final Range d = ranges.get(0);
        assertEquals(d.getLower(), (long) a.getLower());
        assertEquals(d.getUpper(), (long) a.getUpper());
    }

    @Test
    void subtract3()
    {
        final Range a = Range.newInstance(20, 30);
        final Range b = Range.newInstance(0, 10);
        final List<Range> ranges = a.subtract(b);
        assertEquals(ranges.size(), (long) 1);
        final Range d = ranges.get(0);
        assertEquals(d.getLower(), (long) a.getLower());
        assertEquals(d.getUpper(), (long) a.getUpper());
    }

    @Test
    void subtract4()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(3, 5);
        final List<Range> ranges = a.subtract(b);
        assertEquals(ranges.size(), (long) 2);
        final Range low = ranges.get(0);
        final Range high = ranges.get(1);
        assertEquals(low.getLower(), (long) 0);
        assertEquals(low.getUpper(), (long) 2);
        assertEquals(high.getLower(), (long) 6);
        assertEquals(high.getUpper(), (long) 10);
    }

    @Test
    void subtract5()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(3, 20);
        final List<Range> ranges = a.subtract(b);
        assertEquals(ranges.size(), (long) 1);
        final Range d = ranges.get(0);
        assertEquals(d.getLower(), (long) 0);
        assertEquals(d.getUpper(), (long) 2);
    }

    @Test
    void subtract6()
    {
        final Range a = Range.newInstance(0, 10);
        final Range b = Range.newInstance(-10, 5);
        final List<Range> ranges = a.subtract(b);
        assertEquals(ranges.size(), (long) 1);
        final Range d = ranges.get(0);
        assertEquals(d.getLower(), (long) 6);
        assertEquals(d.getUpper(), (long) 10);
    }

    @Test
    void setSubtract1()
    {
        final RangeSet orig = createRangeSet(0, 10) ;
        final RangeSet update = createRangeSet(3, 15) ;
        orig.subtract(update) ;
        checkRange(orig, 0, 2) ;
    }

    @Test
    void setSubtract2()
    {
        final RangeSet orig = createRangeSet(0, 10) ;
        final RangeSet update = createRangeSet(3, 10) ;
        orig.subtract(update) ;
        checkRange(orig, 0, 2) ;
    }

    @Test
    void setSubtract3()
    {
        final RangeSet orig = createRangeSet(0, 10) ;
        final RangeSet update = createRangeSet(3, 4) ;
        orig.subtract(update) ;
        checkRange(orig, 0, 2, 5, 10) ;
    }

    @Test
    void setSubtract4()
    {
        final RangeSet orig = createRangeSet(3, 15) ;
        final RangeSet update = createRangeSet(0, 10) ;
        orig.subtract(update) ;
        checkRange(orig, 11, 15) ;
    }

    @Test
    void setSubtract5()
    {
        final RangeSet orig = createRangeSet(3, 10) ;
        final RangeSet update = createRangeSet(0, 10) ;
        orig.subtract(update) ;
        checkRange(orig) ;
    }

    @Test
    void setSubtract6()
    {
        final RangeSet orig = createRangeSet(3, 10) ;
        final RangeSet update = createRangeSet(0, 15) ;
        orig.subtract(update) ;
        checkRange(orig) ;
    }

    @Test
    void setSubtract7()
    {
        final RangeSet orig = createRangeSet(0, 10) ;
        final RangeSet update = createRangeSet(0, 15) ;
        orig.subtract(update) ;
        checkRange(orig) ;
    }

    @Test
    void setSubtract8()
    {
        final RangeSet orig = createRangeSet(0, 15) ;
        final RangeSet update = createRangeSet(0, 10) ;
        orig.subtract(update) ;
        checkRange(orig, 11, 15) ;
    }

    @Test
    void setSubtract9()
    {
        final RangeSet orig = createRangeSet(0, 15, 20, 30) ;
        final RangeSet update = createRangeSet(2, 3, 5, 6, 8, 9, 22, 23, 27, 28) ;
        orig.subtract(update) ;
        checkRange(orig, 0, 1, 4, 4, 7, 7, 10, 15, 20, 21, 24, 26, 29, 30) ;
    }

    @Test
    void setSubtract10()
    {
        final RangeSet orig = createRangeSet(0, 15, 20, 30) ;
        final RangeSet update = createRangeSet(0, 2, 4, 6, 10, 22, 24, 24, 27, 30) ;
        orig.subtract(update) ;
        checkRange(orig, 3, 3, 7, 9, 23, 23, 25, 26) ;
    }

    @Test
    void setSubtract11()
    {
        final RangeSet orig = createRangeSet(0, 2, 4, 6, 10, 22, 24, 24, 27, 30) ;
        final RangeSet update = createRangeSet(0, 2, 4, 6, 10, 22, 24, 24, 27, 30) ;
        orig.subtract(update) ;
        checkRange(orig) ;
    }
    
    private RangeSet createRangeSet(final int ... bounds)
    {
        final RangeSet set = RangeSetFactory.createRangeSet();
        final int length = (bounds == null ? 0 : bounds.length) ;
        int count = 0 ;
        while (count < length)
        {
            set.add(bounds[count++], bounds[count++]) ;
        }
        return set ;
    }
    
    private void checkRange(final RangeSet rangeSet, final int ... bounds)
    {
        final int length = (bounds == null ? 0 : bounds.length) ;
        assertEquals(length / 2, (long) rangeSet.size(), "Range count");
        final Iterator<Range> iter = rangeSet.iterator() ;
        int count = 0 ;
        while (count < length)
        {
            final Range range = iter.next() ;
            final long expected1 = bounds[count++];
            assertEquals(expected1, range.getLower(), "Range lower");
            final long expected = bounds[count++];
            assertEquals(expected, range.getUpper(), "Range upper");
        }
    }
}
