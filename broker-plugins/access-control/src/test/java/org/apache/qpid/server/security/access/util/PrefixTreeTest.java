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
package org.apache.qpid.server.security.access.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class PrefixTreeTest extends UnitTestBase
{
    @Test
    void prefixWithWildcard_Single()
    {
        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcd"));
        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcd").mergeWithPrefix("abcd"));
        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcdXYZ").mergeWithPrefix("abcd"));
        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcd").mergeWithPrefix("abcdXYZ"));

        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcd").mergeWithFinalValue("abcd"));
        testPrefixWithWildcard_Single(PrefixTree.fromFinalValue("abcd").mergeWithPrefix("abcd"));
        testPrefixWithWildcard_Single(PrefixTree.fromPrefixWithWildCard("abcd").mergeWithFinalValue("abcdXYZ"));
        testPrefixWithWildcard_Single(PrefixTree.fromFinalValue("abcdXYZ").mergeWithPrefix("abcd"));
    }

    private void testPrefixWithWildcard_Single(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(1, tree.size());
        for (final String str : tree)
        {
            assertEquals("abcd*", str);
        }
        assertEquals("abcd", tree.prefix());
        assertEquals('a', tree.firstPrefixCharacter());
        assertNotNull(tree.branches());
        assertTrue(tree.branches().isEmpty());

        assertTrue(tree.match("abcd"));
        assertTrue(tree.match("abcd.e"));
        assertFalse(tree.match("Abcdx"));
        assertFalse(tree.match("abc"));
        assertFalse(tree.match("ab"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void prefixWithWildcard()
    {
        final String[] strings = new String[]{"exchange.public.*", "exchange.private.*", "response.public.*", "response.private.*", "response.*"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2])
                    .mergeWith(strs[3])
                    .mergeWith(strs[4]);
            testPrefixWithWildcard(tree);
            testPrefixWithWildcard(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testPrefixWithWildcard(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(3, tree.size());
        final String[] array = new String[3];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"exchange.private.*", "exchange.public.*", "response.*"}, array);
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());

        PrefixTree branch = tree.branches().get('e');
        assertNotNull(branch);
        assertEquals('e', branch.firstPrefixCharacter());
        assertEquals("exchange.p", branch.prefix());
        assertEquals(2, branch.size());
        assertEquals(2, branch.branches().size());
        assertNotNull(branch.branches().get('r'));
        assertEquals("rivate.", branch.branches().get('r').prefix());
        assertNotNull(branch.branches().get('u'));
        assertEquals("ublic.", branch.branches().get('u').prefix());

        branch = tree.branches().get('r');
        assertNotNull(branch);
        assertEquals('r', branch.firstPrefixCharacter());
        assertEquals("response.", branch.prefix());
        assertEquals(1, branch.size());
        assertEquals(0, branch.branches().size());

        assertTrue(tree.match("response.x"));
        assertTrue(tree.match("response."));
        assertTrue(tree.match("exchange.private.A"));
        assertTrue(tree.match("exchange.private."));
        assertTrue(tree.match("exchange.public.B"));
        assertTrue(tree.match("exchange.public."));
        assertFalse(tree.match("response"));
        assertFalse(tree.match("exchange.private"));
        assertFalse(tree.match("exchange.public"));

        assertFalse(tree.match("exchange.rest"));
        assertFalse(tree.match("reg"));
        assertFalse(tree.match("error"));
        assertFalse(tree.match("warning"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void prefixWithWildcard_RootWith3Branches()
    {
        final String[] strings = new String[]{"A", "B", "C"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.fromPrefixWithWildCard(strs[0])
                    .mergeWithPrefix(strs[1])
                    .mergeWithPrefix(strs[2]);
            testPrefixWithWildcard_RootWith3Branches(tree);
        }
    }

    private void testPrefixWithWildcard_RootWith3Branches(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(3, tree.size());
        final String[] array = new String[3];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"A*", "B*", "C*"}, array);
        assertNotNull(tree.branches());
        assertEquals(3, tree.branches().size());

        PrefixTree branch = tree.branches().get('A');
        assertNotNull(branch);
        assertEquals('A', branch.firstPrefixCharacter());
        assertEquals("A", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('B');
        assertNotNull(branch);
        assertEquals('B', branch.firstPrefixCharacter());
        assertEquals("B", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', branch.firstPrefixCharacter());
        assertEquals("C", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("A"));
        assertTrue(tree.match("Ax"));
        assertTrue(tree.match("B"));
        assertTrue(tree.match("Bx"));
        assertTrue(tree.match("C"));
        assertTrue(tree.match("Cx"));

        assertFalse(tree.match("x"));
        assertFalse(tree.match("b"));
        assertFalse(tree.match("cC"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void prefixWithWildcard_BranchSplit()
    {
        final String[] strings = new String[]{"AB*", "AC*"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0]).mergeWith(strs[1]);
            testPrefixWithWildcard_BranchSplit(tree);
            testPrefixWithWildcard_BranchSplit(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testPrefixWithWildcard_BranchSplit(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(2, tree.size());
        final String[] array = new String[2];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"AB*", "AC*"}, array);
        assertEquals(2, tree.size());
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());

        assertEquals('A', tree.firstPrefixCharacter());
        assertEquals("A", tree.prefix());

        PrefixTree branch = tree.branches().get('B');
        assertNotNull(branch);
        assertEquals('B', branch.firstPrefixCharacter());
        assertEquals("B", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', branch.firstPrefixCharacter());
        assertEquals("C", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("AB"));
        assertTrue(tree.match("ABx"));
        assertTrue(tree.match("AC"));
        assertTrue(tree.match("ACx"));

        assertFalse(tree.match("A"));
        assertFalse(tree.match("Ab"));
        assertFalse(tree.match("Ac"));
        assertFalse(tree.match("b"));
        assertFalse(tree.match("cC"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void exactString_Single()
    {
        testExactString_Single(PrefixTree.fromFinalValue("abcd"));
        testExactString_Single(PrefixTree.fromFinalValue("abcd").mergeWithFinalValue("abcd"));
    }

    private void testExactString_Single(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(1, tree.size());
        for (final String str : tree)
        {
            assertEquals("abcd", str);
        }
        assertEquals("abcd", tree.prefix());
        assertEquals('a', tree.firstPrefixCharacter());
        assertNotNull(tree.branches());
        assertTrue(tree.branches().isEmpty());

        assertTrue(tree.match("abcd"));
        assertFalse(tree.match("aBcd"));
        assertFalse(tree.match("abcd."));
        assertFalse(tree.match("abc"));
        assertFalse(tree.match("x"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void exactString()
    {
        final String[] strings = new String[]{"exchange.public", "exchange.private", "response.public", "response.private", "response"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2])
                    .mergeWith(strs[3])
                    .mergeWith(strs[4]);
            testExactString(tree);
            testExactString(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testExactString(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(5, tree.size());
        final String[] array = new String[5];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"exchange.private", "exchange.public", "response", "response.private", "response.public"},
                array);
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());

        PrefixTree branch = tree.branches().get('e');
        assertNotNull(branch);
        assertEquals('e', branch.firstPrefixCharacter());
        assertEquals("exchange.p", branch.prefix());
        assertEquals(2, branch.size());
        assertEquals(2, branch.branches().size());
        assertNotNull(branch.branches().get('r'));
        assertEquals("rivate", branch.branches().get('r').prefix());
        assertNotNull(branch.branches().get('u'));
        assertEquals("ublic", branch.branches().get('u').prefix());

        branch = tree.branches().get('r');
        assertNotNull(branch);
        assertEquals('r', branch.firstPrefixCharacter());
        assertEquals("response", branch.prefix());
        assertEquals(3, branch.size());
        assertEquals(1, branch.branches().size());

        branch = branch.branches().get('.');
        assertNotNull(branch);
        assertEquals('.', branch.firstPrefixCharacter());
        assertEquals(".p", branch.prefix());
        assertEquals(2, branch.size());
        assertEquals(2, branch.branches().size());

        assertNotNull(branch.branches().get('r'));
        assertEquals("rivate", branch.branches().get('r').prefix());
        assertNotNull(branch.branches().get('u'));
        assertEquals("ublic", branch.branches().get('u').prefix());

        assertTrue(tree.match("exchange.private"));
        assertTrue(tree.match("exchange.public"));
        assertTrue(tree.match("response"));
        assertTrue(tree.match("response.private"));
        assertTrue(tree.match("response.public"));

        assertFalse(tree.match("exchange.privat"));
        assertFalse(tree.match("exchange.privateX"));
        assertFalse(tree.match("exchange.publi"));
        assertFalse(tree.match("exchange.publicX"));
        assertFalse(tree.match("respons"));
        assertFalse(tree.match("response.p"));
        assertFalse(tree.match("response.privat"));
        assertFalse(tree.match("response.privateX"));
        assertFalse(tree.match("response.publi"));
        assertFalse(tree.match("response.publicX"));

        assertFalse(tree.match("exchange.rest"));
        assertFalse(tree.match("reg"));
        assertFalse(tree.match("error"));
        assertFalse(tree.match("warning"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void exactString_RootWith3Branches()
    {
        final String[] strings = new String[]{"A", "B", "C"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.fromFinalValue(strs[0])
                    .mergeWithFinalValue(strs[1])
                    .mergeWithFinalValue(strs[2]);
            testExactString_RootWith3Branches(tree);
        }
    }

    private void testExactString_RootWith3Branches(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(3, tree.size());
        final String[] array = new String[3];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"A", "B", "C"}, array);
        assertNotNull(tree.branches());
        assertEquals(3, tree.branches().size());

        PrefixTree branch = tree.branches().get('A');
        assertNotNull(branch);
        assertEquals('A', branch.firstPrefixCharacter());
        assertEquals("A", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('B');
        assertNotNull(branch);
        assertEquals('B', branch.firstPrefixCharacter());
        assertEquals("B", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', branch.firstPrefixCharacter());
        assertEquals("C", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("A"));
        assertTrue(tree.match("B"));
        assertTrue(tree.match("C"));

        assertFalse(tree.match("Ax"));
        assertFalse(tree.match("b"));
        assertFalse(tree.match("Cc"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void exactString_BranchSpit()
    {
        final String[] strings = new String[]{"A", "AB", "AC"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2]);
            testExactString_BranchSpit(tree);
            testExactString_BranchSpit(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testExactString_BranchSpit(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(3, tree.size());
        final String[] array = new String[3];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"A", "AB", "AC"}, array);
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());
        assertEquals("A", tree.prefix());
        assertEquals('A', tree.firstPrefixCharacter());

        PrefixTree branch = tree.branches().get('B');
        assertNotNull(branch);
        assertEquals('B', branch.firstPrefixCharacter());
        assertEquals("B", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', branch.firstPrefixCharacter());
        assertEquals("C", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("A"));
        assertTrue(tree.match("AB"));
        assertTrue(tree.match("AC"));

        assertFalse(tree.match("Ax"));
        assertFalse(tree.match("Ab"));
        assertFalse(tree.match("Ac"));
        assertFalse(tree.match("aa"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void mixing()
    {
        final String[] strings = new String[]{"exchange.public", "exchange.private.A", "exchange.private.*", "response.public", "response.private", "response.p*", "response"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2])
                    .mergeWith(strs[3])
                    .mergeWith(strs[4])
                    .mergeWith(strs[5])
                    .mergeWith(strs[6]);
            testMixing(tree);
            testMixing(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testMixing(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(4, tree.size());
        final String[] array = new String[4];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"exchange.private.*", "exchange.public", "response", "response.p*"}, array);
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());

        PrefixTree branch = tree.branches().get('e');
        assertNotNull(branch);
        assertEquals('e', branch.firstPrefixCharacter());
        assertEquals("exchange.p", branch.prefix());
        assertEquals(2, branch.size());
        assertEquals(2, branch.branches().size());
        assertNotNull(branch.branches().get('r'));
        assertEquals("rivate.", branch.branches().get('r').prefix());
        assertNotNull(branch.branches().get('u'));
        assertEquals("ublic", branch.branches().get('u').prefix());

        branch = tree.branches().get('r');
        assertNotNull(branch);
        assertEquals('r', branch.firstPrefixCharacter());
        assertEquals("response", branch.prefix());
        assertEquals(2, branch.size());
        assertEquals(1, branch.branches().size());

        branch = branch.branches().get('.');
        assertNotNull(branch);
        assertEquals('.', branch.firstPrefixCharacter());
        assertEquals(".p", branch.prefix());
        assertEquals(1, branch.size());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("exchange.private.A"));
        assertTrue(tree.match("exchange.private."));
        assertTrue(tree.match("exchange.public"));
        assertTrue(tree.match("response"));
        assertTrue(tree.match("response.private"));
        assertTrue(tree.match("response.public"));
        assertTrue(tree.match("response.p"));

        assertFalse(tree.match("exchange.privat"));
        assertFalse(tree.match("exchange.privateX"));
        assertFalse(tree.match("exchange.publi"));
        assertFalse(tree.match("exchange.publicX"));
        assertFalse(tree.match("respons"));
        assertFalse(tree.match("response."));

        assertFalse(tree.match("exchange.rest"));
        assertFalse(tree.match("reg"));
        assertFalse(tree.match("error"));
        assertFalse(tree.match("warning"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void mixing_BranchSplit()
    {
        final String[] strings = new String[]{"AB*", "AC*", "AD", "AE"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2])
                    .mergeWith(strs[3]);
            testMixing_BranchSplit(tree);
            testMixing_BranchSplit(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testMixing_BranchSplit(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(4, tree.size());
        final String[] array = new String[4];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"AB*", "AC*", "AD", "AE"}, array);
        assertNotNull(tree.branches());
        assertEquals(4, tree.branches().size());

        assertEquals('A', tree.firstPrefixCharacter());
        assertEquals("A", tree.prefix());

        PrefixTree branch = tree.branches().get('B');
        assertNotNull(branch);
        assertEquals('B', branch.firstPrefixCharacter());
        assertEquals("B", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', branch.firstPrefixCharacter());
        assertEquals("C", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('D');
        assertNotNull(branch);
        assertEquals('D', branch.firstPrefixCharacter());
        assertEquals("D", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        branch = tree.branches().get('E');
        assertNotNull(branch);
        assertEquals('E', branch.firstPrefixCharacter());
        assertEquals("E", branch.prefix());
        assertTrue(branch.branches().isEmpty());

        assertTrue(tree.match("AB"));
        assertTrue(tree.match("ABx"));
        assertTrue(tree.match("AC"));
        assertTrue(tree.match("ACx"));
        assertTrue(tree.match("AD"));
        assertTrue(tree.match("AE"));

        assertFalse(tree.match("A"));
        assertFalse(tree.match("Ab"));
        assertFalse(tree.match("Ac"));
        assertFalse(tree.match("Ad"));
        assertFalse(tree.match("aE"));
        assertFalse(tree.match("ADx"));
        assertFalse(tree.match("b"));
        assertFalse(tree.match("cC"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void mixing_BranchSplit2()
    {
        final String[] strings = new String[]{"AXB*", "AXC*", "AYD", "AYE"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(strs[0])
                    .mergeWith(strs[1])
                    .mergeWith(strs[2])
                    .mergeWith(strs[3]);
            testMixing_BranchSplit2(tree);
            testMixing_BranchSplit2(PrefixTree.from(Arrays.asList(strs)));
        }
    }

    private void testMixing_BranchSplit2(PrefixTree tree)
    {
        assertNotNull(tree);
        assertEquals(4, tree.size());
        final String[] array = new String[4];
        int i = 0;
        for (final String str : tree)
        {
            array[i++] = str;
        }
        assertArrayEquals(new String[]{"AXB*", "AXC*", "AYD", "AYE"}, array);
        assertNotNull(tree.branches());
        assertEquals(2, tree.branches().size());

        assertEquals('A', tree.firstPrefixCharacter());
        assertEquals("A", tree.prefix());

        PrefixTree branch = tree.branches().get('X');
        assertNotNull(branch);
        assertEquals('X', branch.firstPrefixCharacter());
        assertEquals("X", branch.prefix());
        assertEquals(2, branch.branches().size());

        PrefixTree subBranch = branch.branches().get('B');
        assertNotNull(subBranch);
        assertEquals('B', subBranch.firstPrefixCharacter());
        assertEquals("B", subBranch.prefix());
        assertTrue(subBranch.branches().isEmpty());

        subBranch = branch.branches().get('C');
        assertNotNull(branch);
        assertEquals('C', subBranch.firstPrefixCharacter());
        assertEquals("C", subBranch.prefix());
        assertTrue(subBranch.branches().isEmpty());

        branch = tree.branches().get('Y');
        assertNotNull(branch);
        assertEquals('Y', branch.firstPrefixCharacter());
        assertEquals("Y", branch.prefix());
        assertEquals(2, branch.branches().size());

        subBranch = branch.branches().get('D');
        assertNotNull(subBranch);
        assertEquals('D', subBranch.firstPrefixCharacter());
        assertEquals("D", subBranch.prefix());
        assertTrue(subBranch.branches().isEmpty());

        subBranch = branch.branches().get('E');
        assertNotNull(subBranch);
        assertEquals('E', subBranch.firstPrefixCharacter());
        assertEquals("E", subBranch.prefix());
        assertTrue(subBranch.branches().isEmpty());

        assertTrue(tree.match("AXB"));
        assertTrue(tree.match("AXBx"));
        assertTrue(tree.match("AXC"));
        assertTrue(tree.match("AXCx"));
        assertTrue(tree.match("AYD"));
        assertTrue(tree.match("AYE"));

        assertFalse(tree.match("A"));
        assertFalse(tree.match("AXb"));
        assertFalse(tree.match("AXc"));
        assertFalse(tree.match("AYd"));
        assertFalse(tree.match("aYE"));
        assertFalse(tree.match("AYDx"));
        assertFalse(tree.match("b"));
        assertFalse(tree.match("cC"));
        assertFalse(tree.match(""));
        assertFalse(tree.match(null));
    }

    @Test
    void from_Exception()
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.from((String) null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.from(""),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void fromFinalValue_Exception()
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.fromFinalValue(null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.from(""),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void fromPrefixWithWildCard_Exception()
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.fromPrefixWithWildCard(null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
                () -> PrefixTree.fromPrefixWithWildCard(""),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void mergeWith_Exception()
    {
        final PrefixTree tree = PrefixTree.from("A");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> tree.mergeWith((String) null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
                () -> tree.mergeWith(""),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void mergeWithPrefix_Exception()
    {
        final PrefixTree tree = PrefixTree.from("A");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> tree.mergeWithPrefix(null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
            () -> tree.mergeWithPrefix(""),
            "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void mergeWithFinalValue_Exception()
    {
        final PrefixTree tree = PrefixTree.from("A");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> tree.mergeWithFinalValue(null),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());

        thrown = assertThrows(IllegalArgumentException.class,
                () -> tree.mergeWithFinalValue(""),
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    @Test
    void firstPrefixCharacter()
    {
        final PrefixTree tree = PrefixTree.fromFinalValue("A").mergeWithFinalValue("B");

        final UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class,
                tree::firstPrefixCharacter,
                "Expected exception not thrown");
        assertNotNull(thrown.getMessage());
    }

    private List<String[]> permute(final String[] array, final int startIndex)
    {
        final List<String[]> result = new ArrayList<>();
        result.add(array);
        for (int i = startIndex + 1; i < array.length; i++)
        {
            final String[] copy = Arrays.copyOf(array, array.length);
            final String aux = copy[startIndex];
            copy[startIndex] = copy[i];
            copy[i] = aux;
            result.addAll(permute(copy, startIndex + 1));
        }
        return result;
    }

    @Test
    void iterator()
    {
        final String[] strings = new String[]{"AXB*", "AXC*", "AYD", "AYE", "D"};
        for (final String[] strs : permute(strings, 0))
        {
            final PrefixTree tree = PrefixTree.from(Arrays.asList(strs));
            testIterator(tree, strings);
        }
    }

    private void testIterator(final PrefixTree tree, final String[] strings)
    {
        assertNotNull(tree);

        final List<String> list = StreamSupport.stream(tree.spliterator(), false).collect(Collectors.toList());
        assertEquals(Arrays.asList(strings), list);

        final Iterator<String> iterator = tree.iterator();
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        for (final String str : strings)
        {
            assertEquals(str, iterator.next());
        }
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());

        assertThrows(NoSuchElementException.class, iterator::next, "An exception is expected");
    }
}