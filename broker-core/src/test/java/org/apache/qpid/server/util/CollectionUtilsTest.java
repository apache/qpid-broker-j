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

package org.apache.qpid.server.util;

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CollectionUtilsTest extends UnitTestBase
{
    @Test
    public void testIsEmpty()
    {
        assertTrue(CollectionUtils.isEmpty(null));
        assertTrue(CollectionUtils.isEmpty(Collections.<String>emptyList()));
        assertTrue(CollectionUtils.isEmpty(Collections.<Integer>emptySet()));
        assertFalse(CollectionUtils.isEmpty(Collections.singletonList("A")));
        assertFalse(CollectionUtils.isEmpty(Collections.singleton(234)));
    }

    @Test
    public void testIsSingle()
    {
        assertFalse(CollectionUtils.isSingle(Collections.<String>emptyList()));
        assertFalse(CollectionUtils.isSingle(Collections.<Integer>emptySet()));
        assertTrue(CollectionUtils.isSingle(Collections.singletonList("A")));
        assertTrue(CollectionUtils.isSingle(Collections.singleton(234)));
        assertFalse(CollectionUtils.isSingle(Arrays.asList(234, 3456)));
    }

    @Test
    public void testImmutable_NullAsInput()
    {
        Set<Object> result = CollectionUtils.immutable(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
        try
        {
            result.add("A");
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
    }

    @Test
    public void testImmutable_EmptyInput()
    {
        Set<String> set = new HashSet<>();
        Set<String> result = CollectionUtils.immutable(set);
        assertNotNull(result);
        assertTrue(result.isEmpty());
        try
        {
            result.add("A");
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
    }

    @Test
    public void testImmutable_SingleInput()
    {
        Set<String> set = new HashSet<>();
        set.add("B");
        Set<String> result = CollectionUtils.immutable(set);
        assertNotNull(result);
        assertEquals(set, result);
        try
        {
            result.add("A");
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
        try
        {
            result.clear();
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
        try
        {
            result.remove(result.iterator().next());
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
    }

    @Test
    public void testImmutable_MultipleInputs()
    {
        Set<String> set = new HashSet<>();
        set.add("B");
        set.add("C");
        Set<String> result = CollectionUtils.immutable(set);
        assertNotNull(result);
        assertEquals(set, result);
        try
        {
            result.add("A");
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
        try
        {
            result.clear();
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
        try
        {
            result.remove(result.iterator().next());
            fail("An exception is expected");
        }
        catch (RuntimeException ignored)
        {
        }
    }
}