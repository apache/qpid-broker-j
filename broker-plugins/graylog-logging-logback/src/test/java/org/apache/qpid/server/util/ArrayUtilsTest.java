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

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class ArrayUtilsTest extends TestCase
{
    @Test
    public void testClone_NullInput()
    {
        assertNull(ArrayUtils.clone(null));
    }

    @Test
    public void testClone_EmptyInput()
    {
        String[] data = new String[0];
        String[] result = ArrayUtils.clone(data);
        assertArrayEquals(data, result);
        assertNotSame(data, result);
    }

    @Test
    public void testClone()
    {
        String[] data = new String[]{"A", null, "B", "CC"};
        String[] result = ArrayUtils.clone(data);
        assertArrayEquals(data, result);
        assertNotSame(data, result);
    }

    @Test
    public void testIsEmpty_NullInput()
    {
        assertTrue(ArrayUtils.isEmpty(null));
    }

    @Test
    public void testIsEmpty_EmptyInput()
    {
        assertTrue(ArrayUtils.isEmpty(new String[0]));
    }

    @Test
    public void testIsEmpty()
    {
        assertFalse(ArrayUtils.isEmpty(new String[]{"A"}));
        assertFalse(ArrayUtils.isEmpty(new String[]{"A", "B"}));
        assertFalse(ArrayUtils.isEmpty(new String[]{null}));
    }
}
