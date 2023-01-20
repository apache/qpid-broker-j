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
package org.apache.qpid.disttest.client.property;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class RangePropertyValueTest extends UnitTestBase
{
    private RangePropertyValue _generator;

    @BeforeEach
    public void setUp() throws Exception
    {
        _generator = new RangePropertyValue();
        _generator.setUpper(10.0);
        _generator.setLower(0.0);
        _generator.setStep(2.0);
        _generator.setType("double");
    }

    @Test
    public void testGetters()
    {
        assertEquals(10.0, (Object) _generator.getUpper(), "Unexpected upper boundary");
        assertEquals(0.0, (Object) _generator.getLower(), "Unexpected lower boundary");
        assertEquals(2.0, (Object) _generator.getStep(), "Unexpected step");
        assertEquals("double", _generator.getType(), "Unexpected type");
        assertTrue(_generator.isCyclic(), "Unexpected cyclic");
    }

    @Test
    public void testGetValue()
    {
        double[] expected = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
        for (int j = 0; j < 2; j++)
        {
            for (final double v : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Double;
                assertTrue(condition, "Should be Double");
                assertEquals(v, value, "Unexpected value ");
            }
        }
    }

    @Test
    public void testGetValueNonCyclic()
    {
        _generator.setCyclic(false);
        double[] expected = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
        for (final double v : expected)
        {
            Object value = _generator.getValue();
            final boolean condition = value instanceof Double;
            assertTrue(condition, "Should be Double");
            assertEquals(v, value, "Unexpected value ");
        }
        for (int i = 0; i < expected.length; i++)
        {
            Object value = _generator.getValue();
            assertEquals(expected[expected.length - 1], value, "Unexpected value ");
        }
    }

    @Test
    public void testGetValueInt()
    {
        _generator.setType("int");
        int[] expected = { 0, 2, 4, 6, 8, 10 };
        for (int j = 0; j < 2; j++)
        {
            for (final int k : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Integer;
                assertTrue(condition, "Should be Double");
                assertEquals(k, value, "Unexpected value ");
            }
        }
    }

    @Test
    public void testGetValueByte()
    {
        _generator.setType("byte");
        byte[] expected = { 0, 2, 4, 6, 8, 10 };
        for (int j = 0; j < 2; j++)
        {
            for (final byte b : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Byte;
                assertTrue(condition, "Should be Double");
                assertEquals(b, value, "Unexpected value ");
            }
        }
    }

    @Test
    public void testGetValueLong()
    {
        _generator.setType("long");
        long[] expected = { 0, 2, 4, 6, 8, 10 };
        for (int j = 0; j < 2; j++)
        {
            for (final long l : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Long;
                assertTrue(condition, "Should be Double");
                assertEquals(l, value, "Unexpected value ");
            }
        }
    }

    @Test
    public void testGetValueShort()
    {
        _generator.setType("short");
        short[] expected = { 0, 2, 4, 6, 8, 10 };
        for (int j = 0; j < 2; j++)
        {
            for (final short item : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Short;
                assertTrue(condition, "Should be Double");
                assertEquals(item, value, "Unexpected value ");
            }
        }
    }

    @Test
    public void testGetValueFloat()
    {
        _generator.setType("float");
        float[] expected = { 0.0f, 2.0f, 4.0f, 6.0f, 8.0f, 10.0f };
        for (int j = 0; j < 2; j++)
        {
            for (final float v : expected)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Float;
                assertTrue(condition, "Should be Double");
                assertEquals(v, value, "Unexpected value ");
            }
        }
    }
}
