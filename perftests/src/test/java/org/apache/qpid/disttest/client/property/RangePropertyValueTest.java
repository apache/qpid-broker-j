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

import org.junit.Assert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RangePropertyValueTest extends UnitTestBase
{
    private RangePropertyValue _generator;

    @Before
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
        assertEquals("Unexpected upper boundary", new Double(10.0), (Object) _generator.getUpper());
        assertEquals("Unexpected lower boundary", new Double(0.0), (Object) _generator.getLower());
        assertEquals("Unexpected step", new Double(2.0), (Object) _generator.getStep());
        assertEquals("Unexpected type", "double", _generator.getType());
        assertTrue("Unexpected cyclic", _generator.isCyclic());
    }

    @Test
    public void testGetValue()
    {
        double[] expected = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Double;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
            }
        }
    }

    @Test
    public void testGetValueNonCyclic()
    {
        _generator.setCyclic(false);
        double[] expected = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
        for (int i = 0; i < expected.length; i++)
        {
            Object value = _generator.getValue();
            final boolean condition = value instanceof Double;
            assertTrue("Should be Double", condition);
            assertEquals("Unexpected value ", expected[i], value);
        }
        for (int i = 0; i < expected.length; i++)
        {
            Object value = _generator.getValue();
            assertEquals("Unexpected value ", expected[expected.length - 1], value);
        }
    }

    @Test
    public void testGetValueInt()
    {
        _generator.setType("int");
        int[] expected = { 0, 2, 4, 6, 8, 10 };
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Integer;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
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
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Byte;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
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
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Long;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
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
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Short;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
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
            for (int i = 0; i < expected.length; i++)
            {
                Object value = _generator.getValue();
                final boolean condition = value instanceof Float;
                assertTrue("Should be Double", condition);
                assertEquals("Unexpected value ", expected[i], value);
            }
        }
    }
}
