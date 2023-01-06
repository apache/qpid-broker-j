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

public class RandomPropertyValueTest extends UnitTestBase
{
    private RandomPropertyValue _generator;

    @BeforeEach
    public void setUp() throws Exception
    {
        _generator = new RandomPropertyValue();
        _generator.setUpper(20.0);
        _generator.setLower(10.0);
        _generator.setType("double");
    }

    @Test
    public void testGetters()
    {
        assertEquals(20.0, (Object) _generator.getUpper(), "Unexpected upper boundary");
        assertEquals(10.0, (Object) _generator.getLower(), "Unexpected lower boundary");
        assertEquals("double", _generator.getType(), "Unexpected type");
    }

    @Test
    public void testGetValue()
    {
        Object value = _generator.getValue();
        final boolean condition = value instanceof Double;
        assertTrue(condition, "Unexpected type");
        assertTrue((Double) value >= 10.0 && (Double) value <= 20.0, "Unexpected value");
    }

    @Test
    public void testGetValueInt()
    {
        _generator.setType("int");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Integer;
        assertTrue(condition, "Unexpected type");
        assertTrue((Integer) value >= 10 && (Integer) value <= 20, "Unexpected value");
    }

    @Test
    public void testGetValueLong()
    {
        _generator.setType("long");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Long;
        assertTrue(condition, "Unexpected type");
        assertTrue((Long) value >= 10 && (Long) value <= 20, "Unexpected value");
    }

    @Test
    public void testGetValueFloat()
    {
        _generator.setType("float");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Float;
        assertTrue(condition, "Unexpected type");
        assertTrue((Float) value >= 10.0 && (Float) value <= 20.0, "Unexpected value");
    }
}
