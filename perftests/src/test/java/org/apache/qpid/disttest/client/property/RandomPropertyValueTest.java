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

public class RandomPropertyValueTest extends UnitTestBase
{
    private RandomPropertyValue _generator;

    @Before
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
        assertEquals("Unexpected upper boundary", new Double(20.0), (Object) _generator.getUpper());
        assertEquals("Unexpected lower boundary", new Double(10.0), (Object) _generator.getLower());
        assertEquals("Unexpected type", "double", _generator.getType());
    }

    @Test
    public void testGetValue()
    {
        Object value = _generator.getValue();
        final boolean condition = value instanceof Double;
        assertTrue("Unexpected type", condition);
        assertTrue("Unexpected value", ((Double) value).doubleValue() >= 10.0
                                              && ((Double) value).doubleValue() <= 20.0);
    }

    @Test
    public void testGetValueInt()
    {
        _generator.setType("int");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Integer;
        assertTrue("Unexpected type", condition);
        assertTrue("Unexpected value", ((Integer) value).intValue() >= 10 && ((Integer) value).intValue() <= 20);
    }

    @Test
    public void testGetValueLong()
    {
        _generator.setType("long");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Long;
        assertTrue("Unexpected type", condition);
        assertTrue("Unexpected value", ((Long) value).longValue() >= 10 && ((Long) value).longValue() <= 20);
    }

    @Test
    public void testGetValueFloat()
    {
        _generator.setType("float");
        Object value = _generator.getValue();
        final boolean condition = value instanceof Float;
        assertTrue("Unexpected type", condition);
        assertTrue("Unexpected value",
                          ((Float) value).floatValue() >= 10.0 && ((Float) value).floatValue() <= 20.0);
    }
}
