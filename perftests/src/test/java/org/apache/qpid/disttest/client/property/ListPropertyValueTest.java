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

import java.util.ArrayList;
import java.util.List;

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

public class ListPropertyValueTest extends UnitTestBase
{
    private ListPropertyValue _generator;
    private List<PropertyValue> _items;

    @Before
    public void setUp() throws Exception
    {
        _generator = new ListPropertyValue();
        _items = new ArrayList<PropertyValue>();
        _items.add(new SimplePropertyValue(new Integer(1)));
        _items.add(new SimplePropertyValue(new Double(2.1)));
        _items.add(new SimplePropertyValue(new Boolean(true)));
        ListPropertyValue innerList = new ListPropertyValue();
        List<PropertyValue> innerListItems = new ArrayList<PropertyValue>();
        innerListItems.add(new SimplePropertyValue("test"));
        innerListItems.add(new SimplePropertyValue(new Integer(2)));
        innerList.setItems(innerListItems);
        _items.add(innerList);
        _generator.setItems(_items);
    }

    @Test
    public void testGetItems()
    {
        List<? extends Object> items = _generator.getItems();
        assertEquals("Unexpected list items", _items, items);
    }

    @Test
    public void testGetValue()
    {
        for (int i = 0; i < 2; i++)
        {
            assertEquals("Unexpected first item", new Integer(1), _generator.getValue());
            assertEquals("Unexpected second item", new Double(2.1), _generator.getValue());
            assertEquals("Unexpected third item", new Boolean(true), _generator.getValue());
            if (i == 0)
            {
                assertEquals("Unexpected forth item", "test", _generator.getValue());
            }
            else
            {
                assertEquals("Unexpected forth item", new Integer(2), _generator.getValue());
            }
        }
    }

    @Test
    public void testNonCyclicGetValue()
    {
        _generator.setCyclic(false);
        assertFalse("Generator should not be cyclic", _generator.isCyclic());
        assertEquals("Unexpected first item", new Integer(1), _generator.getValue());
        assertEquals("Unexpected second item", new Double(2.1), _generator.getValue());
        assertEquals("Unexpected third item", new Boolean(true), _generator.getValue());
        assertEquals("Unexpected forth item", "test", _generator.getValue());
        assertEquals("Unexpected fifth item", new Integer(2), _generator.getValue());
        assertEquals("Unexpected sixs item", "test", _generator.getValue());
        assertEquals("Unexpected sevens item", new Integer(2), _generator.getValue());
    }
}
