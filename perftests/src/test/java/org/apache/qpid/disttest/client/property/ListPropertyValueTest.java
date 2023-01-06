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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ListPropertyValueTest extends UnitTestBase
{
    private ListPropertyValue _generator;
    private List<PropertyValue> _items;

    @BeforeEach
    public void setUp() throws Exception
    {
        _generator = new ListPropertyValue();
        _items = new ArrayList<>();
        _items.add(new SimplePropertyValue(1));
        _items.add(new SimplePropertyValue(2.1));
        _items.add(new SimplePropertyValue(Boolean.TRUE));
        ListPropertyValue innerList = new ListPropertyValue();
        List<PropertyValue> innerListItems = new ArrayList<>();
        innerListItems.add(new SimplePropertyValue("test"));
        innerListItems.add(new SimplePropertyValue(2));
        innerList.setItems(innerListItems);
        _items.add(innerList);
        _generator.setItems(_items);
    }

    @Test
    public void testGetItems()
    {
        List<? extends Object> items = _generator.getItems();
        assertEquals(_items, items, "Unexpected list items");
    }

    @Test
    public void testGetValue()
    {
        for (int i = 0; i < 2; i++)
        {
            assertEquals(1, _generator.getValue(), "Unexpected first item");
            assertEquals(2.1, _generator.getValue(), "Unexpected second item");
            assertEquals(Boolean.TRUE, _generator.getValue(), "Unexpected third item");
            if (i == 0)
            {
                assertEquals("test", _generator.getValue(), "Unexpected forth item");
            }
            else
            {
                assertEquals(2, _generator.getValue(), "Unexpected forth item");
            }
        }
    }

    @Test
    public void testNonCyclicGetValue()
    {
        _generator.setCyclic(false);
        assertFalse(_generator.isCyclic(), "Generator should not be cyclic");
        assertEquals(1, _generator.getValue(), "Unexpected first item");
        assertEquals(2.1, _generator.getValue(), "Unexpected second item");
        assertEquals(Boolean.TRUE, _generator.getValue(), "Unexpected third item");
        assertEquals("test", _generator.getValue(), "Unexpected forth item");
        assertEquals(2, _generator.getValue(), "Unexpected fifth item");
        assertEquals("test", _generator.getValue(), "Unexpected sixs item");
        assertEquals(2, _generator.getValue(), "Unexpected sevens item");
    }
}
