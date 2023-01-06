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
 *
 */
package org.apache.qpid.disttest.charting.chartbuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.awt.Color;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ColorFactoryTest extends UnitTestBase
{
    @Test
    public void testBlue()
    {
        assertEquals(Color.blue, ColorFactory.toColour("blue"));
        assertEquals(Color.blue, ColorFactory.toColour("BLUE"));
        assertEquals(Color.blue, ColorFactory.toColour("Blue"));
    }

    @Test
    public void testDarkBlue()
    {
        assertEquals(Color.blue.darker(), ColorFactory.toColour("dark_blue"));
        assertEquals(Color.blue.darker(), ColorFactory.toColour("DARK_BLUE"));
        assertEquals(Color.blue.darker(), ColorFactory.toColour("Dark_Blue"));
    }
}