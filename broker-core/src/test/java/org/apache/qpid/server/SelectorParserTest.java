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

package org.apache.qpid.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.test.utils.UnitTestBase;

public class SelectorParserTest extends UnitTestBase
{
    @Test
    public void testSelectorWithHyphen()
    {
        testPass("Cost = 2 AND \"property-with-hyphen\" = 'wibble'");
    }

    @Test
    public void testLike()
    {        
        testFail("Cost LIKE 2");
        testPass("Cost LIKE 'Hello'");
    }

    @Test
    public void testStringQuoted()
    {
        testPass("string = 'Test'");
    }

    @Test
    public void testProperty()
    {
        testPass("prop1 = prop2");
    }

    @Test
    public void testPropertyInvalid()
    {
        testFail("prop1 = prop2 foo AND string = 'Test'");
    }

    @Test
    public void testPropertyNames()
    {
        testPass("$min= TRUE AND _max= FALSE AND Prop_2 = true AND prop$3 = false");
    }

    @Test
    public void testProtected()
    {
        testFail("NULL = 0 ");
        testFail("TRUE = 0 ");
        testFail("FALSE = 0 ");
        testFail("NOT = 0 ");
        testFail("AND = 0 ");
        testFail("OR = 0 ");
        testFail("BETWEEN = 0 ");
        testFail("LIKE = 0 ");
        testFail("IN = 0 ");
        testFail("IS = 0 ");
        testFail("ESCAPE = 0 ");
   }

    @Test
    public void testBoolean()
    {
        testPass("min= TRUE  AND max= FALSE ");
        testPass("min= true AND max= false");
    }

    @Test
    public void testDouble()
    {
        testPass("positive=31E2 AND negative=-31.4E3");
        testPass("min=" + Double.MIN_VALUE + " AND max=" + Double.MAX_VALUE);
    }

    @Test
    public void testLong()
    {
        testPass("minLong=" + Long.MIN_VALUE + "L AND maxLong=" + Long.MAX_VALUE + "L");
    }

    @Test
    public void testInt()
    {
        testPass("minInt=" + Integer.MIN_VALUE + " AND maxInt=" + Integer.MAX_VALUE);
    }

    @Test
    public void testSigned()
    {
        testPass("negative=-42 AND positive=+42");
    }

    @Test
    public void testOctal()
    {
        testPass("octal=042");
    }

    private void testPass(final String selector)
    {
        assertDoesNotThrow(() -> new JMSSelectorFilter(selector), "Selector '" + selector + "' was not parsed");
    }

    private void testFail(final String selector)
    {
        assertThrows(Exception.class, () -> new JMSSelectorFilter(selector), "Selector '" + selector + "' was parsed");
    }
}
