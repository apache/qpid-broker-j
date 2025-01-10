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
package org.apache.qpid.server.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class JMSSelectorFilterSyntaxTest
{
    @Test
    void equality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("size")).thenReturn("small");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color = 'red'").matches(message));
        assertTrue(new JMSSelectorFilter("size = 'small'").matches(message));
        assertTrue(new JMSSelectorFilter("price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue'").matches(message));
        assertFalse(new JMSSelectorFilter("size = 'big'").matches(message));
        assertFalse(new JMSSelectorFilter("price = 200").matches(message));
    }

    @Test
    void notEquality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("size")).thenReturn("small");
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("not color = 'red'").matches(message));
        assertFalse(new JMSSelectorFilter("not size = 'small'").matches(message));
        assertFalse(new JMSSelectorFilter("not price = 100").matches(message));
        assertTrue(new JMSSelectorFilter("not color = 'blue'").matches(message));
        assertTrue(new JMSSelectorFilter("not size = 'big'").matches(message));
        assertTrue(new JMSSelectorFilter("not price = 200").matches(message));

        assertFalse(new JMSSelectorFilter("not (color = 'red') and not (price = 100) and not (size = 'small')").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and not price = 100 and not size = 'small'").matches(message));
        assertTrue(new JMSSelectorFilter("not (color = 'blue') and not (price = 200) and not (size = 'big')").matches(message));
        assertTrue(new JMSSelectorFilter("not color = 'blue' and not price = 200 and not size = 'big'").matches(message));
    }

    @Test
    void inequality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("size")).thenReturn("small");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color <> 'blue'").matches(message));
        assertTrue(new JMSSelectorFilter("size <> 'big'").matches(message));
        assertTrue(new JMSSelectorFilter("price <> 200").matches(message));
        assertFalse(new JMSSelectorFilter("color <> 'red'").matches(message));
        assertFalse(new JMSSelectorFilter("size <> 'small'").matches(message));
        assertFalse(new JMSSelectorFilter("price <> 100").matches(message));
    }

    @Test
    void notInequality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("size")).thenReturn("small");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("not (color <> 'red') and not (price <> 100) and not (size <> 'small')").matches(message));
        assertTrue(new JMSSelectorFilter("not color <> 'red' and not price <> 100 and not size <> 'small'").matches(message));
        assertFalse(new JMSSelectorFilter("not (color <> 'blue') and not (price <> 200) and not (size <> 'big')").matches(message));
        assertFalse(new JMSSelectorFilter("not color <> 'blue' and not price <> 200 and not size <> 'big'").matches(message));
    }

    @Test
    void greaterThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price > 10").matches(message));
        assertFalse(new JMSSelectorFilter("price > 100").matches(message));
    }

    @Test
    void notGreaterThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("not price > 10").matches(message));
        assertTrue(new JMSSelectorFilter("not price > 100").matches(message));
    }

    @Test
    void greaterThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price >= 10").matches(message));
        assertTrue(new JMSSelectorFilter("price >= 100").matches(message));
        assertFalse(new JMSSelectorFilter("price >= 200").matches(message));
    }

    @Test
    void notGreaterThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("not price >= 10").matches(message));
        assertFalse(new JMSSelectorFilter("not price >= 100").matches(message));
        assertTrue(new JMSSelectorFilter("not price >= 200").matches(message));
    }

    @Test
    void lessThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price < 110").matches(message));
        assertFalse(new JMSSelectorFilter("price < 100").matches(message));
    }

    @Test
    void notLessThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("not price < 110").matches(message));
        assertTrue(new JMSSelectorFilter("not price < 100").matches(message));
    }

    @Test
    void lessThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price <= 110").matches(message));
        assertTrue(new JMSSelectorFilter("price <= 100").matches(message));
        assertFalse(new JMSSelectorFilter("price <= 10").matches(message));
    }

    @Test
    void notLessThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("not price <= 110").matches(message));
        assertFalse(new JMSSelectorFilter("not price <= 100").matches(message));
        assertTrue(new JMSSelectorFilter("not price <= 10").matches(message));
    }

    @Test
    void and() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color = 'red' and price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 200").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue' and price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue' and price = 200").matches(message));
    }

    @Test
    void multipleAnd() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);
        when(message.getHeader("size")).thenReturn("large");
        when(message.getHeader("height")).thenReturn(10);
        when(message.getHeader("width")).thenReturn(5);
        when(message.getHeader("depth")).thenReturn(2);
        when(message.getHeader("length")).thenReturn(8);

        assertTrue(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large' and height = 10 and width = 5 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue' and price = 100 and size = 'large' and height = 10 and width = 5 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 200 and size = 'large' and height = 10 and width = 5 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'small' and height = 10 and width = 5 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large' and height = 11 and width = 5 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large' and height = 10 and width = 4 and depth = 2 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large' and height = 10 and width = 5 and depth = 3 and length = 8").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large' and height = 10 and width = 5 and depth = 2 and length = 9").matches(message));

        assertFalse(new JMSSelectorFilter("color = 'red' and not price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and not price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("not (color = 'red' and price = 100)").matches(message));
        assertTrue(new JMSSelectorFilter("not color <> 'red' and not price <> 100").matches(message));

        assertTrue(new JMSSelectorFilter("color = 'red' and price = 100 and size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and price = 100 and not size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and not price = 100 and size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and price = 100 and size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and not price = 100 and size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and not price = 100 and not size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("not color = 'red' and price = 100 and not size = 'large'").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and not price = 100 and not size = 'large'").matches(message));

        assertTrue(new JMSSelectorFilter("not color <> 'red' and not price <> 100 and not size <> 'large'").matches(message));
    }

    @Test
    public void chainedNot() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);
        when(message.getHeader("size")).thenReturn("large");

        assertFalse(new JMSSelectorFilter("not(true)").matches(message));
        assertTrue(new JMSSelectorFilter("not not(true)").matches(message));
        assertFalse(new JMSSelectorFilter("not not not(true)").matches(message));
        assertTrue(new JMSSelectorFilter("not not not not(true)").matches(message));

        assertFalse(new JMSSelectorFilter("true and not (false) and not (true)").matches(message));
        assertFalse(new JMSSelectorFilter("true and (not false) and (not true)").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'red' and not (price = 200) and not (size = 'large')").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'red' and not (price = 200) and not (size = 'small')").matches(message));

        when(message.getHeader("entry")).thenReturn("bbb");
        when(message.getHeader("fruit")).thenReturn("apple");

        assertTrue(new JMSSelectorFilter("not (((true and true) or (false or true)) and (not (price not between 90 and 110) and not (not (fruit in ('apple', 'banana', 'cherry')) or (entry NOT LIKE '%aaa%'))))").matches(message));

        when(message.getHeader("entry")).thenReturn("aaa");
        assertFalse(new JMSSelectorFilter("not (((true and true) or (false or true)) and (not (price not between 90 and 110) and not (not (fruit in ('apple', 'banana', 'cherry')) or (entry NOT LIKE '%aaa%'))))").matches(message));
    }

    @Test
    void or() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color = 'red' or price = 100").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'red' or price = 200").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue' or price = 200").matches(message));
    }

    @Test
    void multipleOr() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);
        when(message.getHeader("size")).thenReturn("large");
        when(message.getHeader("height")).thenReturn(10);
        when(message.getHeader("width")).thenReturn(5);
        when(message.getHeader("depth")).thenReturn(2);
        when(message.getHeader("length")).thenReturn(8);

        assertFalse(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'small' or height = 11 or width = 4 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'red' or price = 200 or size = 'small' or height = 11 or width = 4 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 100 or size = 'small' or height = 11 or width = 4 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'large' or height = 11 or width = 4 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'small' or height = 10 or width = 4 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'small' or height = 11 or width = 5 or depth = 1 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'small' or height = 11 or width = 4 or depth = 2 or length = 9").matches(message));
        assertTrue(new JMSSelectorFilter("color = 'blue' or price = 200 or size = 'small' or height = 11 or width = 4 or depth = 1 or length = 8").matches(message));
    }

    @Test
    void in() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("fruit")).thenReturn("apple");
        assertTrue(new JMSSelectorFilter("fruit in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("banana");
        assertTrue(new JMSSelectorFilter("fruit in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("cherry");
        assertTrue(new JMSSelectorFilter("fruit in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("mango");
        assertFalse(new JMSSelectorFilter("fruit in ('apple', 'banana', 'cherry')").matches(message));
    }

    @Test
    void notIn() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("fruit")).thenReturn("apple");
        assertFalse(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertFalse(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertFalse(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("banana");
        assertFalse(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertFalse(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertFalse(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));
        assertTrue(new JMSSelectorFilter("not fruit not in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("cherry");
        assertFalse(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertFalse(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertFalse(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));
        assertTrue(new JMSSelectorFilter("not fruit not in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("mango");
        assertTrue(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertTrue(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertTrue(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));
        assertFalse(new JMSSelectorFilter("not fruit not in ('apple', 'banana', 'cherry')").matches(message));
    }

    @Test
    void between() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price between 90 and 110").matches(message));
        assertTrue(new JMSSelectorFilter("price between 100 and 110").matches(message));
        assertTrue(new JMSSelectorFilter("price between 90 and 100").matches(message));
        assertFalse(new JMSSelectorFilter("price between 110 and 120").matches(message));
    }

    @Test
    void notBetween() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("price not between 90 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("price not between 100 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("price not between 90 and 100").matches(message));
        assertTrue(new JMSSelectorFilter("price not between 110 and 120").matches(message));
        assertFalse(new JMSSelectorFilter("not price not between 110 and 120").matches(message));

        assertFalse(new JMSSelectorFilter("not (price between 90 and 110)").matches(message));
        assertFalse(new JMSSelectorFilter("not (price between 100 and 110)").matches(message));
        assertFalse(new JMSSelectorFilter("not (price between 90 and 100)").matches(message));
        assertTrue(new JMSSelectorFilter("not (price between 110 and 120)").matches(message));
        assertFalse(new JMSSelectorFilter("not (price not between 110 and 120)").matches(message));

        assertFalse(new JMSSelectorFilter("not price between 90 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("not price between 100 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("not price between 90 and 100").matches(message));
        assertTrue(new JMSSelectorFilter("not price between 110 and 120").matches(message));
    }

    @Test
    void like() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("entry")).thenReturn("bbb");
        assertFalse(new JMSSelectorFilter("entry like '%aaa%'").matches(message));

        when(message.getHeader("entry")).thenReturn("aaa");
        assertTrue(new JMSSelectorFilter("entry like '%aaa%'").matches(message));
    }

    @Test
    void notLike() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("entry")).thenReturn("bbb");

        assertTrue(new JMSSelectorFilter("entry NOT LIKE '%aaa%'").matches(message));
        assertTrue(new JMSSelectorFilter("(entry NOT LIKE '%aaa%')").matches(message));

        assertTrue(new JMSSelectorFilter("NOT (entry LIKE '%aaa%')").matches(message));
        assertTrue(new JMSSelectorFilter("NOT entry LIKE '%aaa%'").matches(message));

        when(message.getHeader("entry")).thenReturn("aaa");

        assertFalse(new JMSSelectorFilter("entry NOT LIKE '%aaa%'").matches(message));
        assertFalse(new JMSSelectorFilter("(entry NOT LIKE '%aaa%')").matches(message));

        assertFalse(new JMSSelectorFilter("NOT (entry LIKE '%aaa%')").matches(message));
        assertFalse(new JMSSelectorFilter("NOT entry LIKE '%aaa%'").matches(message));
        assertTrue(new JMSSelectorFilter("NOT (entry NOT LIKE '%aaa%')").matches(message));
        assertTrue(new JMSSelectorFilter("NOT entry NOT LIKE '%aaa%'").matches(message));
    }

    @Test
    void isNull() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("entry")).thenReturn("aaa");

        assertFalse(new JMSSelectorFilter("entry is null").matches(message));
        assertTrue(new JMSSelectorFilter("another_entry is null").matches(message));
    }

    @Test
    void isNotNull() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("entry")).thenReturn("aaa");

        assertTrue(new JMSSelectorFilter("entry is not null").matches(message));
        assertTrue(new JMSSelectorFilter("not (entry is null)").matches(message));
        assertTrue(new JMSSelectorFilter("not entry is null").matches(message));
        assertFalse(new JMSSelectorFilter("not entry is not null").matches(message));

        assertFalse(new JMSSelectorFilter("another_entry is not null").matches(message));
        assertFalse(new JMSSelectorFilter("not (another_entry is null)").matches(message));
        assertFalse(new JMSSelectorFilter("not another_entry is null").matches(message));
        assertTrue(new JMSSelectorFilter("not another_entry is not null").matches(message));
    }

    @Test
    void arithmetic() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("size")).thenReturn(10);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("size + price = 110").matches(message));
        assertTrue(new JMSSelectorFilter("price - size = 90").matches(message));
        assertTrue(new JMSSelectorFilter("price / size = 10").matches(message));
        assertTrue(new JMSSelectorFilter("price * size = 1000").matches(message));

        assertTrue(new JMSSelectorFilter("size / 4 = 2.5").matches(message));
        assertTrue(new JMSSelectorFilter("size / 4.0 = 2.5").matches(message));

        assertTrue(new JMSSelectorFilter("size * 2 = 20.0").matches(message));
        assertTrue(new JMSSelectorFilter("size * 2.0 = 20.0").matches(message));
    }

    @Test
    void notArithmetic() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("size")).thenReturn(10);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("not (size + price = 111)").matches(message));
        assertTrue(new JMSSelectorFilter("not (price - size = 91)").matches(message));
        assertTrue(new JMSSelectorFilter("not (price / size = 11)").matches(message));
        assertTrue(new JMSSelectorFilter("not (price * size = 1001)").matches(message));

        assertTrue(new JMSSelectorFilter("not size + price = 111").matches(message));
        assertTrue(new JMSSelectorFilter("not price - size = 91").matches(message));
        assertTrue(new JMSSelectorFilter("not price / size = 11").matches(message));
        assertTrue(new JMSSelectorFilter("not price * size = 1001").matches(message));

        assertTrue(new JMSSelectorFilter("not (size / 4 = 3.5)").matches(message));
        assertTrue(new JMSSelectorFilter("not (size / 4.0 = 3.5)").matches(message));

        assertTrue(new JMSSelectorFilter("not size / 4 = 3.5").matches(message));
        assertTrue(new JMSSelectorFilter("not size / 4.0 = 3.5").matches(message));

        assertTrue(new JMSSelectorFilter("not (size * 2 = 21.0)").matches(message));
        assertTrue(new JMSSelectorFilter("not (size * 2.0 = 21.0)").matches(message));

        assertTrue(new JMSSelectorFilter("not size * 2 = 21.0").matches(message));
        assertTrue(new JMSSelectorFilter("not size * 2.0 = 21.0").matches(message));
    }

    @Test
    void arithmeticOperatorsPrecedence() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("size")).thenReturn(10);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("1 + 1 * 10 = 11").matches(message));
        assertTrue(new JMSSelectorFilter("(1 + 1) * 10 = 20").matches(message));

        assertTrue(new JMSSelectorFilter("1 + 1 / 10 = 1.1").matches(message));
        assertTrue(new JMSSelectorFilter("(1 + 1) / 10 = 0.2").matches(message));
    }

    @Test
    void logicOperatorsPrecedence() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("a")).thenReturn(1);
        when(message.getHeader("b")).thenReturn(2);
        when(message.getHeader("c")).thenReturn(3);

        assertTrue(new JMSSelectorFilter("True or True and False").matches(message));
        assertTrue(new JMSSelectorFilter("False and True or True").matches(message));

        assertFalse(new JMSSelectorFilter("(True or True) and False").matches(message));
        assertTrue(new JMSSelectorFilter("True or (True and False)").matches(message));

        assertFalse(new JMSSelectorFilter("False and (True or True)").matches(message));
        assertTrue(new JMSSelectorFilter("(False and True) or True").matches(message));

        assertTrue(new JMSSelectorFilter("a = 1 and b = 2 or c = 3").matches(message));
        assertFalse(new JMSSelectorFilter("not a = 1 and b = 2").matches(message));
        assertTrue(new JMSSelectorFilter("a = 1 and (b = 2 or c = 3)").matches(message));
        assertTrue(new JMSSelectorFilter("a = 1 and (b = 2 or c = 4)").matches(message));
        assertTrue(new JMSSelectorFilter("a = 1 and (b = 3 or c = 3)").matches(message));
        assertFalse(new JMSSelectorFilter("a = 1 and (b = 3 or c = 4)").matches(message));
        assertTrue(new JMSSelectorFilter("not (not a = 1)").matches(message));
        assertTrue(new JMSSelectorFilter("not not a = 1").matches(message));

        when(message.getHeader("a")).thenReturn(1);
        when(message.getHeader("b")).thenReturn(2);
        when(message.getHeader("c")).thenReturn(4);

        assertTrue(new JMSSelectorFilter("a = 1 and b = 2 or c = 3").matches(message));

        when(message.getHeader("a")).thenReturn(1);
        when(message.getHeader("b")).thenReturn(1);
        when(message.getHeader("c")).thenReturn(3);

        assertTrue(new JMSSelectorFilter("a = 1 and b = 2 or c = 3").matches(message));

        when(message.getHeader("a")).thenReturn(1);
        when(message.getHeader("b")).thenReturn(2);
        when(message.getHeader("c")).thenReturn(3);

        assertTrue(new JMSSelectorFilter("a = 1 and not b = 2 or c = 3").matches(message));
        assertTrue(new JMSSelectorFilter("(a = 1 and not b = 2) or c = 3").matches(message));
        assertFalse(new JMSSelectorFilter("a = 1 and not (b = 2 or c = 3)").matches(message));
        assertTrue(new JMSSelectorFilter("not (a = 1 and not b = 2) or c = 3").matches(message));

        assertTrue(new JMSSelectorFilter("a = 1 or not b = 2 and c = 3").matches(message));
        assertTrue(new JMSSelectorFilter("(a = 1 or not b = 2) and c = 3").matches(message));
        assertTrue(new JMSSelectorFilter("a = 1 or (not b = 2 and c = 3)").matches(message));
        assertTrue(new JMSSelectorFilter("a = 1 or not (b = 2 and c = 3)").matches(message));
        assertFalse(new JMSSelectorFilter("not (a = 1 or not b = 2) and c = 3").matches(message));
    }
}
