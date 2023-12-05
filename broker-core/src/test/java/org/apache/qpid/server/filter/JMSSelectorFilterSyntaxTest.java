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

public class JMSSelectorFilterSyntaxTest
{
    @Test
    public void equality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color = 'red'").matches(message));
        assertTrue(new JMSSelectorFilter("price = 100").matches(message));
        assertFalse(new JMSSelectorFilter("color = 'blue'").matches(message));
        assertFalse(new JMSSelectorFilter("price = 200").matches(message));
    }

    @Test
    public void inequality() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("color")).thenReturn("red");
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("color <> 'blue'").matches(message));
        assertTrue(new JMSSelectorFilter("price <> 200").matches(message));
        assertFalse(new JMSSelectorFilter("color <> 'red'").matches(message));
        assertFalse(new JMSSelectorFilter("price <> 100").matches(message));
    }

    @Test
    public void greaterThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price > 10").matches(message));
        assertFalse(new JMSSelectorFilter("price > 100").matches(message));
    }

    @Test
    public void greaterThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price >= 10").matches(message));
        assertTrue(new JMSSelectorFilter("price >= 100").matches(message));
        assertFalse(new JMSSelectorFilter("price >= 200").matches(message));
    }

    @Test
    public void lessThan() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price < 110").matches(message));
        assertFalse(new JMSSelectorFilter("price < 100").matches(message));
    }

    @Test
    public void lessThanOrEquals() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price <= 110").matches(message));
        assertTrue(new JMSSelectorFilter("price <= 100").matches(message));
        assertFalse(new JMSSelectorFilter("price <= 10").matches(message));
    }

    @Test
    public void and() throws Exception
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
    public void or() throws Exception
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
    public void in() throws Exception
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
    public void notIn() throws Exception
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

        when(message.getHeader("fruit")).thenReturn("cherry");
        assertFalse(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertFalse(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertFalse(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));

        when(message.getHeader("fruit")).thenReturn("mango");
        assertTrue(new JMSSelectorFilter("fruit not in ('apple', 'banana', 'cherry')").matches(message));
        assertTrue(new JMSSelectorFilter("not (fruit in ('apple', 'banana', 'cherry'))").matches(message));
        assertTrue(new JMSSelectorFilter("not fruit in ('apple', 'banana', 'cherry')").matches(message));
    }

    @Test
    public void between() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertTrue(new JMSSelectorFilter("price between 90 and 110").matches(message));
        assertTrue(new JMSSelectorFilter("price between 100 and 110").matches(message));
        assertTrue(new JMSSelectorFilter("price between 90 and 100").matches(message));
        assertFalse(new JMSSelectorFilter("price between 110 and 120").matches(message));
    }

    @Test
    public void notBetween() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("price")).thenReturn(100);

        assertFalse(new JMSSelectorFilter("price not between 90 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("price not between 100 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("price not between 90 and 100").matches(message));
        assertTrue(new JMSSelectorFilter("price not between 110 and 120").matches(message));

        assertFalse(new JMSSelectorFilter("not (price between 90 and 110)").matches(message));
        assertFalse(new JMSSelectorFilter("not (price between 100 and 110)").matches(message));
        assertFalse(new JMSSelectorFilter("not (price between 90 and 100)").matches(message));
        assertTrue(new JMSSelectorFilter("not (price between 110 and 120)").matches(message));

        assertFalse(new JMSSelectorFilter("not price between 90 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("not price between 100 and 110").matches(message));
        assertFalse(new JMSSelectorFilter("not price between 90 and 100").matches(message));
        assertTrue(new JMSSelectorFilter("not price between 110 and 120").matches(message));
    }

    @Test
    public void like() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("entry")).thenReturn("bbb");
        assertFalse(new JMSSelectorFilter("entry like '%aaa%'").matches(message));

        when(message.getHeader("entry")).thenReturn("aaa");
        assertTrue(new JMSSelectorFilter("entry like '%aaa%'").matches(message));
    }

    @Test
    public void notLike() throws Exception
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
    }

    @Test
    public void isNull() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("entry")).thenReturn("aaa");

        assertFalse(new JMSSelectorFilter("entry is null").matches(message));
        assertTrue(new JMSSelectorFilter("another_entry is null").matches(message));
    }

    @Test
    public void isNotNull() throws Exception
    {
        final Filterable message = mock(Filterable.class);
        when(message.getHeader("entry")).thenReturn("aaa");

        assertTrue(new JMSSelectorFilter("entry is not null").matches(message));
        assertTrue(new JMSSelectorFilter("not (entry is null)").matches(message));
        assertTrue(new JMSSelectorFilter("not entry is null").matches(message));
        assertFalse(new JMSSelectorFilter("another_entry is not null").matches(message));
        assertFalse(new JMSSelectorFilter("not (another_entry is null)").matches(message));
        assertFalse(new JMSSelectorFilter("not another_entry is null").matches(message));
    }

    @Test
    public void arithmetic() throws Exception
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
    public void arithmeticOperatorsPrecedence() throws Exception
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
    public void logicOperatorsPrecedence() throws Exception
    {
        final Filterable message = mock(Filterable.class);

        when(message.getHeader("a")).thenReturn(1);
        when(message.getHeader("b")).thenReturn(2);
        when(message.getHeader("c")).thenReturn(3);
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
    }
}
