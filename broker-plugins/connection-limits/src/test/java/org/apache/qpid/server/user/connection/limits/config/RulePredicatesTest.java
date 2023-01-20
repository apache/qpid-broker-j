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
package org.apache.qpid.server.user.connection.limits.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.user.connection.limits.config.RulePredicates.Property;
import org.apache.qpid.test.utils.UnitTestBase;

public class RulePredicatesTest extends UnitTestBase
{
    @Test
    public void testEmpty()
    {
        final RulePredicates predicates = new RulePredicates();
        predicates.parse("Unknown", "23");

        assertFalse(predicates.isUserBlocked());
        assertNull(predicates.getConnectionCountLimit());
        assertNull(predicates.getConnectionFrequencyLimit());
        assertEquals(RulePredicates.ALL_PORTS, predicates.getPort());
        assertTrue(predicates.isEmpty());
    }

    @Test
    public void testParse_port()
    {
        final RulePredicates predicates = new RulePredicates();
        assertEquals(RulePredicates.ALL_PORTS, predicates.getPort());

        predicates.parse(Property.PORT.name(), "amqp");
        assertEquals("amqp", predicates.getPort());

        try
        {
            predicates.parse(Property.PORT.name(), "amqps");
            fail("An exception is expected here, multiple ports");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Property 'port' has already been defined", e.getMessage());
        }
    }

    @Test
    public void testParse_connectionLimit()
    {
        final RulePredicates predicates = new RulePredicates();
        assertNull(predicates.getConnectionCountLimit());

        predicates.parse(Property.CONNECTION_LIMIT.name(), "70");
        assertEquals(Integer.valueOf(70), predicates.getConnectionCountLimit());

        try
        {
            predicates.parse(Property.CONNECTION_LIMIT.name(), "75");
            fail("An exception is expected here, multiple connection count limits");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Property 'connection_limit' has already been defined", e.getMessage());
        }
    }

    @Test
    public void testParse_connectionLimit_negative()
    {
        final RulePredicates predicates = new RulePredicates();
        assertNull(predicates.getConnectionCountLimit());

        predicates.parse(Property.CONNECTION_LIMIT.name(), "-1");
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), predicates.getConnectionCountLimit());
    }

    @Test
    public void testParse_frequencyLimit()
    {
        final RulePredicates predicates = new RulePredicates();
        assertNull(predicates.getConnectionFrequencyLimit());
        assertNull(predicates.getConnectionFrequencyPeriod());

        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "70");
        assertEquals(Integer.valueOf(70), predicates.getConnectionFrequencyLimit());
        assertNull(predicates.getConnectionFrequencyPeriod());

        try
        {
            predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "75");
            fail("An exception is expected here, multiple connection frequency limits");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Property 'connection_frequency_limit' has already been defined", e.getMessage());
        }
    }

    @Test
    public void testParse_frequencyLimitWithTimePeriod()
    {
        RulePredicates predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "70/23m");
        assertEquals(Integer.valueOf(70), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.ofMinutes(23L), predicates.getConnectionFrequencyPeriod());

        predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "67/H");
        assertEquals(Integer.valueOf(67), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.ofHours(1L), predicates.getConnectionFrequencyPeriod());

        predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "11/PT5m7.5s");
        assertEquals(Integer.valueOf(11), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.parse("PT5m7.5s"), predicates.getConnectionFrequencyPeriod());
    }

    @Test
    public void testParse_frequencyLimitWithTimePeriod_longDescription()
    {
        RulePredicates predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "70/Second");
        assertEquals(Integer.valueOf(70), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.ofSeconds(1L), predicates.getConnectionFrequencyPeriod());

        predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "67/Minute");
        assertEquals(Integer.valueOf(67), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.ofMinutes(1L), predicates.getConnectionFrequencyPeriod());

        predicates = new RulePredicates();
        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "11/Hour");
        assertEquals(Integer.valueOf(11), predicates.getConnectionFrequencyLimit());
        assertEquals(Duration.ofHours(1L), predicates.getConnectionFrequencyPeriod());
    }

    @Test
    public void testParse_frequencyLimit_negative()
    {
        final RulePredicates predicates = new RulePredicates();
        assertNull(predicates.getConnectionFrequencyLimit());

        predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "-1");
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), predicates.getConnectionFrequencyLimit());
    }

    @Test
    public void testParse_frequencyLimit_negativeTimePeriod()
    {
        final RulePredicates predicates = new RulePredicates();
        assertNull(predicates.getConnectionFrequencyLimit());

        try
        {
            predicates.parse(Property.CONNECTION_FREQUENCY_LIMIT.name(), "100/-PT2m");
            fail("An exception is expected, time period can not be negative");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Frequency period can not be negative -120 s", e.getMessage());
        }
    }

    @Test
    public void testSetBlockedUser()
    {
        final RulePredicates predicates = new RulePredicates();
        assertFalse(predicates.isUserBlocked());

        predicates.setBlockedUser();
        assertTrue(predicates.isUserBlocked());
    }

    @Test
    public void testParseProperty()
    {
        assertEquals(Property.CONNECTION_LIMIT, Property.parse("CONNECTION_LIMIT"));
        assertEquals(Property.CONNECTION_LIMIT, Property.parse("connection-limit"));
        assertEquals(Property.CONNECTION_LIMIT, Property.parse("ConnectionLimit"));
        assertEquals(Property.CONNECTION_LIMIT, Property.parse("ConNectioN-limit"));
        assertNull(Property.parse("Connection=Limit"));
    }

    @Test
    public void testIsAllUser()
    {
        assertTrue(RulePredicates.isAllUser("all"));
        assertTrue(RulePredicates.isAllUser("All"));
        assertTrue(RulePredicates.isAllUser("ALL"));
        assertFalse(RulePredicates.isAllUser("any"));
        assertFalse(RulePredicates.isAllUser("Any"));
        assertFalse(RulePredicates.isAllUser("ANY"));
        assertFalse(RulePredicates.isAllUser("true"));
    }

    @Test
    public void testIsAllPort()
    {
        assertTrue(RulePredicates.isAllPort("all"));
        assertTrue(RulePredicates.isAllPort("All"));
        assertTrue(RulePredicates.isAllPort("ALL"));
        assertFalse(RulePredicates.isAllPort("any"));
        assertFalse(RulePredicates.isAllPort("Any"));
        assertFalse(RulePredicates.isAllPort("ANY"));
        assertFalse(RulePredicates.isAllPort("true"));
    }
}