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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.user.connection.limits.config.RulePredicates.Property;
import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;
import org.apache.qpid.test.utils.UnitTestBase;

public class RuleTest extends UnitTestBase
{
    @Test
    public void testNewInstanceFromConnectionLimit_NonBlocking()
    {
        final ConnectionLimitRule connectionLimit = new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return "Amqp";
            }

            @Override
            public String getIdentity()
            {
                return "All";
            }

            @Override
            public Boolean getBlocked()
            {
                return null;
            }

            @Override
            public Integer getCountLimit()
            {
                return 10;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return 30;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return 10000L;
            }
        };
        final Rule rule = Rule.newInstance(connectionLimit);

        assertNotNull(rule);
        assertEquals(connectionLimit.getCountLimit(), rule.getCountLimit());
        assertEquals(connectionLimit.getFrequencyLimit(), rule.getFrequencyLimit());
        assertEquals(Duration.ofMillis(connectionLimit.getFrequencyPeriod()), rule.getFrequencyPeriod());
        assertEquals(Collections.singletonMap(Duration.ofMillis(connectionLimit.getFrequencyPeriod()), rule.getFrequencyLimit()),
                rule.getFrequencyLimits());
        assertEquals(connectionLimit.getPort(), rule.getPort());
        assertEquals(connectionLimit.getIdentity(), rule.getIdentity());
        assertFalse(rule.isUserBlocked());
        assertFalse(rule.isEmpty());
    }

    @Test
    public void testNewInstanceFromConnectionLimit_Empty()
    {
        final ConnectionLimitRule connectionLimit = new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return null;
            }

            @Override
            public Boolean getBlocked()
            {
                return null;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        };
        try
        {
            Rule.newInstance(connectionLimit);
            fail("An exception is expected, connection limit rule can not be empty");
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testNewInstanceFromConnectionLimit_Blocking()
    {
        final ConnectionLimitRule connectionLimit = new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return "amqp";
            }

            @Override
            public String getIdentity()
            {
                return "all";
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.TRUE;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        };
        final Rule rule = Rule.newInstance(connectionLimit);

        assertNotNull(rule);
        assertEquals(Integer.valueOf(0), rule.getCountLimit());
        assertEquals(Integer.valueOf(0), rule.getFrequencyLimit());
        assertTrue(rule.getFrequencyLimits().isEmpty());
        assertEquals(connectionLimit.getPort(), rule.getPort());
        assertEquals(connectionLimit.getIdentity(), rule.getIdentity());
        assertTrue(rule.isUserBlocked());
        assertFalse(rule.isEmpty());
    }

    @Test
    public void testNewInstanceFromPredicates_NonBlocking()
    {
        final RulePredicates predicates = new RulePredicates();
        predicates.addProperty(Property.CONNECTION_LIMIT, "10");
        predicates.addProperty(Property.CONNECTION_FREQUENCY_LIMIT, "20/m");
        predicates.addProperty(Property.PORT, "Amqps");
        final Rule rule = Rule.newInstance("User", predicates);

        assertNotNull(rule);
        assertEquals(predicates.getConnectionCountLimit(), rule.getCountLimit());
        assertEquals(predicates.getConnectionFrequencyLimit(), rule.getFrequencyLimit());
        assertEquals(predicates.getConnectionFrequencyPeriod(), rule.getFrequencyPeriod());
        assertEquals(Collections.singletonMap(predicates.getConnectionFrequencyPeriod(), predicates.getConnectionFrequencyLimit()),
                rule.getFrequencyLimits());
        assertEquals(predicates.getPort(), rule.getPort());
        assertEquals("User", rule.getIdentity());
        assertFalse(rule.isUserBlocked());
        assertFalse(rule.isEmpty());
    }

    @Test
    public void testNewInstanceFromPredicates_Empty()
    {
        final RulePredicates predicates = new RulePredicates();
        try
        {
            Rule.newInstance("user", predicates);
            fail("An exception is expected, connection limit rule can not be empty");
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testNewInstanceFromPredicates_Blocking()
    {
        final RulePredicates predicates = new RulePredicates();
        predicates.setBlockedUser();
        predicates.addProperty(Property.PORT, "amqps");
        final Rule rule = Rule.newInstance("user", predicates);

        assertNotNull(rule);
        assertEquals(Integer.valueOf(0), rule.getCountLimit());
        assertEquals(Integer.valueOf(0), rule.getFrequencyLimit());
        assertTrue(rule.getFrequencyLimits().isEmpty());
        assertEquals(predicates.getPort(), rule.getPort());
        assertEquals("user", rule.getIdentity());
        assertTrue(rule.isUserBlocked());
        assertFalse(rule.isEmpty());
    }
}