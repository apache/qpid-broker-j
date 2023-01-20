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
package org.apache.qpid.server.query.engine.parsing.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.MapObjectAccessor;

/**
 * Tests designed to verify the {@link CollectorFactory} functionality
 */
public class CollectorFactoryTest
{
    private final List<Map<String, Object>> _entities = Arrays.asList(
        ImmutableMap.<String, Object>builder()
            .put("age", 10)
            .put("firstname", "Anna")
            .put("lastname", "Abbot")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("age", 20)
            .put("firstname", "Bart")
            .put("lastname", "Burton")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("age", 30)
            .put("firstname", "Emma")
            .put("lastname", "Edwards")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("age", 40)
            .put("firstname", "Piter")
            .put("lastname", "Parker")
            .build(),
        ImmutableMap.<String, Object>builder()
            .put("age", 50)
            .put("firstname", "Vincent")
            .put("lastname", "Van Gogh")
            .build()
    );

    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void averaging()
    {
        Collector ageCollector = CollectorFactory.collector(CollectorType.AVERAGING).apply(new MapObjectAccessor("age"));
        Object avgAge = _entities.stream().collect(ageCollector);
        assertEquals(30.0, avgAge);
    }

    @Test()
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void counting()
    {
        Collector ageCollector = CollectorFactory.collector(CollectorType.COUNTING).apply(new MapObjectAccessor("age"));
        Object cntAge = _entities.stream().collect(ageCollector);
        assertEquals(5L, cntAge);

        Collector firstNameCollector = CollectorFactory.collector(CollectorType.COUNTING).apply(new MapObjectAccessor("firstname"));
        Object cntFirstName = _entities.stream().collect(firstNameCollector);
        assertEquals(5L, cntFirstName);

        Collector lastNameCollector = CollectorFactory.collector(CollectorType.COUNTING).apply(new MapObjectAccessor("lastname"));
        Object cntLastName = _entities.stream().collect(lastNameCollector);
        assertEquals(5L, cntLastName);
    }

    @Test()
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void maximizing()
    {
        Collector ageCollector = CollectorFactory.collector(CollectorType.MAXIMIZING).apply(new MapObjectAccessor("age"));
        Object maxAge = _entities.stream().collect(ageCollector);
        assertEquals(50, maxAge);

        Collector firstNameCollector = CollectorFactory.collector(CollectorType.MAXIMIZING).apply(new MapObjectAccessor("firstname"));
        Object maxFirstName = _entities.stream().collect(firstNameCollector);
        assertEquals("Vincent", maxFirstName);

        Collector lastNameCollector = CollectorFactory.collector(CollectorType.MAXIMIZING).apply(new MapObjectAccessor("lastname"));
        Object maxLastName = _entities.stream().collect(lastNameCollector);
        assertEquals("Van Gogh", maxLastName);
    }

    @Test()
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void minimizing()
    {
        Collector ageCollector = CollectorFactory.collector(CollectorType.MINIMIZING).apply(new MapObjectAccessor("age"));
        Object minAge = _entities.stream().collect(ageCollector);
        assertEquals(10, minAge);

        Collector firstNameCollector = CollectorFactory.collector(CollectorType.MINIMIZING).apply(new MapObjectAccessor("firstname"));
        Object minFirstName = _entities.stream().collect(firstNameCollector);
        assertEquals("Anna", minFirstName);

        Collector lastNameCollector = CollectorFactory.collector(CollectorType.MINIMIZING).apply(new MapObjectAccessor("lastname"));
        Object minLastName = _entities.stream().collect(lastNameCollector);
        assertEquals("Abbot", minLastName);
    }

    @Test()
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void summing()
    {
        Collector ageCollector = CollectorFactory.collector(CollectorType.SUMMING).apply(new MapObjectAccessor("age"));
        Object sumAge = _entities.stream().collect(ageCollector);
        assertEquals(150.0, sumAge);
    }

    @Test()
    public void collectorTypeNull()
    {
        try
        {
            CollectorFactory.collector(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.COLLECTOR_TYPE_NULL, e.getMessage());
        }
    }
}
