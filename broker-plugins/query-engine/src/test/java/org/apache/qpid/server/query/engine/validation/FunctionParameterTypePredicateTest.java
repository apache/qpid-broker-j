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
package org.apache.qpid.server.query.engine.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

/**
 * Tests designed to verify the {@link FunctionParameterTypePredicate} functionality
 */
public class FunctionParameterTypePredicateTest
{
    @Test()
    public void emptyPredicate()
    {
        try
        {
            FunctionParameterTypePredicate.builder().build();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_ARGS_PREDICATE_EMPTY, e.getMessage());
        }
    }

    @Test()
    public void allowDisallowBooleans()
    {
        try
        {
            FunctionParameterTypePredicate.builder().allowBooleans().disallowBooleans().build();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_ARGS_PREDICATE_EMPTY, e.getMessage());
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void booleans()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowBooleans().build();
        assertFalse(predicate.test(null));
        assertTrue(predicate.test((R) Boolean.TRUE));
        assertTrue(predicate.test((R) Boolean.FALSE));
        assertTrue(predicate.test((R) (Boolean) true));
        assertTrue(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void numbers()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowNumbers().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertTrue(predicate.test((R) (Integer) 1));
        assertTrue(predicate.test((R) (Long) 1L));
        assertTrue(predicate.test((R) (Double) 1.0));
        assertTrue(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void enums()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowEnums().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertTrue(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void nulls()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowNulls().build();
        assertTrue(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void strings()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowStrings().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertTrue(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void datetime()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowDateTimeTypes().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertTrue(predicate.test((R) new Date()));
        assertTrue(predicate.test((R) Instant.now()));
        assertTrue(predicate.test((R) LocalDate.now()));
        assertTrue(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void comparables()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowComparables().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertTrue(predicate.test((R) (Integer) 1));
        assertTrue(predicate.test((R) (Long) 1L));
        assertTrue(predicate.test((R) (Double) 1.0));
        assertTrue(predicate.test((R) BigDecimal.ONE));
        assertTrue(predicate.test((R) "true"));
        assertTrue(predicate.test((R) UUID.randomUUID()));
        assertTrue(predicate.test((R) OverflowPolicy.NONE));
        assertTrue(predicate.test((R) new Date()));
        assertTrue(predicate.test((R) Instant.now()));
        assertTrue(predicate.test((R) LocalDate.now()));
        assertTrue(predicate.test((R) LocalDateTime.now()));
    }
}
