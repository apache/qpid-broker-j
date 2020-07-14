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
package org.apache.qpid.server.model.validator;

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ValidationResultTest extends UnitTestBase
{
    @Test
    public void testValid()
    {
        ValidationResult<Long> result = ValidationResult.valid(() -> 234L);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.valid(234L);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }

    @Test
    public void testInvalid()
    {
        ValidationResult<Long> result = ValidationResult.invalid(() -> 234L);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.invalid(234L);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }

    @Test
    public void testNewResult()
    {
        ValidationResult<Long> result = ValidationResult.newResult(true, () -> 234L);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(true, 234L);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(false, () -> 234L);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(false, 234L);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }

    @Test
    public void testNewResult_FromValidationResult()
    {
        ValidationResult<Long> result = ValidationResult.newResult(true, ValidationResult.invalid(() -> 234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(true, ValidationResult.invalid(234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(false, ValidationResult.valid(() -> 234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.newResult(false, ValidationResult.valid(234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }

    @Test
    public void testValid_FromValidationResult()
    {
        ValidationResult<Long> result = ValidationResult.valid(ValidationResult.invalid(() -> 234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.valid(ValidationResult.valid(() -> 234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.valid(ValidationResult.invalid(234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.valid(ValidationResult.valid(234L));
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }

    @Test
    public void testInvalid_FromValidationResult()
    {
        ValidationResult<Long> result = ValidationResult.invalid(ValidationResult.invalid(() -> 234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.invalid(ValidationResult.invalid(234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.invalid(ValidationResult.valid(() -> 234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());

        result = ValidationResult.invalid(ValidationResult.valid(234L));
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(234L), result.get());
    }
}
