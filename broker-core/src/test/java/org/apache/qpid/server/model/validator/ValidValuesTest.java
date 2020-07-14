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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValidValuesTest extends UnitTestBase
{

    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", ValidValues.validator(Collections.singletonList("A"), Function.identity()));
        assertNotNull("Factory method has to produce a instance", ValidValues.validator(Collections.singletonList("A")));
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = ValidValues.validator(Collections.singletonList("A"));
        assertNotNull(validator);

        assertFalse(validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullValues()
    {
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"), v -> null);
        assertNotNull(validator);

        assertTrue(validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = ValidValues.validator(Collections.singletonList("A"));
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        ValidationResult<String> result = validator.isValid(nullSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertNull(result.get());
        assertFalse(validator.isValid((Object) nullSupplier));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        assertFalse(validator.isValid("C"));
        assertFalse(validator.isValid(123));

        validator = ValidValues.validator(Collections.singleton(Arrays.asList("A", "B")));
        assertNotNull(validator);
        assertFalse(validator.isValid(Collections.singletonList("A")));

        Set<String> validValues = new HashSet<>(Arrays.asList("B", "A"));
        validator = ValidValues.validator(Collections.singletonList(validValues));
        assertNotNull(validator);
        assertFalse(validator.isValid(Collections.singleton("A")));

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);
        assertFalse(validator.isValid("A"));
    }

    @Test
    public void testIsValid_InvalidInput_withSupplier()
    {
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        Supplier<String> cSupplier = () -> "C";
        ValidationResult<String> result = validator.isValid(cSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals("C", result.get());
        assertFalse(validator.isValid((Object) cSupplier));

        Supplier<Integer> supplier123 = () -> 123;
        ValidationResult<Integer> result2 = validator.isValid(supplier123);
        assertNotNull(result2);
        assertFalse(result2.isValid());
        assertEquals(Integer.valueOf(123), result2.get());
        assertFalse(validator.isValid((Object) supplier123));

        validator = ValidValues.validator(Collections.singleton(Arrays.asList("A", "B")));
        assertNotNull(validator);

        Supplier<List<String>> listOfASupplier = () -> Collections.singletonList("A");
        ValidationResult<List<String>> result3 = validator.isValid(listOfASupplier);
        assertNotNull(result3);
        assertFalse(result3.isValid());
        assertEquals(Collections.singletonList("A"), result3.get());
        assertFalse(validator.isValid((Object) listOfASupplier));

        Set<String> validValues = new HashSet<>(Arrays.asList("B", "A"));
        validator = ValidValues.validator(Collections.singletonList(validValues));
        assertNotNull(validator);
        Supplier<Set<String>> setOfASupplier = () -> Collections.singleton("A");
        ValidationResult<Set<String>> result4 = validator.isValid(setOfASupplier);
        assertNotNull(result4);
        assertFalse(result4.isValid());
        assertEquals(Collections.singleton("A"), result4.get());
        assertFalse(validator.isValid((Object) setOfASupplier));

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);

        Supplier<String> aSupplier = () -> "A";
        result = validator.isValid(aSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals("A", result.get());
        assertFalse(validator.isValid((Object) aSupplier));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        assertTrue(validator.isValid("A"));

        List<List<String>> validValues = new ArrayList<>();
        validValues.add(Collections.singletonList("A"));
        validValues.add(Arrays.asList("A", "B"));
        validator = ValidValues.validator(validValues);
        assertNotNull(validator);

        assertTrue(validator.isValid(Collections.singletonList("A")));
        assertTrue(validator.isValid(Arrays.asList("A", "B")));

        Set<String> validValue = new HashSet<>(Arrays.asList("B", "A"));
        validator = ValidValues.validator(Collections.singletonList(validValue));
        assertNotNull(validator);
        Set<String> value = new HashSet<>(Arrays.asList("A", "B"));
        assertTrue(validator.isValid(value));

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);

        assertTrue(validator.isValid("a"));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        Supplier<String> aSupplier = () -> "A";
        ValidationResult<String> result = validator.isValid(aSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals("A", result.get());
        assertTrue(validator.isValid((Object) aSupplier));

        List<List<String>> validValues = new ArrayList<>();
        validValues.add(Collections.singletonList("A"));
        validValues.add(Arrays.asList("A", "B"));
        validator = ValidValues.validator(validValues);
        assertNotNull(validator);

        Supplier<List<String>> listOfASupplier = () -> Collections.singletonList("A");
        ValidationResult<List<String>> result2 = validator.isValid(listOfASupplier);
        assertNotNull(result2);
        assertTrue(result2.isValid());
        assertEquals(Collections.singletonList("A"), result2.get());
        assertTrue(validator.isValid((Object) listOfASupplier));

        Supplier<List<String>> listOfABSupplier = () -> Arrays.asList("A", "B");
        ValidationResult<List<String>> result3 = validator.isValid(listOfABSupplier);
        assertNotNull(result3);
        assertTrue(result3.isValid());
        assertEquals(Arrays.asList("A", "B"), result3.get());
        assertTrue(validator.isValid((Object) listOfABSupplier));

        Set<String> validValue = new HashSet<>(Arrays.asList("B", "A"));
        validator = ValidValues.validator(Collections.singletonList(validValue));
        assertNotNull(validator);
        Set<String> value = new HashSet<>(Arrays.asList("A", "B"));
        Supplier<Set<String>> setOfABSupplier = () -> value;
        ValidationResult<Set<String>> result4 = validator.isValid(setOfABSupplier);
        assertNotNull(result4);
        assertTrue(result4.isValid());
        assertEquals(new HashSet<>(Arrays.asList("A", "B")), result4.get());
        assertTrue(validator.isValid((Object) setOfABSupplier));

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "a";
        result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals("a", result.get());
        assertTrue(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = ValidValues.validator(Collections.singletonList("A"));
        assertNotNull(validator);

        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid values are: [A]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_NullAsInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = ValidValues.validator(Collections.singletonList("A"));
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid values are: [A]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid values are: [A]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        try
        {
            validator.validate("A", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        List<List<String>> validValues = new ArrayList<>();
        validValues.add(Collections.singletonList("A"));
        validValues.add(Arrays.asList("A", "B"));
        validator = ValidValues.validator(validValues);
        assertNotNull(validator);

        try
        {
            validator.validate(Collections.singletonList("A"), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate(Arrays.asList("A", "B"), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);

        try
        {
            validator.validate("a", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        try
        {
            Supplier<String> aSupplier = () -> "A";
            Supplier<String> supplier = validator.validate(aSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("A", supplier.get());

            validator.validate((Object) aSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        List<List<String>> validValues = new ArrayList<>();
        validValues.add(Collections.singletonList("A"));
        validValues.add(Arrays.asList("A", "B"));
        validator = ValidValues.validator(validValues);
        assertNotNull(validator);

        try
        {
            Supplier<List<String>> listOfASupplier = () -> Collections.singletonList("A");
            Supplier<List<String>> supplier = validator.validate(listOfASupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Collections.singletonList("A"), supplier.get());

            validator.validate((Object) listOfASupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            Supplier<List<String>> listOfABSupplier = () -> Arrays.asList("A", "B");
            Supplier<List<String>> supplier = validator.validate(listOfABSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Arrays.asList("A", "B"), supplier.get());

            validator.validate((Object) listOfABSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        validator = ValidValues.validator(Arrays.asList("A", "B"), s -> s instanceof String ? ((String) s).toLowerCase() : s);
        assertNotNull(validator);

        try
        {
            Supplier<String> aSupplier = () -> "a";
            Supplier<String> supplier = validator.validate(aSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("a", supplier.get());

            validator.validate((Object) aSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testErrorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B"));
        assertNotNull(validator);

        String message = validator.errorMessage("C", object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'C'. Valid values are: [A, B]", message);
    }

    @Test
    public void testAndThen_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator secondaryValidator = "B"::equals;
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B")).andThen(secondaryValidator);
        assertNotNull(validator);

        assertFalse(validator.isValid("A"));
        assertFalse(validator.isValid("C"));

        try
        {
            validator.validate("A", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'A'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate("C", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'C'. Valid values are: [A, B]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testAndThen_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator secondaryValidator = "B"::equals;
        ValueValidator validator = ValidValues.validator(Arrays.asList("A", "B")).andThen(secondaryValidator);
        assertNotNull(validator);

        assertTrue(validator.isValid("B"));
        try
        {
            validator.validate("B", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}