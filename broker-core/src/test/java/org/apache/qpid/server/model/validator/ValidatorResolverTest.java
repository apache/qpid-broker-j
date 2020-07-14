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

import org.apache.qpid.server.model.Initialization;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ValidatorResolverTest extends UnitTestBase
{
    static class Annotation implements ManagedAttribute
    {
        public boolean mandatory = false;
        public boolean immutable = false;
        public String defaultValue = "";

        public String[] validValues = new String[0];
        public String valuePattern;
        public String validator;

        @Override
        public boolean secure()
        {
            return false;
        }

        @Override
        public boolean mandatory()
        {
            return mandatory;
        }

        @Override
        public boolean persist()
        {
            return false;
        }

        @Override
        public String defaultValue()
        {
            return defaultValue;
        }

        @Override
        public String description()
        {
            return "";
        }

        @Override
        public String[] validValues()
        {
            return validValues;
        }

        @Override
        public String validValuePattern()
        {
            return valuePattern;
        }

        @Override
        public boolean oversize()
        {
            return false;
        }

        @Override
        public String oversizedAltText()
        {
            return "";
        }

        @Override
        public String secureValueFilter()
        {
            return "";
        }

        @Override
        public boolean immutable()
        {
            return immutable;
        }

        @Override
        public Initialization initialization()
        {
            return Initialization.none;
        }

        @Override
        public boolean updateAttributeDespiteUnchangedValue()
        {
            return false;
        }

        @Override
        public String validator()
        {
            return validator;
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            return Annotation.class;
        }
    }

    @Test
    public void testNewInstance()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);

        resolver = ValidatorResolver.newInstance("", new String[0], false, String.class, "attr");
        assertNotNull(resolver);
    }

    @Test
    public void testValidator_withValidValues()
    {
        TestConfiguredObject object = new TestConfiguredObject();

        String[] validValues = {"A", "B"};
        Function<Object, ?> converter = value -> (value instanceof String) ? ((String) value).toLowerCase() : value;

        Annotation annotation = new Annotation();
        annotation.validValues = validValues;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValues(resolver, converter);

        resolver = ValidatorResolver.newInstance(null, validValues, false, String.class, "attr");
        testValidator_withValidValues(resolver, converter);
    }

    public void testValidator_withValidValues(Resolver resolver, Function<Object, ?> converter)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(converter);
        assertNotNull(validator);
        assertTrue(validator.isValid("a"));
        assertTrue(validator.isValid("b"));
        assertFalse(validator.isValid("c"));
        assertFalse(validator.isValid("A"));
        assertFalse(validator.isValid("B"));
        assertFalse(validator.isValid("C"));
        assertFalse(validator.isValid(123));
        assertTrue(validator.isValid((String) null));
    }

    public static List<String> values()
    {
        return Arrays.asList("A", "B", "C");
    }

    @Test
    public void testValidator_withValidValuesMethod()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String[] validValues = {"org.apache.qpid.server.model.validator.ValidatorResolverTest#values()"};
        Function<Object, ?> converter = value -> (value instanceof String) ? ((String) value).toLowerCase() : value;

        Annotation annotation = new Annotation();
        annotation.validValues = validValues;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod(resolver, converter);

        resolver = ValidatorResolver.newInstance(null, validValues, false, String.class, "attr");
        testValidator_withValidValuesMethod(resolver, converter);
    }

    private void testValidator_withValidValuesMethod(Resolver resolver, Function<Object, ?> converter)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(converter);
        assertNotNull(validator);
        assertTrue(validator.isValid("a"));
        assertTrue(validator.isValid("b"));
        assertTrue(validator.isValid("c"));
        assertFalse(validator.isValid("A"));
        assertFalse(validator.isValid("B"));
        assertFalse(validator.isValid("C"));
        assertFalse(validator.isValid(123));
        assertTrue(validator.isValid((String) null));
    }

    @Test
    public void testValidator_withoutValidValuesMethod()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String[] validValues = {"[A]"};
        Function<Object, ?> converter = value -> (value instanceof String) ? ((String) value).toLowerCase() : value;

        Annotation annotation = new Annotation();
        annotation.validValues = validValues;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");

        testValidator_withoutValidValuesMethod(resolver, converter);

        resolver = ValidatorResolver.newInstance(null, validValues, false, String.class, "attr");
        testValidator_withoutValidValuesMethod(resolver, converter);
    }

    private void testValidator_withoutValidValuesMethod(Resolver resolver, Function<Object, ?> converter)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(converter);
        assertNotNull(validator);
        assertTrue(validator.isValid("[a]"));
        assertFalse(validator.isValid("[b]"));
        assertFalse(validator.isValid("[c]"));
        assertFalse(validator.isValid("[A]"));
        assertFalse(validator.isValid("[B]"));
        assertFalse(validator.isValid("[C]"));
        assertFalse(validator.isValid(123));
        assertTrue(validator.isValid((String) null));
    }

    @Test
    public void testValidator_withValidValues_Mandatory()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String[] validValues = {"A", "B"};
        Function<Object, ?> converter = value -> (value instanceof String) ? ((String) value).toLowerCase() : value;

        Annotation annotation = new Annotation();
        annotation.validValues = validValues;
        annotation.mandatory = true;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValues_Mandatory(resolver, converter);
    }

    private void testValidator_withValidValues_Mandatory(Resolver resolver, Function<Object, ?> converter)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(converter, () -> "X");
        assertNotNull(validator);
        assertTrue(validator.isValid("a"));
        assertTrue(validator.isValid("b"));
        assertFalse(validator.isValid("c"));
        assertFalse(validator.test("A"));
        assertFalse(validator.test("B"));
        assertFalse(validator.test("C"));
        assertFalse(validator.test(123));
        assertFalse(validator.test((String) null));
    }

    @Test
    public void testValidator_withValidValues_Immutable()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String[] validValues = {"A", "B"};
        Function<Object, ?> converter = value -> (value instanceof String) ? ((String) value).toLowerCase() : value;

        Annotation annotation = new Annotation();
        annotation.validValues = validValues;
        annotation.immutable = true;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValues_Immutable(resolver, converter);

        resolver = ValidatorResolver.newInstance(null, validValues, true, String.class, "attr");
        testValidator_withValidValues_Immutable(resolver, converter);
    }

    private void testValidator_withValidValues_Immutable(Resolver resolver, Function<Object, ?> converter)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(converter, () -> "b");
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertTrue(validator.test("b"));
        assertFalse(validator.test("c"));
        assertFalse(validator.test("A"));
        assertFalse(validator.test("B"));
        assertFalse(validator.test("C"));
        assertFalse(validator.test(123));
        assertFalse(validator.test((String) null));
    }

    @Test
    public void testValidator_withValidValuePattern()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String valuePattern = "\\w+";

        Annotation annotation = new Annotation();
        annotation.valuePattern = valuePattern;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuePattern(resolver);

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), List.class, "attr");
        testValidator_withValidValuePattern_ListAsInput(resolver);

        resolver = ValidatorResolver.newInstance(valuePattern, null, false, String.class, "attr");
        testValidator_withValidValuePattern(resolver);

        resolver = ValidatorResolver.newInstance(valuePattern, null, false, List.class, "attr");
        testValidator_withValidValuePattern_ListAsInput(resolver);
    }

    private void testValidator_withValidValuePattern(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("{A}"));
        assertFalse(validator.test(""));
        assertTrue(validator.test("b"));
        assertTrue(validator.test("X"));
        assertTrue(validator.test((String) null));
    }

    private void testValidator_withValidValuePattern_ListAsInput(Resolver resolver)
    {
        ValueValidator validator;
        assertNotNull(resolver);

        validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertTrue(validator.test(Collections.singletonList("A")));
        assertTrue(validator.test(Arrays.asList("A", "B")));
        assertFalse(validator.test(Arrays.asList("A", "")));
        assertFalse(validator.test(Arrays.asList("{}}", "B")));
        assertTrue(validator.test((List<String>) null));
    }

    @Test
    public void testValidator_withValidValuePattern_Mandatory()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String valuePattern = "\\w+";

        Annotation annotation = new Annotation();
        annotation.mandatory = true;
        annotation.valuePattern = valuePattern;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuePattern_Mandatory(resolver);
    }

    private void testValidator_withValidValuePattern_Mandatory(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("{A}"));
        assertFalse(validator.test(""));
        assertTrue(validator.test("b"));
        assertTrue(validator.test("X"));
        assertFalse(validator.test((String) null));
    }

    @Test
    public void testValidator_withValidValuePattern_Immutable()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String valuePattern = "\\w+";

        Annotation annotation = new Annotation();
        annotation.immutable = true;
        annotation.valuePattern = valuePattern;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuePattern_Immutable(resolver);

        resolver = ValidatorResolver.newInstance(valuePattern, null, true, String.class, "attr");
        testValidator_withValidValuePattern_Immutable(resolver);
    }

    public void testValidator_withValidValuePattern_Immutable(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity(), () -> "X");
        assertNotNull(validator);
        assertFalse(validator.test("{A}"));
        assertFalse(validator.test(""));
        assertFalse(validator.test("b"));
        assertTrue(validator.test("X"));
        assertFalse(validator.test((String) null));
    }

    @Test
    public void testValidator_withValidator()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String validato = "org.apache.qpid.server.model.validator.AtLeastOne";

        Annotation annotation = new Annotation();
        annotation.validator = validato;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator(resolver);
    }

    private void testValidator_withValidator(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity(), () -> 123);
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertFalse(validator.test(-1));
        assertTrue(validator.test(234L));
        assertTrue(validator.test(123));
        assertTrue(validator.test((Integer) null));
    }

    @Test
    public void testValidator_withValidator_Mandatory()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String validator = "org.apache.qpid.server.model.validator.AtLeastOne";

        Annotation annotation = new Annotation();
        annotation.validator = validator;
        annotation.mandatory = true;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Mandatory(resolver);
    }

    private void testValidator_withValidator_Mandatory(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertFalse(validator.test(-1));
        assertTrue(validator.test(234L));
        assertTrue(validator.test(123));
        assertFalse(validator.test((Integer) null));
    }

    @Test
    public void testValidator_withValidator_Immutable()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        String validator = "org.apache.qpid.server.model.validator.AtLeastOne";

        Annotation annotation = new Annotation();
        annotation.validator = validator;
        annotation.immutable = true;

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Immutable(resolver);
    }

    private void testValidator_withValidator_Immutable(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity(), () -> 123);
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertFalse(validator.test(-1));
        assertFalse(validator.test(234));
        assertTrue(validator.test(123));
        assertFalse(validator.test((Integer) null));
    }

    @Test
    public void testValidator_withValidValuesMethod_InvalidClassName()
    {
        TestConfiguredObject object = new TestConfiguredObject();

        Annotation annotation = new Annotation();
        String[] validValues1 = {"og.apache.qpid.server.model.validator.ValidatorResolverTest#values()"};

        annotation.validValues = validValues1;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        resolver = ValidatorResolver.newInstance(null, validValues1, false, String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        String[] validValues2 = {"{org.apache.qpid.server.model.validator.ValidatorResolverTest}#values()"};

        annotation.validValues = validValues2;
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        resolver = ValidatorResolver.newInstance(null, validValues2, false, String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);
    }

    private void testValidator_withValidValuesMethod_Invalid(Resolver resolver)
    {
        assertNotNull(resolver);
        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("ABC"));
    }

    @Test
    public void testValidator_withValidValuesMethod_InvalidMethodName()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        String[] validValues1 = {"org.apache.qpid.server.model.validator.ValidatorResolverTest#val()"};

        annotation.validValues = validValues1;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        resolver = ValidatorResolver.newInstance(null, validValues1, false, String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        String[] validValues2 = {"org.apache.qpid.server.model.validator.ValidatorResolverTest$values()"};

        annotation.validValues = validValues2;
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        resolver = ValidatorResolver.newInstance(null, validValues2, false, String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);
    }

    public static class ValidValuesTestClass
    {
        public interface ParentCollection<T, P> extends Collection<T>
        {
            P parent();
        }

        public interface StringList extends Collection<String>
        {
        }

        public List<String> nonStaticMethod()
        {
            return Collections.singletonList("ABC");
        }

        protected static List<String> nonPublicMethod()
        {
            return Collections.singletonList("ABC");
        }

        public static List<String> methodWithArgument(String str)
        {
            return Collections.singletonList(str);
        }

        public static String[] arrayReturnMethod()
        {
            return new String[]{"ABC"};
        }

        public static ParentCollection<String, Object> parentListMethod()
        {
            return new ParentCollection<String, Object>()
            {
                @Override
                public String parent()
                {
                    return "";
                }

                @Override
                public int size()
                {
                    return 1;
                }

                @Override
                public boolean isEmpty()
                {
                    return false;
                }

                @Override
                public boolean contains(Object o)
                {
                    return "ABC".equals(o);
                }

                @Override
                public Iterator<String> iterator()
                {
                    return Collections.singletonList("ABC").iterator();
                }

                @Override
                public Object[] toArray()
                {
                    return new String[]{"ABC"};
                }

                @Override
                public <T> T[] toArray(T[] a)
                {
                    return a;
                }

                @Override
                public boolean add(String s)
                {
                    return false;
                }

                @Override
                public boolean remove(Object o)
                {
                    return false;
                }

                @Override
                public boolean containsAll(Collection<?> c)
                {
                    return Collections.singletonList("ABC").containsAll(c);
                }

                @Override
                public boolean addAll(Collection<? extends String> c)
                {
                    return false;
                }

                @Override
                public boolean removeAll(Collection<?> c)
                {
                    return false;
                }

                @Override
                public boolean retainAll(Collection<?> c)
                {
                    return false;
                }

                @Override
                public void clear()
                {
                }
            };
        }

        public static StringList stringListMethod()
        {
            return new StringList()
            {
                @Override
                public int size()
                {
                    return 1;
                }

                @Override
                public boolean isEmpty()
                {
                    return false;
                }

                @Override
                public boolean contains(Object o)
                {
                    return "ABC".equals(o);
                }

                @Override
                public Iterator<String> iterator()
                {
                    return Collections.singletonList("ABC").iterator();
                }

                @Override
                public Object[] toArray()
                {
                    return new String[]{"ABC"};
                }

                @Override
                public <T> T[] toArray(T[] a)
                {
                    return a;
                }

                @Override
                public boolean add(String s)
                {
                    return false;
                }

                @Override
                public boolean remove(Object o)
                {
                    return false;
                }

                @Override
                public boolean containsAll(Collection<?> c)
                {
                    return Collections.singletonList("ABC").containsAll(c);
                }

                @Override
                public boolean addAll(Collection<? extends String> c)
                {
                    return false;
                }

                @Override
                public boolean removeAll(Collection<?> c)
                {
                    return false;
                }

                @Override
                public boolean retainAll(Collection<?> c)
                {
                    return false;
                }

                @Override
                public void clear()
                {
                }
            };
        }

        public static List<Object> objectListReturnMethod()
        {
            return Collections.singletonList("ABC");
        }

        public static List<String> runtimeException()
        {
            throw new RuntimeException("Error");
        }
    }

    @Test
    public void testValidator_withValidValuesMethod_InvalidMethodType()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#nonStaticMethod()"};
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#nonPublicMethod()"};
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#methodWithArgument(\"ABC\")"};
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);
    }

    @Test
    public void testValidator_withValidValuesMethod_InvalidReturnType()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#arrayReturnMethod()"};
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#parentListMethod()"};
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#stringListMethod()"};
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#objectListReturnMethod()"};
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        testValidator_withValidValuesMethod_Invalid(resolver);
    }

    @Test
    public void testValidator_withValidValuesMethod_Exception()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validValues = new String[]{"org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidValuesTestClass#runtimeException"};
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
    }

    public static class Equals implements ValueValidator
    {

        private String str = "ABC";

        public static ValueValidator a()
        {
            return new Equals("a");
        }

        public static ValueValidator ab()
        {
            return new Equals("ab");
        }

        public static ValueValidator abc()
        {
            return new Equals("abc");
        }

        public Equals()
        {
            super();
        }

        public Equals(String str)
        {
            super();
            this.str = str;
        }

        @Override
        public boolean test(Object value)
        {
            return str.equals(value);
        }
    }

    @Test
    public void testValidator_withValidator_FactoryMethod()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();
        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$Equals#ab";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertTrue(validator.test("ab"));
        assertFalse(validator.test("abc"));
    }

    @Test
    public void testValidator_withValidator_DefaultConstructor()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();
        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$Equals";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertFalse(validator.test("a"));
        assertFalse(validator.test("ab"));
        assertFalse(validator.test("abc"));
        assertTrue(validator.test("ABC"));
    }

    @Test
    public void testValidator_withValidator_InvalidClassName()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validator = "og.apache.qpid.server.model.validator.ValidatorResolverTest$Equals#ab";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);

        annotation.validator = "{org.apache.qpid.server.model.validator.ValidatorResolverTest$Equals}#ab";

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);
    }

    private void testValidator_withValidator_Invalid(Resolver resolver)
    {
        assertNotNull(resolver);

        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
        assertTrue(validator.test("a"));
        assertTrue(validator.test("ab"));
        assertTrue(validator.test("abc"));
    }

    public static class ValidatorTestClass implements ValueValidator
    {
        public ValidatorTestClass nonStaticMethod()
        {
            return new ValidatorTestClass();
        }

        protected static ValidatorTestClass nonPublicMethod()
        {
            return new ValidatorTestClass();
        }

        public static ValidatorTestClass methodWithArgument(String str)
        {
            return new ValidatorTestClass();
        }

        public static String stringReturnType()
        {
            return "";
        }

        public static ValidatorTestClass runtimeException()
        {
            throw new RuntimeException("Error");
        }

        private ValidatorTestClass()
        {
        }

        @Override
        public boolean test(Object value)
        {
            return false;
        }

    }

    public static class NoValidatorTestClass
    {

    }

    @Test
    public void testValidator_withValidator_InvalidMethodType()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClass#nonStaticMethod";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClass#nonPublicMethod";

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClass#methodWithArgument";

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);
    }

    @Test
    public void testValidator_withValidator_MethodException()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClass#runtimeException";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        assertNotNull(resolver);
        ValueValidator validator = resolver.validator(Function.identity());
        assertNotNull(validator);
    }

    @Test
    public void testValidator_withValidator_InvalidConstructor()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClass";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$NoValidatorTestClass";

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);
    }

    public static class ValidatorTestClassRuntimeException implements ValueValidator
    {
        @Override
        public boolean test(Object value)
        {
            return false;
        }

        public ValidatorTestClassRuntimeException()
        {
            throw new RuntimeException("Error");
        }
    }

    public static class ValidatorTestClassInstantiationException implements ValueValidator
    {
        @Override
        public boolean test(Object value)
        {
            return false;
        }

        public ValidatorTestClassInstantiationException() throws InstantiationException
        {
            throw new InstantiationException("Error");
        }
    }

    @Test
    public void testValidator_withValidator_ConstructorException()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClassRuntimeException";

        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);

        annotation.validator = "org.apache.qpid.server.model.validator.ValidatorResolverTest$ValidatorTestClassInstantiationException";

        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), Integer.class, "attr");
        testValidator_withValidator_Invalid(resolver);
    }

    @Test
    public void testIsMandatory()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.mandatory = false;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);
        assertFalse(resolver.isMandatory());

        annotation.mandatory = true;
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);
        assertTrue(resolver.isMandatory());

        resolver = ValidatorResolver.newInstance(null, null, false, String.class, "attr");
        assertNotNull(resolver);
        assertFalse(resolver.isMandatory());
    }

    @Test
    public void testIsImmutable()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();

        annotation.immutable = false;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);
        assertFalse(resolver.isImmutable());

        annotation.immutable = true;
        resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);
        assertTrue(resolver.isImmutable());

        resolver = ValidatorResolver.newInstance(null, null, false, String.class, "attr");
        assertNotNull(resolver);
        assertFalse(resolver.isImmutable());

        resolver = ValidatorResolver.newInstance(null, null, true, String.class, "attr");
        assertNotNull(resolver);
        assertTrue(resolver.isImmutable());
    }

    @Test
    public void testValidator_FromAnnotation()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        Annotation annotation = new Annotation();
        String validator = "org.apache.qpid.server.model.validator.AtLeastOne";

        annotation.validator = validator;
        Resolver resolver = ValidatorResolver.newInstance(annotation, object.getClass(), String.class, "attr");
        assertNotNull(resolver);
        assertEquals(validator, resolver.validator());

        resolver = ValidatorResolver.newInstance(null, null, true, String.class, "attr");
        assertNotNull(resolver);
        assertEquals("", resolver.validator());
    }
}