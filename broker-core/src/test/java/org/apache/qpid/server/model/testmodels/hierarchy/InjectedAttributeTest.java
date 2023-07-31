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
package org.apache.qpid.server.model.testmodels.hierarchy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredDerivedInjectedAttribute;
import org.apache.qpid.server.model.ConfiguredObjectInjectedAttribute;
import org.apache.qpid.server.model.ConfiguredObjectInjectedOperation;
import org.apache.qpid.server.model.ConfiguredObjectInjectedStatistic;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredSettableInjectedAttribute;
import org.apache.qpid.server.model.Initialization;
import org.apache.qpid.server.model.InjectedAttributeStatisticOrOperation;
import org.apache.qpid.server.model.OperationParameter;
import org.apache.qpid.server.model.OperationParameterFromInjection;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar.Colour;
import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.test.utils.UnitTestBase;

public class InjectedAttributeTest extends UnitTestBase
{
    private static final InjectedAttributeStatisticOrOperation.TypeValidator TYPE_VALIDATOR =
            TestCar.class::isAssignableFrom;

    private static class TestInjector implements ConfiguredObjectAttributeInjector
    {
        private final Collection<ConfiguredObjectInjectedAttribute<?, ?>> _injectedAttributes;
        private final Collection<ConfiguredObjectInjectedStatistic<?, ?>> _injectedStatistics;
        private final Collection<ConfiguredObjectInjectedOperation<?>> _injectedOperations;

        private TestInjector(ConfiguredObjectInjectedAttribute<?, ?>... attributes)
        {
            this(Arrays.asList(attributes), List.of(), List.of());
        }

        private TestInjector(ConfiguredObjectInjectedStatistic<?, ?>... statistics)
        {
            this(List.of(), Arrays.asList(statistics), List.of());
        }

        private TestInjector(ConfiguredObjectInjectedOperation<?>... operations)
        {
            this(List.of(), List.of(), Arrays.asList(operations));
        }

        private TestInjector(final Collection<ConfiguredObjectInjectedAttribute<?, ?>> injectedAttributes,
                             final Collection<ConfiguredObjectInjectedStatistic<?, ?>> injectedStatistics,
                             final Collection<ConfiguredObjectInjectedOperation<?>> injectedOperations)
        {
            _injectedAttributes = injectedAttributes;
            _injectedStatistics = injectedStatistics;
            _injectedOperations = injectedOperations;
        }

        @Override
        public Collection<ConfiguredObjectInjectedAttribute<?, ?>> getInjectedAttributes()
        {
            return _injectedAttributes;
        }

        @Override
        public Collection<ConfiguredObjectInjectedStatistic<?, ?>> getInjectedStatistics()
        {
            return _injectedStatistics;
        }

        @Override
        public Collection<ConfiguredObjectInjectedOperation<?>> getInjectedOperations()
        {
            return _injectedOperations;
        }

        @Override
        public String getType()
        {
            return "TEST";
        }
    }

    @Test
    public void testInjectedSettableAttributeWithDefault()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attrInjector =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Integer>(
                        "meaningOfLife",
                        Integer.class,
                        Integer.class,
                        "42",
                        false,
                        true,
                        false,
                        "",
                        false,
                        "",
                        "",
                        null,
                        "", TYPE_VALIDATOR, Initialization.none);

        final TestModel model = new TestModel(null, new TestInjector(attrInjector));

        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);

        assertEquals(42, testCar.getAttribute("meaningOfLife"), "incorrect attribute value");

        testCar.setAttributes(Map.of("meaningOfLife", 54));

        assertEquals(54, testCar.getAttribute("meaningOfLife"), "incorrect attribute value");

        final Map<String, String> context = new HashMap<>(testCar.getContext());
        context.put("varieties","57");
        testCar.setAttributes(Map.of("context", context));
        testCar.setAttributes(Map.of("meaningOfLife", "${varieties}"));

        assertEquals(57, testCar.getAttribute("meaningOfLife"), "incorrect attribute value");
    }


    @Test
    public void testInjectedSettableAttributeValidValues()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attrInjector =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Integer>(
                        "meaningOfLife",
                        Integer.class,
                        Integer.class,
                        "42",
                        false,
                        true,
                        false,
                        "",
                        false,
                        "",
                        "",
                        new String[] { "42", "49" },
                        "", TYPE_VALIDATOR, Initialization.none);

        final TestModel model = new TestModel(null, new TestInjector(attrInjector));

        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);

        assertEquals(42, testCar.getAttribute("meaningOfLife"), "incorrect attribute value");

        testCar.setAttributes(Map.of("meaningOfLife", 49));

        assertEquals(49,  testCar.getAttribute("meaningOfLife"), "incorrect attribute value");
        assertThrows(IllegalConfigurationException.class,
                () -> testCar.setAttributes(Map.of("meaningOfLife", 54)),
                "Should not be able to set attribute value to 54 as it is not a valid value");
    }

    @Test
    public void testInjectedSettableAttributeEnumValidValues_Unrestricted()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attribute =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Colour>(
                        "trimColour",
                        Colour.class,
                        Colour.class,
                        Colour.BLACK.name(),
                        false,
                        true,
                        false,
                        "",
                        false,
                        "",
                        "",
                        null,
                        "",
                        null, Initialization.none);

        assertEquals(List.of("BLACK", "RED", "BLUE", "GREY"), attribute.validValues(),
                "The attribute's valid values should match the set of the enum");
    }

    @Test
    public void testInjectedSettableAttributeEnumValidValues_RestrictedSet()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attribute =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Colour>(
                        "trimColour",
                        Colour.class,
                        Colour.class,
                        Colour.BLACK.name(),
                        false,
                        true,
                        false,
                        "",
                        false,
                        "",
                        "",
                        new String[] {Colour.GREY.name(), Colour.BLACK.name()},
                        "",
                        null, Initialization.none);

        assertEquals(List.of("GREY", "BLACK"), attribute.validValues(),
                "The attribute's valid values should match the restricted set defined on the attribute itself");
    }

    @Test
    public void testInjectedDerivedAttribute() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);

        final ConfiguredDerivedInjectedAttribute<?, ?> attrInjector =
                new ConfiguredDerivedInjectedAttribute<TestCar<?>, Integer>("meaningOfLife",
                                                                            method,
                                                                            null, false,
                                                                            false,
                                                                            "",
                                                                            false,
                                                                            "",
                                                                            "",
                                                                            TYPE_VALIDATOR);

        final TestModel model = new TestModel(null, new TestInjector(attrInjector));

        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);

        assertEquals(42, testCar.getAttribute("meaningOfLife"), "incorrect attribute value");
    }

    @Test
    public void testInjectedStatistic() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);
        final ConfiguredObjectInjectedStatistic<?, ?> statInjector =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("meaningOfLife",
                                                                           method,
                                                                           null, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "What is 6 x 9?",
                                                                           null,
                                                                           false);
        final TestModel model = new TestModel(null, new TestInjector(statInjector));
        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);
        final Map<String, Object> statistics = testCar.getStatistics();

        assertEquals(3, (long) statistics.size(), "incorrect number of statistics");
        assertEquals(42, statistics.get("meaningOfLife"), "incorrect statistic value");
    }

    @Test
    public void testInjectedStatisticWithParameters() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("getWhatISent", TestCar.class, Integer.TYPE);
        final ConfiguredObjectInjectedStatistic<?, ?> statInjector1 =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("whatISent1",
                                                                           method,
                                                                           new Object[] { 1 }, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "One",
                                                                           null,
                                                                           false);
        final ConfiguredObjectInjectedStatistic<?, ?> statInjector2 =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("whatISent2",
                                                                           method,
                                                                           new Object[] { 2 }, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "Two",
                                                                           null,
                                                                           false);
        final TestModel model = new TestModel(null, new TestInjector(statInjector1, statInjector2));
        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);
        final Map<String, Object> statistics = testCar.getStatistics();

        assertEquals(4, (long) statistics.size(), "incorrect number of statistics");
        assertEquals(1, statistics.get("whatISent1"), "incorrect statistic value");
        assertEquals(2, statistics.get("whatISent2"), "incorrect statistic value");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testInjectedOperation() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("fly", TestCar.class, Integer.TYPE);
        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("height", Integer.TYPE, Integer.TYPE, "", "", new String[0],
                                                        false);
        final ConfiguredObjectInjectedOperation<?> operationInjector =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("fly", "", true,
                                                                  false,
                                                                  "", params, method, null, TYPE_VALIDATOR);
        final TestModel model = new TestModel(null, new TestInjector(operationInjector));
        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);
        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());

        assertTrue(allOperations.containsKey("fly"), "Operation fly(int height) is missing");

        final ConfiguredObjectOperation foundOperation = allOperations.get("fly");

        Object result = foundOperation.perform(testCar, Map.of("height", 0));

        assertEquals(Boolean.TRUE, result, "Car should be able to fly at 0m");

        result = foundOperation.perform(testCar, Map.of("height", 5000));

        assertEquals(Boolean.FALSE, result, "Car should not be able to fly at 5000m");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testInjectedOperationWithStaticParams() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("saySomething", TestCar.class, String.class, Integer.TYPE);
        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("count", Integer.TYPE, Integer.TYPE, "", "", new String[0], false);
        final ConfiguredObjectInjectedOperation<?> hello =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("sayHello", "", true,
                                                                  false,
                                                                  "", params, method, new String[] { "Hello"},
                                                                  TYPE_VALIDATOR);
        final ConfiguredObjectInjectedOperation<?> goodbye =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("sayGoodbye", "", true,
                                                                  false,
                                                                  "",
                                                                  params, method, new String[] { "Goodbye"},
                                                                  TYPE_VALIDATOR);
        final TestModel model = new TestModel(null, new TestInjector(hello, goodbye));
        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);
        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());

        assertTrue(allOperations.containsKey("sayHello"), "Operation sayHello(int count) is missing");
        assertTrue(allOperations.containsKey("sayGoodbye"), "Operation sayGoodbye(int count) is missing");

        final ConfiguredObjectOperation helloOperation = allOperations.get("sayHello");
        final ConfiguredObjectOperation goodbyeOperation = allOperations.get("sayGoodbye");

        Object result = helloOperation.perform(testCar, Map.of("count", 3));

        assertEquals(Arrays.asList("Hello", "Hello", "Hello"), result, "Car should say 'Hello' 3 times");

        result = goodbyeOperation.perform(testCar, Map.of("count", 1));

        assertEquals(List.of("Goodbye"), result, "Car say 'Goodbye' once");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testOperationWithMandatoryParameter_RejectsNullParameter() throws Exception
    {
        final Method method = InjectedAttributeTest.class.getDeclaredMethod("ping", TestCar.class, String.class);
        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("arg", String.class, String.class, "", "", new String[0], true);
        final ConfiguredObjectInjectedOperation<?> operationInjector =
                new ConfiguredObjectInjectedOperation<TestCar<?>>(method.getName(), "", true,
                                                                  false,
                                                                  "", params, method, null, TYPE_VALIDATOR);
        final TestModel model = new TestModel(null, new TestInjector(operationInjector));
        final TestCar<?> testCar = new TestStandardCarImpl(Map.of("name", "Arthur"), model);
        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());
        final ConfiguredObjectOperation operation = allOperations.get(method.getName());

        assertThrows(IllegalArgumentException.class, () -> operation.perform(testCar, Map.of()), "Exception not thrown");
        assertThrows(IllegalArgumentException.class, () -> operation.perform(testCar, Collections.singletonMap("arg", null)),
                "Exception not thrown");
    }

    @SuppressWarnings("unused")
    public static String ping(TestCar<?> car, String arg)
    {
        return arg;
    }

    @SuppressWarnings("unused")
    public static int getMeaningOfLife(TestCar<?> car)
    {
        return 42;
    }

    @SuppressWarnings("unused")
    public static int getWhatISent(TestCar<?> car, int whatIsent)
    {
        return whatIsent;
    }

    @SuppressWarnings("unused")
    public static boolean fly(TestCar<?> car, int height)
    {
        return height == 0;
    }

    @SuppressWarnings("unused")
    public static List<String> saySomething(TestCar<?> car, String whatToSay, int count)
    {
        return Collections.nCopies(count, whatToSay);
    }
}
