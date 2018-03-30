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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredDerivedInjectedAttribute;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectInjectedAttribute;
import org.apache.qpid.server.model.ConfiguredObjectInjectedOperation;
import org.apache.qpid.server.model.ConfiguredObjectInjectedStatistic;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredSettableInjectedAttribute;
import org.apache.qpid.server.model.Initialization;
import org.apache.qpid.server.model.InjectedAttributeOrStatistic;
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
            new InjectedAttributeOrStatistic.TypeValidator()
            {
                @Override
                public boolean appliesToType(final Class<? extends ConfiguredObject<?>> type)
                {
                    return TestCar.class.isAssignableFrom(type);
                }
            };

    private static class TestInjector implements ConfiguredObjectAttributeInjector
    {

        private Collection<ConfiguredObjectInjectedAttribute<?, ?>> _injectedAttributes;
        private Collection<ConfiguredObjectInjectedStatistic<?, ?>> _injectedStatistics;
        private Collection<ConfiguredObjectInjectedOperation<?>> _injectedOperations;

        private TestInjector(ConfiguredObjectInjectedAttribute<?, ?>... attributes)
        {
            this(Arrays.asList(attributes),
                 Collections.<ConfiguredObjectInjectedStatistic<?, ?>>emptyList(),
                 Collections.<ConfiguredObjectInjectedOperation<?>>emptyList());
        }

        private TestInjector(ConfiguredObjectInjectedStatistic<?, ?>... statistics)
        {
            this(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>emptyList(),
                 Arrays.asList(statistics),
                 Collections.<ConfiguredObjectInjectedOperation<?>>emptyList());
        }

        private TestInjector(ConfiguredObjectInjectedOperation<?>... operations)
        {
            this(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>emptyList(),
                 Collections.<ConfiguredObjectInjectedStatistic<?, ?>>emptyList(),
                 Arrays.asList(operations));
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
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Integer>("meaningOfLife",
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

        TestModel model = new TestModel(null, new TestInjector(attrInjector));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        assertEquals("incorrect attribute value", Integer.valueOf(42), testCar.getAttribute("meaningOfLife"));

        testCar.setAttributes(Collections.<String,Object>singletonMap("meaningOfLife", 54));

        assertEquals("incorrect attribute value", Integer.valueOf(54), testCar.getAttribute("meaningOfLife"));

        Map<String, String> context = new HashMap<>(testCar.getContext());
        context.put("varieties","57");
        testCar.setAttributes(Collections.<String,Object>singletonMap("context", context));
        testCar.setAttributes(Collections.<String,Object>singletonMap("meaningOfLife", "${varieties}"));

        assertEquals("incorrect attribute value", Integer.valueOf(57), testCar.getAttribute("meaningOfLife"));
    }


    @Test
    public void testInjectedSettableAttributeValidValues()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attrInjector =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Integer>("meaningOfLife",
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

        TestModel model = new TestModel(null, new TestInjector(attrInjector));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        assertEquals("incorrect attribute value", Integer.valueOf(42), testCar.getAttribute("meaningOfLife"));

        testCar.setAttributes(Collections.<String,Object>singletonMap("meaningOfLife", 49));

        assertEquals("incorrect attribute value", Integer.valueOf(49), testCar.getAttribute("meaningOfLife"));

        try
        {
            testCar.setAttributes(Collections.<String, Object>singletonMap("meaningOfLife", 54));
            fail("Should not be able to set attribute value to 54 as it is not a valid value");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testInjectedSettableAttributeEnumValidValues_Unrestricted()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attribute =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Colour>("trimColour",
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

        assertEquals("The attribute's valid values should match the set of the enum",
                            Lists.newArrayList("BLACK", "RED", "BLUE", "GREY"),
                            attribute.validValues());
    }

    @Test
    public void testInjectedSettableAttributeEnumValidValues_RestrictedSet()
    {
        final ConfiguredSettableInjectedAttribute<?, ?> attribute =
                new ConfiguredSettableInjectedAttribute<TestCar<?>, Colour>("trimColour",
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

        assertEquals(
                "The attribute's valid values should match the restricted set defined on the attribute itself",
                Lists.newArrayList("GREY", "BLACK"),
                attribute.validValues());
    }

    @Test
    public void testInjectedDerivedAttribute() throws Exception
    {
        Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);

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

        TestModel model = new TestModel(null, new TestInjector(attrInjector));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        assertEquals("incorrect attribute value", Integer.valueOf(42), testCar.getAttribute("meaningOfLife"));
    }


    @Test
    public void testInjectedStatistic() throws Exception
    {

        Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);

        final ConfiguredObjectInjectedStatistic<?, ?> statInjector =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("meaningOfLife",
                                                                           method,
                                                                           null, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "What is 6 x 9?");

        TestModel model = new TestModel(null, new TestInjector(statInjector));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, Object> statistics = testCar.getStatistics();
        assertEquals("incorrect number of statistics", (long) 1, (long) statistics.size());
        assertEquals("incorrect statistic value", 42, statistics.get("meaningOfLife"));
    }


    @Test
    public void testInjectedStatisticWithParameters() throws Exception
    {

        Method method = InjectedAttributeTest.class.getDeclaredMethod("getWhatISent", TestCar.class, Integer.TYPE);

        final ConfiguredObjectInjectedStatistic<?, ?> statInjector1 =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("whatISent1",
                                                                           method,
                                                                           new Object[] { 1 }, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "One");
        final ConfiguredObjectInjectedStatistic<?, ?> statInjector2 =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("whatISent2",
                                                                           method,
                                                                           new Object[] { 2 }, "",
                                                                           TYPE_VALIDATOR,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "Two");
        TestModel model = new TestModel(null, new TestInjector(statInjector1, statInjector2));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, Object> statistics = testCar.getStatistics();
        assertEquals("incorrect number of statistics", (long) 2, (long) statistics.size());
        assertEquals("incorrect statistic value", 1, statistics.get("whatISent1"));
        assertEquals("incorrect statistic value", 2, statistics.get("whatISent2"));
    }


    @Test
    public void testInjectedOperation() throws Exception
    {
        Method method = InjectedAttributeTest.class.getDeclaredMethod("fly", TestCar.class, Integer.TYPE);

        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("height", Integer.TYPE, Integer.TYPE, "", "", new String[0],
                                                        false);
        final ConfiguredObjectInjectedOperation<?> operationInjector =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("fly", "", true,
                                                                  false,
                                                                  "", params, method, null, TYPE_VALIDATOR);

        TestModel model = new TestModel(null, new TestInjector(operationInjector));

        TestCar testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());

        assertTrue("Operation fly(int height) is missing", allOperations.containsKey("fly"));

        final ConfiguredObjectOperation foundOperation = allOperations.get("fly");

        Object result = foundOperation.perform(testCar, Collections.<String, Object>singletonMap("height", 0));

        assertEquals("Car should be able to fly at 0m", Boolean.TRUE, result);

        result = foundOperation.perform(testCar, Collections.<String, Object>singletonMap("height", 5000));

        assertEquals("Car should not be able to fly at 5000m", Boolean.FALSE, result);
    }

    @Test
    public void testInjectedOperationWithStaticParams() throws Exception
    {

        Method method = InjectedAttributeTest.class.getDeclaredMethod("saySomething", TestCar.class, String.class, Integer.TYPE);

        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("count", Integer.TYPE, Integer.TYPE, "", "", new String[0],
                                                        false);

        final ConfiguredObjectInjectedOperation<?> hello =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("sayHello", "", true,
                                                                  false,
                                                                  "", params, method, new String[] { "Hello"},
                                                                  TYPE_VALIDATOR
                );
        final ConfiguredObjectInjectedOperation<?> goodbye =
                new ConfiguredObjectInjectedOperation<TestCar<?>>("sayGoodbye", "", true,
                                                                  false,
                                                                  "",
                                                                  params, method, new String[] { "Goodbye"},
                                                                  TYPE_VALIDATOR
                );

        TestModel model = new TestModel(null, new TestInjector(hello, goodbye));

        TestCar testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());

        assertTrue("Operation sayHello(int count) is missing", allOperations.containsKey("sayHello"));
        assertTrue("Operation sayGoodbye(int count) is missing", allOperations.containsKey("sayGoodbye"));

        final ConfiguredObjectOperation helloOperation = allOperations.get("sayHello");
        final ConfiguredObjectOperation goodbyeOperation = allOperations.get("sayGoodbye");

        Object result = helloOperation.perform(testCar, Collections.<String, Object>singletonMap("count", 3));

        assertEquals("Car should say 'Hello' 3 times", Arrays.asList("Hello", "Hello", "Hello"), result);

        result = goodbyeOperation.perform(testCar, Collections.<String, Object>singletonMap("count", 1));

        assertEquals("Car say 'Goodbye' once", Collections.singletonList("Goodbye"), result);
    }

    @Test
    public void testOperationWithMandatoryParameter_RejectsNullParameter() throws Exception
    {
        Method method = InjectedAttributeTest.class.getDeclaredMethod("ping", TestCar.class, String.class);

        final OperationParameter[] params = new OperationParameter[1];
        params[0] = new OperationParameterFromInjection("arg", String.class, String.class, "", "", new String[0], true);
        final ConfiguredObjectInjectedOperation<?> operationInjector =
                new ConfiguredObjectInjectedOperation<TestCar<?>>(method.getName(), "", true,
                                                                  false,
                                                                  "", params, method, null, TYPE_VALIDATOR);

        TestModel model = new TestModel(null, new TestInjector(operationInjector));
        TestCar testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, ConfiguredObjectOperation<?>> allOperations =
                model.getTypeRegistry().getOperations(testCar.getClass());

        final ConfiguredObjectOperation operation = allOperations.get(method.getName());

        try
        {
            operation.perform(testCar, Collections.<String, Object>emptyMap());
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException iae)
        {
            // PASS
        }

        try
        {
            operation.perform(testCar, Collections.<String, Object>singletonMap("arg", null));
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException iae)
        {
            // PASS
        }

    }

    @SuppressWarnings("unused")
    public static String ping(TestCar<?> car, String arg)
    {
        return arg;
    }

    public static int getMeaningOfLife(TestCar<?> car)
    {
        return 42;
    }


    public static int getWhatISent(TestCar<?> car, int whatIsent)
    {
        return whatIsent;
    }

    public static boolean fly(TestCar<?> car, int height)
    {
        return height == 0;
    }

    public static List<String> saySomething(TestCar<?> car, String whatToSay, int count)
    {
        List<String> list = new ArrayList<>();
        for(int i = 0; i < count; i++)
        {
            list.add(whatToSay);
        }
        return list;
    }

}
