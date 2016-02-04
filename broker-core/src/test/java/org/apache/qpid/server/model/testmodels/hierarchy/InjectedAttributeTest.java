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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.security.AccessControlException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.test.utils.QpidTestCase;

public class InjectedAttributeTest extends QpidTestCase
{

    private static class TestInjector implements ConfiguredObjectAttributeInjector
    {

        private Collection<ConfiguredObjectInjectedAttribute<?, ?>> _injectedAttributes;
        private Collection<ConfiguredObjectInjectedStatistic<?, ?>> _injectedStatistics;

        private TestInjector(final Collection<ConfiguredObjectInjectedAttribute<?, ?>> injectedAttributes,
                             final Collection<ConfiguredObjectInjectedStatistic<?, ?>> injectedStatistics)
        {
            _injectedAttributes = injectedAttributes;
            _injectedStatistics = injectedStatistics;
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
        public String getType()
        {
            return "TEST";
        }
    }

    public void testInjectedSettableAttributeWithDefault()
    {
        InjectedAttributeOrStatistic.TypeValidator validator =
                new InjectedAttributeOrStatistic.TypeValidator()
                {
                    @Override
                    public boolean appliesToType(final Class<? extends ConfiguredObject<?>> type)
                    {
                        return TestCar.class.isAssignableFrom(type);
                    }
                };

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
                                                                             validator);

        TestModel model = new TestModel(null, Collections.<ConfiguredObjectAttributeInjector>singleton(new TestInjector(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>singletonList(
                attrInjector), Collections.<ConfiguredObjectInjectedStatistic<?, ?>>emptySet())));

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


    public void testInjectedSettableAttributeValidValues()
    {

        InjectedAttributeOrStatistic.TypeValidator validator =
                new InjectedAttributeOrStatistic.TypeValidator()
                {
                    @Override
                    public boolean appliesToType(final Class<? extends ConfiguredObject<?>> type)
                    {
                        return TestCar.class.isAssignableFrom(type);
                    }
                };

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
                                                                             validator);

        TestModel model = new TestModel(null, Collections.<ConfiguredObjectAttributeInjector>singleton(new TestInjector(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>singletonList(
                attrInjector), Collections.<ConfiguredObjectInjectedStatistic<?, ?>>emptySet())));

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

    public void testInjectedDerivedAttribute() throws Exception
    {
        Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);
        InjectedAttributeOrStatistic.TypeValidator validator =
                new InjectedAttributeOrStatistic.TypeValidator()
                {
                    @Override
                    public boolean appliesToType(final Class<? extends ConfiguredObject<?>> type)
                    {
                        return TestCar.class.isAssignableFrom(type);
                    }
                };

        final ConfiguredDerivedInjectedAttribute<?, ?> attrInjector =
                new ConfiguredDerivedInjectedAttribute<TestCar<?>, Integer>("meaningOfLife",
                                                                            method,
                                                                            false,
                                                                            false,
                                                                            "",
                                                                            false,
                                                                            "",
                                                                            "",
                                                                            validator);

        TestModel model = new TestModel(null, Collections.<ConfiguredObjectAttributeInjector>singleton(new TestInjector(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>singletonList(
                attrInjector), Collections.<ConfiguredObjectInjectedStatistic<?, ?>>emptySet())));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);


        assertEquals("incorrect attribute value", Integer.valueOf(42), testCar.getAttribute("meaningOfLife"));

    }


    public void testInjectedStatistic() throws Exception
    {

        Method method = InjectedAttributeTest.class.getDeclaredMethod("getMeaningOfLife", TestCar.class);
        InjectedAttributeOrStatistic.TypeValidator validator =
                new InjectedAttributeOrStatistic.TypeValidator()
                {
                    @Override
                    public boolean appliesToType(final Class<? extends ConfiguredObject<?>> type)
                    {
                        return TestCar.class.isAssignableFrom(type);
                    }
                };

        final ConfiguredObjectInjectedStatistic<?, ?> statInjector =
                new ConfiguredObjectInjectedStatistic<TestCar<?>, Integer>("meaningOfLife",
                                                                           method,
                                                                           "",
                                                                           validator,
                                                                           StatisticUnit.COUNT,
                                                                           StatisticType.POINT_IN_TIME,
                                                                           "What is 6 x 9?");

        TestModel model = new TestModel(null, Collections.<ConfiguredObjectAttributeInjector>singleton(new TestInjector(Collections.<ConfiguredObjectInjectedAttribute<?, ?>>emptyList(), Collections.<ConfiguredObjectInjectedStatistic<?, ?>>singletonList(statInjector))));

        TestCar<?> testCar = new TestStandardCarImpl(Collections.<String,Object>singletonMap("name", "Arthur"), model);

        final Map<String, Number> statistics = testCar.getStatistics();
        assertEquals("incorrect number of statistics", 1, statistics.size());
        assertEquals("incorrect statistic value", 42, statistics.get("meaningOfLife"));
    }


    public static int getMeaningOfLife(TestCar<?> car)
    {
        return 42;
    }
}
