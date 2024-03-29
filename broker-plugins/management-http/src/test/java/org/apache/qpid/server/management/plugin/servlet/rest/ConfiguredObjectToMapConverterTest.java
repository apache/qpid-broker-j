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
package org.apache.qpid.server.management.plugin.servlet.rest;

import static org.apache.qpid.server.management.plugin.servlet.rest.ConfiguredObjectToMapConverter.STATISTICS_MAP_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectMethodAttribute;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestElecEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestEngine;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectToMapConverterTest extends UnitTestBase
{
    private static final String TEST_SYSTEM_PROPERTY1_NAME = "qpid.test.name";
    private static final String TEST_SYSTEM_PROPERTY1_ACTUAL_VALUE = "context-test";
    private static final String TEST_SYSTEM_PROPERTY2_NAME = "qpid.test.name2";
    private static final String TEST_SYSTEM_PROPERTY2_ACTUAL_VALUE = "${" + TEST_SYSTEM_PROPERTY1_NAME + "}-value2";
    private static final String TEST_SYSTEM_PROPERTY2_EFFECTIVE_VALUE = TEST_SYSTEM_PROPERTY1_ACTUAL_VALUE + "-value2";
    private static final String PARENT_CONTEXT_PROPERTY1_NAME = "parentTest";
    private static final String PARENT_CONTEXT_PROPERTY1_ACTUAL_VALUE = "carTestValue";
    private static final String PARENT_CONTEXT_PROPERTY2_NAME = "parentTestExpression";
    private static final String PARENT_CONTEXT_PROPERTY2_ACTUAL_VALUE =
            "${" + PARENT_CONTEXT_PROPERTY1_NAME + "}-${" + TEST_SYSTEM_PROPERTY2_NAME + "}";
    private static final String PARENT_CONTEXT_PROPERTY2_EFFECTIVE_VALUE =
            PARENT_CONTEXT_PROPERTY1_ACTUAL_VALUE + "-" + TEST_SYSTEM_PROPERTY2_EFFECTIVE_VALUE;
    private static final String CHILD_CONTEXT_PROPERTY_NAME = "test";
    private static final String CHILD_CONTEXT_PROPERTY_ACTUAL_VALUE =
            "test-${" + PARENT_CONTEXT_PROPERTY2_NAME + "}";
    private static final String CHILD_CONTEXT_PROPERTY_EFFECTIVE_VALUE =
            "test-" + PARENT_CONTEXT_PROPERTY2_EFFECTIVE_VALUE;
    private ConfiguredObjectToMapConverter _converter;
    private ConfiguredObject _configuredObject;

    @BeforeEach
    public void setUp() throws Exception
    {
        _converter = new ConfiguredObjectToMapConverter();
        _configuredObject = mock(ConfiguredObject.class);
    }

    @Test
    public void testConfiguredObjectWithSingleStatistics()
    {
        final String statisticName = "statisticName";
        final int statisticValue = 10;

        when(_configuredObject.getStatistics()).thenReturn(Map.of(statisticName, (Number) statisticValue));

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              0,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        Map<String, Object> statsAsMap = (Map<String, Object>) resultMap.get(STATISTICS_MAP_KEY);
        assertNotNull(statsAsMap, "Statistics should be part of map");
        assertEquals(1, (long) statsAsMap.size(), "Unexpected number of statistics");
        assertEquals(statisticValue, statsAsMap.get(statisticName), "Unexpected statistic value");
    }

    @Test
    public void testConfiguredObjectWithSingleNonConfiguredObjectAttribute()
    {
        final String attributeName = "attribute";
        final String attributeValue = "value";
        Model model = createTestModel();
        when(_configuredObject.getModel()).thenReturn(model);
        configureMockToReturnOneAttribute(_configuredObject, attributeName, attributeValue);

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              0,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals((long) 1, (long) resultMap.size(), "Unexpected number of attributes");
        assertEquals(attributeValue, resultMap.get(attributeName), "Unexpected attribute value");
    }

    /*
     * For now, it is the name of the configured object is returned as the attribute value, rather than the
     * configured object itself
     */
    @Test
    public void testConfiguredObjectWithSingleConfiguredObjectAttribute()
    {
        final String attributeName = "attribute";
        final ConfiguredObject attributeValue = mock(ConfiguredObject.class);
        when(attributeValue.getName()).thenReturn("attributeConfiguredObjectName");

        configureMockToReturnOneAttribute(_configuredObject, attributeName, attributeValue);

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              0,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals((long) 1, (long) resultMap.size(), "Unexpected number of attributes");
        assertEquals("attributeConfiguredObjectName", resultMap.get(attributeName), "Unexpected attribute value");

    }

    @Test
    public void testConfiguredObjectWithChildAndDepth1()
    {
        final String childAttributeName = "childattribute";
        final String childAttributeValue = "childvalue";

        Model model = createTestModel();

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);
        configureMockToReturnOneAttribute(mockChild, childAttributeName, childAttributeValue);
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(List.of(mockChild));

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals((long) 1, (long) resultMap.size(), "Unexpected parent map size");

        final List<Map<String, Object>> childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals((long) 1, (long) childList.size(), "Unexpected number of children");
        final Map<String, Object> childMap = childList.get(0);
        assertEquals((long) 1, (long) childMap.size(), "Unexpected child map size");
        assertNotNull(childMap);

        assertEquals(childAttributeValue, childMap.get(childAttributeName), "Unexpected child attribute value");
    }

    @Test
    public void testActuals()
    {
        final String childAttributeName = "childattribute";
        final String childAttributeValue = "childvalue";
        final String childActualAttributeValue = "${actualvalue}";
        final Map<String,Object> actualContext = Map.of("key", "value");
        final Set<String> inheritedKeys = new HashSet<>(List.of("key","inheritedkey"));

        Model model = createTestModel();

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);
        when(_configuredObject.getAttributeNames()).thenReturn(List.of(ConfiguredObject.CONTEXT));
        when(_configuredObject.getContextValue(eq(String.class), eq("key"))).thenReturn("value");
        when(_configuredObject.getContextValue(eq(String.class),eq("inheritedkey"))).thenReturn("foo");
        when(_configuredObject.getContextKeys(anyBoolean())).thenReturn(inheritedKeys);
        when(_configuredObject.getContext()).thenReturn(actualContext);
        when(_configuredObject.getActualAttributes()).thenReturn(Map.of(ConfiguredObject.CONTEXT, actualContext));
        when(mockChild.getAttributeNames()).thenReturn(List.of(childAttributeName, ConfiguredObject.CONTEXT));
        when(mockChild.getAttribute(childAttributeName)).thenReturn(childAttributeValue);
        when(mockChild.getActualAttributes()).thenReturn(Map.of(childAttributeName, childActualAttributeValue));
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(List.of(mockChild));


        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              true,
                                                                              120,
                                                                              false,
                                                                              true));
        assertEquals((long) 2, (long) resultMap.size(), "Unexpected parent map size");
        assertEquals(resultMap.get(ConfiguredObject.CONTEXT), actualContext, "Incorrect context");
        List<Map<String, Object>> childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals((long) 1, (long) childList.size(), "Unexpected number of children");
        Map<String, Object> childMap = childList.get(0);
        assertNotNull(childMap);
        assertEquals((long) 1, (long) childMap.size(), "Unexpected child map size");

        assertEquals(childActualAttributeValue, childMap.get(childAttributeName), "Unexpected child attribute value");

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      120,
                                                                                                      false,
                                                                                                      true));
        assertEquals((long) 2, (long) resultMap.size(), "Unexpected parent map size");
        Map<String, Object> effectiveContext = new HashMap<>();
        effectiveContext.put("key","value");
        assertEquals(effectiveContext, resultMap.get(ConfiguredObject.CONTEXT), "Incorrect context");
        childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals((long) 1, (long) childList.size(), "Unexpected number of children");
        childMap = childList.get(0);
        assertEquals((long) 1, (long) childMap.size(), "Unexpected child map size");
        assertNotNull(childMap);

        assertEquals(childAttributeValue, childMap.get(childAttributeName), "Unexpected child attribute value");
    }

    @Test
    public void testOversizedAttributes()
    {

        Model model = createTestModel();
        ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        final Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes =
                typeRegistry.getAttributeTypes(TestChild.class);
        final ConfiguredObjectAttribute longAttr = mock(ConfiguredObjectMethodAttribute.class);
        when(longAttr.isOversized()).thenReturn(true);
        when(longAttr.getOversizedAltText()).thenReturn("");
        when(attributeTypes.get(eq("longAttr"))).thenReturn(longAttr);

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);
        configureMockToReturnOneAttribute(mockChild, "longAttr", "this is not long");
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(List.of(mockChild));


         Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                       ConfiguredObject.class,
                                                                       new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                               1,
                                                                               false,
                                                                               20,
                                                                               false,
                                                                               false));
        Object children = resultMap.get("testchilds");
        assertNotNull(children);
        final boolean condition5 = children instanceof Collection;
        assertTrue(condition5);
        assertTrue(((Collection)children).size() == 1);
        Object attrs = ((Collection)children).iterator().next();
        final boolean condition4 = attrs instanceof Map;
        assertTrue(condition4);
        assertEquals("this is not long", ((Map) attrs).get("longAttr"));

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      8,
                                                                                                      false,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        final boolean condition3 = children instanceof Collection;
        assertTrue(condition3);
        assertTrue(((Collection)children).size() == 1);
        attrs = ((Collection)children).iterator().next();
        final boolean condition2 = attrs instanceof Map;
        assertTrue(condition2);
        assertEquals("this...", ((Map) attrs).get("longAttr"));

        when(longAttr.getOversizedAltText()).thenReturn("test alt text");

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      8,
                                                                                                      false,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        final boolean condition1 = children instanceof Collection;
        assertTrue(condition1);
        assertTrue(((Collection)children).size() == 1);
        attrs = ((Collection)children).iterator().next();
        final boolean condition = attrs instanceof Map;
        assertTrue(condition);
        assertEquals("test alt text", ((Map) attrs).get("longAttr"));
    }

    @Test
    public void testSecureAttributes()
    {

        Model model = createTestModel();
        ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes = typeRegistry.getAttributeTypes(TestChild.class);
        ConfiguredObjectAttribute secureAttribute = mock(ConfiguredObjectMethodAttribute.class);
        when(secureAttribute.isSecure()).thenReturn(true);
        when(secureAttribute.isSecureValue(any())).thenReturn(true);
        when(attributeTypes.get(eq("secureAttribute"))).thenReturn(secureAttribute);

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);

        // set encoded value
        configureMockToReturnOneAttribute(mockChild, "secureAttribute", "*****");

        // set actual values
        when(mockChild.getActualAttributes()).thenReturn(Map.of("secureAttribute", "secret"));
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(List.of(mockChild));
        when(model.getParentType(TestChild.class)).thenReturn((Class)TestChild.class);
        when(_configuredObject.getCategoryClass()).thenReturn(TestChild.class);
        when(mockChild.isDurable()).thenReturn(true);

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              20,
                                                                              false,
                                                                              false));
        Object children = resultMap.get("testchilds");
        assertNotNull(children);
        final boolean condition3 = children instanceof Collection;
        assertTrue(condition3);
        assertTrue(((Collection)children).size() == 1);
        Object attrs = ((Collection)children).iterator().next();
        final boolean condition2 = attrs instanceof Map;
        assertTrue(condition2);
        assertEquals("*****", ((Map) attrs).get("secureAttribute"));

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      true,
                                                                                                      20,
                                                                                                      true,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        final boolean condition1 = children instanceof Collection;
        assertTrue(condition1);
        assertTrue(((Collection)children).size() == 1);
        attrs = ((Collection)children).iterator().next();
        final boolean condition = attrs instanceof Map;
        assertTrue(condition);
        assertEquals("*****", ((Map) attrs).get("secureAttribute"));
    }

    @Test
    public void testIncludeInheritedContextAndEffective()
    {
        TestEngine engine = createEngineWithContext();
        ConfiguredObjectToMapConverter.ConverterOptions options = new ConfiguredObjectToMapConverter.ConverterOptions(
                1,
                false,
                0,
                false,
                false);
        Map<String, Object> resultMap = _converter.convertObjectToMap(engine, TestEngine.class, options);
        Map<String, String> context = getContext(resultMap);

        assertTrue(context.size() >= 5, "Unexpected size of context");
        assertEquals(CHILD_CONTEXT_PROPERTY_EFFECTIVE_VALUE, context.get(CHILD_CONTEXT_PROPERTY_NAME),
                "Unexpected engine context content");
        assertEquals(PARENT_CONTEXT_PROPERTY1_ACTUAL_VALUE, context.get(PARENT_CONTEXT_PROPERTY1_NAME),
                "Unexpected car context content");
        assertEquals(PARENT_CONTEXT_PROPERTY2_EFFECTIVE_VALUE, context.get(PARENT_CONTEXT_PROPERTY2_NAME),
                "Unexpected car context content");
        assertEquals(TEST_SYSTEM_PROPERTY1_ACTUAL_VALUE, context.get(TEST_SYSTEM_PROPERTY1_NAME),
                "Unexpected system context content");
        assertEquals(TEST_SYSTEM_PROPERTY2_EFFECTIVE_VALUE, context.get(TEST_SYSTEM_PROPERTY2_NAME),
                "Unexpected system context content");
    }

    @Test
    public void testIncludeInheritedContextAndActual()
    {
        TestEngine engine = createEngineWithContext();
        ConfiguredObjectToMapConverter.ConverterOptions options = new ConfiguredObjectToMapConverter.ConverterOptions(
                1,
                true,
                0,
                false,
                false);
        Map<String, Object> resultMap = _converter.convertObjectToMap(engine, TestEngine.class, options);
        Map<String, String> context = getContext(resultMap);
        assertTrue(context.size() >= 5, "Unexpected size of context");
        assertEquals(CHILD_CONTEXT_PROPERTY_ACTUAL_VALUE, context.get(CHILD_CONTEXT_PROPERTY_NAME),
                "Unexpected engine context content");
        assertEquals(PARENT_CONTEXT_PROPERTY1_ACTUAL_VALUE, context.get(PARENT_CONTEXT_PROPERTY1_NAME),
                "Unexpected car context content");
        assertEquals(PARENT_CONTEXT_PROPERTY2_ACTUAL_VALUE, context.get(PARENT_CONTEXT_PROPERTY2_NAME),
                "Unexpected car context content");
        assertEquals(TEST_SYSTEM_PROPERTY1_ACTUAL_VALUE, context.get(TEST_SYSTEM_PROPERTY1_NAME),
                "Unexpected system context content");
        assertEquals(TEST_SYSTEM_PROPERTY2_ACTUAL_VALUE, context.get(TEST_SYSTEM_PROPERTY2_NAME),
                "Unexpected system context content");
    }

    @Test
    public void testExcludeInheritedContextAndEffective()
    {
        TestEngine engine = createEngineWithContext();
        ConfiguredObjectToMapConverter.ConverterOptions options = new ConfiguredObjectToMapConverter.ConverterOptions(
                1,
                false,
                0,
                false,
                true);
        Map<String, Object> resultMap = _converter.convertObjectToMap(engine, TestEngine.class, options);
        Map<String, String> context = getContext(resultMap);
        assertEquals((long) 1, (long) context.size(), "Unexpected size of context");
        assertEquals(CHILD_CONTEXT_PROPERTY_EFFECTIVE_VALUE, context.get(CHILD_CONTEXT_PROPERTY_NAME),
                "Unexpected context content");
    }

    @Test
    public void testExcludeInheritedContextAndActual()
    {
        TestEngine engine = createEngineWithContext();
        ConfiguredObjectToMapConverter.ConverterOptions options = new ConfiguredObjectToMapConverter.ConverterOptions(
                1,
                true,
                0,
                false,
                true);
        Map<String, Object> resultMap = _converter.convertObjectToMap(engine, TestEngine.class, options);
        Map<String, String> context = getContext(resultMap);
        assertEquals((long) 1, (long) context.size(), "Unexpected size of context");
        assertEquals(CHILD_CONTEXT_PROPERTY_ACTUAL_VALUE, context.get(CHILD_CONTEXT_PROPERTY_NAME),
                "Unexpected context content");
    }

    private Map<String, String> getContext(final Map<String, Object> resultMap)
    {
        Object contextValue = resultMap.get(ConfiguredObject.CONTEXT);
        final boolean condition = contextValue instanceof Map;
        assertTrue(condition, "Unexpected type of context");
        Map<String, String> context = (Map<String, String>) contextValue;
        return context;
    }

    private TestEngine createEngineWithContext()
    {
        setTestSystemProperty(TEST_SYSTEM_PROPERTY1_NAME, TEST_SYSTEM_PROPERTY1_ACTUAL_VALUE);
        setTestSystemProperty(TEST_SYSTEM_PROPERTY2_NAME, TEST_SYSTEM_PROPERTY2_ACTUAL_VALUE);
        Model model = org.apache.qpid.server.model.testmodels.hierarchy.TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, "myCar");
        Map<String, String> carContext = new HashMap<>();
        carContext.put(PARENT_CONTEXT_PROPERTY1_NAME, PARENT_CONTEXT_PROPERTY1_ACTUAL_VALUE);
        carContext.put(PARENT_CONTEXT_PROPERTY2_NAME, PARENT_CONTEXT_PROPERTY2_ACTUAL_VALUE);
        carAttributes.put(ConfiguredObject.CONTEXT, carContext);
        TestCar car = model.getObjectFactory().create(TestCar.class, carAttributes, null);
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        Map<String, String> engineContext = new HashMap<>();
        engineContext.put(CHILD_CONTEXT_PROPERTY_NAME, CHILD_CONTEXT_PROPERTY_ACTUAL_VALUE);
        engineAttributes.put(ConfiguredObject.CONTEXT, engineContext);
        return (TestEngine) car.createChild(TestEngine.class, engineAttributes);
    }

    private Model createTestModel()
    {
        Model model = mock(Model.class);
        final List<Class<? extends ConfiguredObject>> list = new ArrayList<>();
        list.add(TestChild.class);
        when(model.getChildTypes(ConfiguredObject.class)).thenReturn(list);
        final ConfiguredObjectTypeRegistry typeRegistry = mock(ConfiguredObjectTypeRegistry.class);
        final Map<String, ConfiguredObjectAttribute<?, ?>> attrTypes = mock(Map.class);
        when(attrTypes.get(any(String.class))).thenReturn(mock(ConfiguredObjectMethodAttribute.class));
        when(typeRegistry.getAttributeTypes(any(Class.class))).thenReturn(attrTypes);
        when(model.getTypeRegistry()).thenReturn(typeRegistry);
        return model;
    }

    private void configureMockToReturnOneAttribute(ConfiguredObject mockConfiguredObject, String attributeName, Object attributeValue)
    {
        when(mockConfiguredObject.getAttributeNames()).thenReturn(List.of(attributeName));
        when(mockConfiguredObject.getAttribute(attributeName)).thenReturn(attributeValue);
    }

    private static interface TestChild extends ConfiguredObject
    {
    }
}
