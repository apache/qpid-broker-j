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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectMethodAttribute;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestElecEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestEngine;
import org.apache.qpid.test.utils.QpidTestCase;

public class ConfiguredObjectToMapConverterTest extends QpidTestCase
{
    private ConfiguredObjectToMapConverter _converter = new ConfiguredObjectToMapConverter();
    private ConfiguredObject _configuredObject = mock(ConfiguredObject.class);

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public void testConfiguredObjectWithSingleStatistics() throws Exception
    {
        final String statisticName = "statisticName";
        final int statisticValue = 10;

        when(_configuredObject.getStatistics()).thenReturn(Collections.singletonMap(statisticName, (Number) statisticValue));

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              0,
                                                                              false,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        Map<String, Object> statsAsMap = (Map<String, Object>) resultMap.get(STATISTICS_MAP_KEY);
        assertNotNull("Statistics should be part of map", statsAsMap);
        assertEquals("Unexpected number of statistics", 1, statsAsMap.size());
        assertEquals("Unexpected statistic value", statisticValue, statsAsMap.get(statisticName));
    }

    public void testConfiguredObjectWithSingleNonConfiguredObjectAttribute() throws Exception
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
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals("Unexpected number of attributes", 1, resultMap.size());
        assertEquals("Unexpected attribute value", attributeValue, resultMap.get(attributeName));
    }

    /*
     * For now, it is the name of the configured object is returned as the attribute value, rather than the
     * configured object itself
     */
    public void testConfiguredObjectWithSingleConfiguredObjectAttribute() throws Exception
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
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals("Unexpected number of attributes", 1, resultMap.size());
        assertEquals("Unexpected attribute value", "attributeConfiguredObjectName", resultMap.get(attributeName));
    }

    public void testConfiguredObjectWithChildAndDepth1()
    {
        final String childAttributeName = "childattribute";
        final String childAttributeValue = "childvalue";

        Model model = createTestModel();

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);
        configureMockToReturnOneAttribute(mockChild, childAttributeName, childAttributeValue);
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(Arrays.asList(mockChild));

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              false));
        assertEquals("Unexpected parent map size", 1, resultMap.size());

        final List<Map<String, Object>> childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals("Unexpected number of children", 1, childList.size());
        final Map<String, Object> childMap = childList.get(0);
        assertEquals("Unexpected child map size", 1, childMap.size());
        assertNotNull(childMap);

        assertEquals("Unexpected child attribute value", childAttributeValue, childMap.get(childAttributeName));
    }

    public void testActuals()
    {
        final String childAttributeName = "childattribute";
        final String childAttributeValue = "childvalue";
        final String childActualAttributeValue = "${actualvalue}";
        final Map<String,Object> actualContext = Collections.<String,Object>singletonMap("key","value");
        final Set<String> inheritedKeys = new HashSet<>(Arrays.asList("key","inheritedkey"));

        Model model = createTestModel();

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(_configuredObject.getModel()).thenReturn(model);
        when(_configuredObject.getAttributeNames()).thenReturn(Collections.singletonList(ConfiguredObject.CONTEXT));
        when(_configuredObject.getContextValue(eq(String.class), eq("key"))).thenReturn("value");
        when(_configuredObject.getContextValue(eq(String.class),eq("inheritedkey"))).thenReturn("foo");
        when(_configuredObject.getContextKeys(anyBoolean())).thenReturn(inheritedKeys);
        when(_configuredObject.getContext()).thenReturn(actualContext);
        when(_configuredObject.getActualAttributes()).thenReturn(Collections.singletonMap(ConfiguredObject.CONTEXT, actualContext));
        when(mockChild.getAttributeNames()).thenReturn(Arrays.asList(childAttributeName, ConfiguredObject.CONTEXT));
        when(mockChild.getAttribute(childAttributeName)).thenReturn(childAttributeValue);
        when(mockChild.getActualAttributes()).thenReturn(Collections.singletonMap(childAttributeName, childActualAttributeValue));
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(Arrays.asList(mockChild));


        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              true,
                                                                              false,
                                                                              120,
                                                                              false,
                                                                              true));
        assertEquals("Unexpected parent map size", 2, resultMap.size());
        assertEquals("Incorrect context", resultMap.get(ConfiguredObject.CONTEXT), actualContext);
        List<Map<String, Object>> childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals("Unexpected number of children", 1, childList.size());
        Map<String, Object> childMap = childList.get(0);
        assertNotNull(childMap);
        assertEquals("Unexpected child map size", 1, childMap.size());

        assertEquals("Unexpected child attribute value", childActualAttributeValue, childMap.get(childAttributeName));

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      false,
                                                                                                      120,
                                                                                                      false,
                                                                                                      true));
        assertEquals("Unexpected parent map size", 2, resultMap.size());
        Map<String, Object> effectiveContext = new HashMap<>();
        effectiveContext.put("key","value");
        assertEquals("Incorrect context", effectiveContext, resultMap.get(ConfiguredObject.CONTEXT));
        childList = (List<Map<String, Object>>) resultMap.get("testchilds");
        assertEquals("Unexpected number of children", 1, childList.size());
        childMap = childList.get(0);
        assertEquals("Unexpected child map size", 1, childMap.size());
        assertNotNull(childMap);

        assertEquals("Unexpected child attribute value", childAttributeValue, childMap.get(childAttributeName));

    }

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
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(Arrays.asList(mockChild));


         Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                       ConfiguredObject.class,
                                                                       new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                               1,
                                                                               false,
                                                                               false,
                                                                               20,
                                                                               false,
                                                                               false));
        Object children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        Object attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("this is not long", ((Map) attrs).get("longAttr"));



        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      false,
                                                                                                      8,
                                                                                                      false,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("this...", ((Map) attrs).get("longAttr"));




        when(longAttr.getOversizedAltText()).thenReturn("test alt text");

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      false,
                                                                                                      8,
                                                                                                      false,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("test alt text", ((Map) attrs).get("longAttr"));


    }

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
        when(mockChild.getActualAttributes()).thenReturn(Collections.singletonMap("secureAttribute", "secret"));
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(Arrays.asList(mockChild));
        when(model.getParentTypes(TestChild.class)).thenReturn(Collections.<Class<? extends ConfiguredObject>>singleton(TestChild.class));
        when(_configuredObject.getCategoryClass()).thenReturn(TestChild.class);
        when(mockChild.isDurable()).thenReturn(true);

        Map<String, Object> resultMap = _converter.convertObjectToMap(_configuredObject,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              false,
                                                                              20,
                                                                              false,
                                                                              false));
        Object children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        Object attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("*****", ((Map) attrs).get("secureAttribute"));

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      true,
                                                                                                      true,
                                                                                                      20,
                                                                                                      true,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("secret", ((Map) attrs).get("secureAttribute"));

        resultMap = _converter.convertObjectToMap(_configuredObject,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      true,
                                                                                                      false,
                                                                                                      20,
                                                                                                      true,
                                                                                                      false));

        children = resultMap.get("testchilds");
        assertNotNull(children);
        assertTrue(children instanceof Collection);
        assertTrue(((Collection)children).size()==1);
        attrs = ((Collection)children).iterator().next();
        assertTrue(attrs instanceof Map);
        assertEquals("*****", ((Map) attrs).get("secureAttribute"));
    }

    public void testExcludeInheritedContext()
    {
        Model model = org.apache.qpid.server.model.testmodels.hierarchy.TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, "myCar");
        carAttributes.put(ConfiguredObject.CONTEXT, Collections.singletonMap("parentTest", "parentTestValue"));
        TestCar car = model.getObjectFactory().create(TestCar.class,
                                                      carAttributes);
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        engineAttributes.put(ConfiguredObject.CONTEXT, Collections.singletonMap("test", "testValue"));
        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        Map<String, Object> resultMap = _converter.convertObjectToMap(engine,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              false,
                                                                              0,
                                                                              false,
                                                                              false));
        Object contextValue = resultMap.get("context");
        assertTrue("Unexpected type of context", contextValue instanceof Map);
        assertTrue("Unexpected size of context", ((Map)contextValue).size() > 1);
        assertEquals("Unexpected context content", "testValue", ((Map)contextValue).get("test"));
        assertEquals("Unexpected context content", "parentTestValue", ((Map)contextValue).get("parentTest"));

        resultMap = _converter.convertObjectToMap(engine,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      false,
                                                                                                      0,
                                                                                                      false,
                                                                                                      true));
        contextValue = resultMap.get("context");
        assertTrue("Unexpected type of context", contextValue instanceof Map);
        assertEquals("Unexpected size of context", 1, ((Map)contextValue).size());
        assertEquals("Unexpected context content","testValue", ((Map)contextValue).get("test"));





        /*



//        Model model = createTestModel();
        ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes = typeRegistry.getAttributeTypes(TestChild.class);
        ConfiguredObjectAttribute contextAttribute = mock(ConfiguredObjectMethodAttribute.class);
        when(attributeTypes.get(eq("context"))).thenReturn(contextAttribute);

        TestChild mockChild = mock(TestChild.class);
        when(mockChild.getModel()).thenReturn(model);
        when(mockChild.getCategoryClass()).thenReturn(TestChild.class);
        when(mockChild.getParent(Matchers.<Class>any())).thenReturn(_configuredObject);
        when(_configuredObject.getModel()).thenReturn(model);
        when(_configuredObject.getChildren(TestChild.class)).thenReturn(Arrays.asList(mockChild));
        when(model.getParentTypes(TestChild.class)).thenReturn(Collections.<Class<? extends ConfiguredObject>>singleton(TestChild.class));
        when(_configuredObject.getCategoryClass()).thenReturn(TestChild.class);


        // set encoded value
        final Map<String,String> context = new HashMap<>();
        context.put("test", "testValue");
        configureMockToReturnOneAttribute(mockChild, "context", context);
        when(mockChild.getContextKeys(anyBoolean())).thenReturn(context.keySet());
        when(mockChild.getContextValue(String.class, "test")).thenReturn(context.get("test"));

        final Map<String, String> parentContext = new HashMap<>();
        parentContext.put("parentTest", "parentTestValue");
        configureMockToReturnOneAttribute(_configuredObject, "context", parentContext);
        when(_configuredObject.getContextKeys(anyBoolean())).thenReturn(parentContext.keySet());
        when(_configuredObject.getContextValue(String.class, "parentTest")).thenReturn(parentContext.get("parentTest"));

        Map<String, Object> resultMap = _converter.convertObjectToMap(mockChild,
                                                                      ConfiguredObject.class,
                                                                      new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                              1,
                                                                              false,
                                                                              false,
                                                                              0,
                                                                              false,
                                                                              false));
        Object contextValue = resultMap.get("context");
        assertTrue("Unexpected type of context", contextValue instanceof Map);
        assertEquals("Unexpected size of context", 2, ((Map)contextValue).size());
        assertEquals("Unexpected context content", "testValue", ((Map)contextValue).get("test"));
        assertEquals("Unexpected context content", "parentTestValue", ((Map)contextValue).get("parentTest"));

        resultMap = _converter.convertObjectToMap(mockChild,
                                                  ConfiguredObject.class,
                                                  new ConfiguredObjectToMapConverter.ConverterOptions(1,
                                                                                                      false,
                                                                                                      false,
                                                                                                      0,
                                                                                                      false,
                                                                                                      true));
        contextValue = resultMap.get("context");
        assertTrue("Unexpected type of context", contextValue instanceof Map);
        assertEquals("Unexpected size of context", 1, ((Map)contextValue).size());
        assertEquals("Unexpected context content","testValue", ((Map)contextValue).get("test"));*/
    }

    private Model createTestModel()
    {
        Model model = mock(Model.class);
        final List<Class<? extends ConfiguredObject>> list = new ArrayList<Class<? extends ConfiguredObject>>();
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
        when(mockConfiguredObject.getAttributeNames()).thenReturn(Arrays.asList(attributeName));
        when(mockConfiguredObject.getAttribute(attributeName)).thenReturn(attributeValue);
    }

    private static interface TestChild extends ConfiguredObject
    {
    }
}
