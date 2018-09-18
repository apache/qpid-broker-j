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
 */
package org.apache.qpid.server.model.testmodels.singleton;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Tests behaviour of AbstractConfiguredObject related to attributes including
 * persistence, defaulting, and attribute values derived from context variables.
 */
public class AbstractConfiguredObjectTest extends UnitTestBase
{
    private final Model _model = TestModel.getInstance();

    @Test
    public void testAttributePersistence()
    {
        final String objectName = "testNonPersistAttributes";
        TestSingleton object =
                _model.getObjectFactory().create(TestSingleton.class,
                                                Collections.<String, Object>singletonMap(ConfiguredObject.NAME,
                                                                                         objectName),
                                               null);

        assertEquals(objectName, object.getName());
        assertNull(object.getAutomatedNonPersistedValue());
        assertNull(object.getAutomatedPersistedValue());
        assertEquals((long) TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());

        ConfiguredObjectRecord record = object.asObjectRecord();

        assertEquals(objectName, record.getAttributes().get(ConfiguredObject.NAME));

        assertFalse(record.getAttributes().containsKey(TestSingleton.AUTOMATED_PERSISTED_VALUE));
        assertFalse(record.getAttributes().containsKey(TestSingleton.AUTOMATED_NONPERSISTED_VALUE));
        assertFalse(record.getAttributes().containsKey(TestSingleton.DERIVED_VALUE));

        Map<String, Object> updatedAttributes = new HashMap<>();

        final String newValue = "newValue";

        updatedAttributes.put(TestSingleton.AUTOMATED_PERSISTED_VALUE, newValue);
        updatedAttributes.put(TestSingleton.AUTOMATED_NONPERSISTED_VALUE, newValue);
        updatedAttributes.put(TestSingleton.DERIVED_VALUE, System.currentTimeMillis());  // Will be ignored
        object.setAttributes(updatedAttributes);

        assertEquals(newValue, object.getAutomatedPersistedValue());
        assertEquals(newValue, object.getAutomatedNonPersistedValue());

        record = object.asObjectRecord();
        assertEquals(objectName, record.getAttributes().get(ConfiguredObject.NAME));
        assertEquals(newValue, record.getAttributes().get(TestSingleton.AUTOMATED_PERSISTED_VALUE));

        assertFalse(record.getAttributes().containsKey(TestSingleton.AUTOMATED_NONPERSISTED_VALUE));
        assertFalse(record.getAttributes().containsKey(TestSingleton.DERIVED_VALUE));
    }

    @Test
    public void testDefaultedAttributeValue()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = Collections.<String, Object>singletonMap(TestSingleton.NAME, objectName);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestSingleton.DEFAULTED_VALUE_DEFAULT, object1.getDefaultedValue());
    }

    @Test
    public void testOverriddenDefaultedAttributeValue()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.DEFAULTED_VALUE, "override");

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals("override", object.getDefaultedValue());
    }

    @Test
    public void testOverriddenDefaultedAttributeValueRevertedToDefault()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.DEFAULTED_VALUE, "override");

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals("override", object.getDefaultedValue());

        object.setAttributes(Collections.singletonMap(TestSingleton.DEFAULTED_VALUE, null));

        assertEquals(TestSingleton.DEFAULTED_VALUE_DEFAULT, object.getDefaultedValue());
    }

    @Test
    public void testDefaultInitialization()
    {
        TestSingleton object =
                _model.getObjectFactory().create(TestSingleton.class,
                                                 Collections.<String, Object>singletonMap(ConfiguredObject.NAME,
                                                                                          "testDefaultInitialization"), null);
        assertEquals(object.getAttrWithDefaultFromContextNoInit(), TestSingleton.testGlobalDefault);
        assertEquals(object.getAttrWithDefaultFromContextCopyInit(), TestSingleton.testGlobalDefault);
        assertEquals(object.getAttrWithDefaultFromContextMaterializeInit(), TestSingleton.testGlobalDefault);

        assertFalse(object.getActualAttributes().containsKey("attrWithDefaultFromContextNoInit"));
        assertEquals("${" + TestSingleton.TEST_CONTEXT_DEFAULT + "}",
                            object.getActualAttributes().get("attrWithDefaultFromContextCopyInit"));
        assertEquals(TestSingleton.testGlobalDefault,
                            object.getActualAttributes().get("attrWithDefaultFromContextMaterializeInit"));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "testDefaultInitialization2");
        attributes.put(ConfiguredObject.CONTEXT, Collections.singletonMap(TestSingleton.TEST_CONTEXT_DEFAULT, "foo"));
        object = _model.getObjectFactory().create(TestSingleton.class,
                                         attributes, null);
        assertEquals("foo", object.getAttrWithDefaultFromContextNoInit());
        assertEquals("foo", object.getAttrWithDefaultFromContextCopyInit());
        assertEquals("foo", object.getAttrWithDefaultFromContextMaterializeInit());

        assertFalse(object.getActualAttributes().containsKey("attrWithDefaultFromContextNoInit"));
        assertEquals("${" + TestSingleton.TEST_CONTEXT_DEFAULT + "}",
                            object.getActualAttributes().get("attrWithDefaultFromContextCopyInit"));
        assertEquals("foo", object.getActualAttributes().get("attrWithDefaultFromContextMaterializeInit"));

        setTestSystemProperty(TestSingleton.TEST_CONTEXT_DEFAULT, "bar");
        object = _model.getObjectFactory().create(TestSingleton.class,
                                        Collections.<String, Object>singletonMap(ConfiguredObject.NAME,
                                                                                 "testDefaultInitialization3"), null);

        assertEquals("bar", object.getAttrWithDefaultFromContextNoInit());
        assertEquals("bar", object.getAttrWithDefaultFromContextCopyInit());
        assertEquals("bar", object.getAttrWithDefaultFromContextMaterializeInit());

        assertFalse(object.getActualAttributes().containsKey("attrWithDefaultFromContextNoInit"));
        assertEquals("${" + TestSingleton.TEST_CONTEXT_DEFAULT + "}",
                            object.getActualAttributes().get("attrWithDefaultFromContextCopyInit"));
        assertEquals("bar", object.getActualAttributes().get("attrWithDefaultFromContextMaterializeInit"));
    }

    @Test
    public void testEnumAttributeValueFromString()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.ENUM_VALUE, TestEnum.TEST_ENUM1.name());

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestEnum.TEST_ENUM1, object1.getEnumValue());
    }

    @Test
    public void testEnumAttributeValueFromEnum()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.ENUM_VALUE, TestEnum.TEST_ENUM1);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestEnum.TEST_ENUM1, object1.getEnumValue());
    }

    @Test
    public void testIntegerAttributeValueFromString()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.INT_VALUE, "-4");

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals((long) -4, (long) object1.getIntValue());
    }

    @Test
    public void testIntegerAttributeValueFromInteger()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.INT_VALUE, 5);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals((long) 5, (long) object1.getIntValue());
    }

    @Test
    public void testIntegerAttributeValueFromDouble()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.INT_VALUE, 6.1);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals((long) 6, (long) object.getIntValue());
    }

    @Test
    public void testDateAttributeFromMillis()
    {
        final String objectName = "myName";
        long now = System.currentTimeMillis();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.DATE_VALUE, now);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals(new Date(now), object.getDateValue());
    }

    @Test
    public void testDateAttributeFromIso8601()
    {
        final String objectName = "myName";
        String date = "1970-01-01";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.DATE_VALUE, date);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals(new Date(0), object.getDateValue());
    }

    @Test
    public void testStringAttributeValueFromContextVariableProvidedBySystemProperty()
    {
        String sysPropertyName = "testStringAttributeValueFromContextVariableProvidedBySystemProperty";
        String contextToken = "${" + sysPropertyName + "}";

        System.setProperty(sysPropertyName, "myValue");

        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.STRING_VALUE, contextToken);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals("myValue", object1.getStringValue());

        // System property set empty string

        System.setProperty(sysPropertyName, "");
        TestSingleton object2 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals("", object2.getStringValue());

        // System property not set
        System.clearProperty(sysPropertyName);

        TestSingleton object3 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        // yields the unexpanded token - not sure if this is really useful behaviour?
        assertEquals(contextToken, object3.getStringValue());
    }

    @Test
    public void testMapAttributeValueFromContextVariableProvidedBySystemProperty()
    {
        String sysPropertyName = "testMapAttributeValueFromContextVariableProvidedBySystemProperty";
        String contextToken = "${" + sysPropertyName + "}";

        Map<String,String> expectedMap = new HashMap<>();
        expectedMap.put("field1", "value1");
        expectedMap.put("field2", "value2");

        System.setProperty(sysPropertyName, "{ \"field1\" : \"value1\", \"field2\" : \"value2\"}");

        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.MAP_VALUE, contextToken);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(expectedMap, object1.getMapValue());

        // System property not set
        System.clearProperty(sysPropertyName);
    }

    @Test
    public void testStringAttributeValueFromContextVariableProvidedObjectsContext()
    {
        String contextToken = "${myReplacement}";

        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);
        attributes.put(ConfiguredObject.CONTEXT, Collections.singletonMap("myReplacement", "myValue"));
        attributes.put(TestSingleton.STRING_VALUE, contextToken);

        TestSingleton object1 = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
        // Check the object's context itself
        assertTrue(object1.getContext().containsKey("myReplacement"));
        assertEquals("myValue", object1.getContext().get("myReplacement"));

        assertEquals(objectName, object1.getName());
        assertEquals("myValue", object1.getStringValue());
    }

    @Test
    public void testInvalidIntegerAttributeValueFromContextVariable()
    {
        final Map<String, Object> attributes = new HashMap<>();

        attributes.put(TestSingleton.NAME, "myName");
        attributes.put(TestSingleton.TYPE, TestSingletonImpl.TEST_SINGLETON_TYPE);
        attributes.put(TestSingleton.CONTEXT, Collections.singletonMap("contextVal", "notAnInteger"));
        attributes.put(TestSingleton.INT_VALUE, "${contextVal}");

        try
        {
            _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
            fail("creation of child object should have failed due to invalid value");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
            String message = e.getMessage();
            assertTrue("Message does not contain the attribute name", message.contains("intValue"));
            assertTrue("Message does not contain the non-interpolated value", message.contains("contextVal"));
            assertTrue("Message does not contain the interpolated value", message.contains("contextVal"));
        }
    }

    @Test
    public void testCreateEnforcesAttributeValidValues() throws Exception
    {
        final String objectName = getTestName();
        Map<String, Object> illegalCreateAttributes = new HashMap<>();
        illegalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        illegalCreateAttributes.put(TestSingleton.VALID_VALUE, "illegal");

        try
        {
            _model.getObjectFactory().create(TestSingleton.class, illegalCreateAttributes, null);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }

        Map<String, Object> legalCreateAttributes = new HashMap<>();
        legalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        legalCreateAttributes.put(TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE1);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals(TestSingleton.VALID_VALUE1, object.getValidValue());
    }

    @Test
    public void testCreateEnforcesAttributeValidValuePattern() throws Exception
    {
        final String objectName = getTestName();
        Map<String, Object> illegalCreateAttributes = new HashMap<>();
        illegalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        illegalCreateAttributes.put(TestSingleton.VALUE_WITH_PATTERN, "illegal");

        try
        {
            _model.getObjectFactory().create(TestSingleton.class, illegalCreateAttributes, null);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }

        illegalCreateAttributes = new HashMap<>();
        illegalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        illegalCreateAttributes.put(TestSingleton.LIST_VALUE_WITH_PATTERN, Arrays.asList("1.1.1.1", "1"));

        try
        {
            _model.getObjectFactory().create(TestSingleton.class, illegalCreateAttributes, null);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }


        Map<String, Object> legalCreateAttributes = new HashMap<>();
        legalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        legalCreateAttributes.put(TestSingleton.VALUE_WITH_PATTERN, "foozzzzzbar");
        legalCreateAttributes.put(TestSingleton.LIST_VALUE_WITH_PATTERN, Arrays.asList("1.1.1.1", "255.255.255.255"));

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals("foozzzzzbar", object.getValueWithPattern());
    }


    @Test
    public void testChangeEnforcesAttributeValidValues() throws Exception
    {
        final String objectName = getTestName();
        Map<String, Object> legalCreateAttributes = new HashMap<>();
        legalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        legalCreateAttributes.put(TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE1);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals(TestSingleton.VALID_VALUE1, object.getValidValue());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE2));
        assertEquals(TestSingleton.VALID_VALUE2, object.getValidValue());

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.VALID_VALUE, "illegal"));
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException iae)
        {
            // PASS
        }

        assertEquals(TestSingleton.VALID_VALUE2, object.getValidValue());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALID_VALUE, null));
        assertNull(object.getValidValue());
    }

    @Test
    public void testCreateEnforcesAttributeValidValuesWithSets() throws Exception
    {
        final String objectName = getTestName();
        final Map<String, Object> name = Collections.singletonMap(ConfiguredObject.NAME, (Object)objectName);

        Map<String, Object> illegalCreateAttributes = new HashMap<>(name);
        illegalCreateAttributes.put(TestSingleton.ENUMSET_VALUES, Collections.singleton(TestEnum.TEST_ENUM3));

        try
        {
            _model.getObjectFactory().create(TestSingleton.class, illegalCreateAttributes, null);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }

        {
            Map<String, Object> legalCreateAttributesEnums = new HashMap<>(name);
            legalCreateAttributesEnums.put(TestSingleton.ENUMSET_VALUES,
                                           Arrays.asList(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3));

            TestSingleton obj = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributesEnums, null);
            assertTrue(obj.getEnumSetValues().containsAll(Arrays.asList(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3)));
        }

        {
            Map<String, Object> legalCreateAttributesStrings = new HashMap<>(name);
            legalCreateAttributesStrings.put(TestSingleton.ENUMSET_VALUES,
                                             Arrays.asList(TestEnum.TEST_ENUM2.name(), TestEnum.TEST_ENUM3.name()));

            TestSingleton
                    obj = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributesStrings, null);
            assertTrue(obj.getEnumSetValues().containsAll(Arrays.asList(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3)));
        }
    }


    @Test
    public void testChangeEnforcesAttributeValidValuePatterns() throws Exception
    {
        final String objectName = getTestName();
        Map<String, Object> legalCreateAttributes = new HashMap<>();
        legalCreateAttributes.put(ConfiguredObject.NAME, objectName);
        legalCreateAttributes.put(TestSingleton.VALUE_WITH_PATTERN, "foozzzzzbar");
        legalCreateAttributes.put(TestSingleton.LIST_VALUE_WITH_PATTERN, Arrays.asList("1.1.1.1", "255.255.255.255"));

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals("foozzzzzbar", object.getValueWithPattern());
        assertEquals(Arrays.asList("1.1.1.1", "255.255.255.255"), object.getListValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALUE_WITH_PATTERN, "foobar"));
        assertEquals("foobar", object.getValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.LIST_VALUE_WITH_PATTERN, Collections.singletonList("1.2.3.4")));
        assertEquals(Collections.singletonList("1.2.3.4"), object.getListValueWithPattern());

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.VALUE_WITH_PATTERN, "foobaz"));
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException iae)
        {
            // PASS
        }


        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.LIST_VALUE_WITH_PATTERN, Arrays.asList("1.1.1.1", "1")));
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException iae)
        {
            // PASS
        }

        assertEquals("foobar", object.getValueWithPattern());
        assertEquals(Collections.singletonList("1.2.3.4"), object.getListValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALUE_WITH_PATTERN, null));
        assertNull(object.getValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.LIST_VALUE_WITH_PATTERN, Collections.emptyList()));
        assertEquals(Collections.emptyList(), object.getListValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.LIST_VALUE_WITH_PATTERN, null));
        assertNull(object.getListValueWithPattern());
    }

    @Test
    public void testDefaultContextIsInContextKeys()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertTrue("context default not in contextKeys",
                          object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT));
        assertEquals("default", object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT));

        setTestSystemProperty(TestSingleton.TEST_CONTEXT_DEFAULT, "notdefault");
        assertTrue("context default not in contextKeys",
                          object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT));
        assertEquals("notdefault", object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT));
    }

    @Test
    public void testDefaultContextVariableWhichRefersToThis()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertTrue("context default not in contextKeys",
                          object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT_WITH_THISREF));

        String expected = "a context var that refers to an attribute " + objectName;
        assertEquals(expected,
                            object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT_WITH_THISREF));
    }

    @Test
    public void testDerivedAttributeValue()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
        assertEquals((long) TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());

        // Check that update is ignored
        object.setAttributes(Collections.singletonMap(TestSingleton.DERIVED_VALUE, System.currentTimeMillis()));

        assertEquals((long) TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());
    }

    @Test
    public void testSecureValueRetrieval()
    {
        final String objectName = "myName";
        final String secret = "secret";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);
        attributes.put(TestSingleton.SECURE_VALUE, secret);

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals(AbstractConfiguredObject.SECURED_STRING_VALUE,
                            object.getAttribute(TestSingleton.SECURE_VALUE));
        assertEquals(secret, object.getSecureValue());

        //verify we can retrieve the actual secure value using system rights
        object.doAsSystem(
                     new PrivilegedAction<Object>()
                     {
                         @Override
                         public Object run()
                         {
                             assertEquals(secret, object.getAttribute(TestSingleton.SECURE_VALUE));
                             assertEquals(secret, object.getSecureValue());
                             return null;
                         }
                     });
    }

    @Test
    public void testImmutableAttribute()
    {
        final String originalValue = "myvalue";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "myName");
        attributes.put(TestSingleton.IMMUTABLE_VALUE, originalValue);

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertEquals("Immutable value unexpectedly changed", originalValue, object.getImmutableValue());

        // Update to the same value is allowed
                     object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, originalValue));

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, "newvalue"));
            fail("Exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            // PASS
        }
        assertEquals(originalValue, object.getImmutableValue());

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, null));
            fail("Exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            // PASS
        }

        assertEquals("Immutable value unexpectedly changed", originalValue, object.getImmutableValue());
    }

    @Test
    public void testImmutableAttributeNullValue()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "myName");
        attributes.put(TestSingleton.IMMUTABLE_VALUE, null);

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        assertNull(object.getImmutableValue());

        // Update to the same value is allowed
        object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, null));

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, "newvalue"));
            fail("Exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            // PASS
        }
        assertNull("Immutable value unexpectedly changed", object.getImmutableValue());
    }

    /** Id and Type are key attributes in the model and are thus worthy of test of their own */
    @Test
    public void testIdAndTypeAreImmutableAttribute()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "myName");

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
        UUID originalUuid = object.getId();
        String originalType = object.getType();

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.ID, UUID.randomUUID()));
            fail("Exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            // PASS
        }

        assertEquals(originalUuid, object.getId());

        try
        {
            object.setAttributes(Collections.singletonMap(TestSingleton.TYPE, "newtype"));
            fail("Exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            // PASS
        }

        assertEquals(originalType, object.getType());
    }

    @Test
    public void testSetAttributesFiresListener()
    {
        final String objectName = "listenerFiring";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);
        attributes.put(TestSingleton.STRING_VALUE, "first");

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        final AtomicInteger listenerCount = new AtomicInteger();
        final LinkedHashMap<String, String> updates = new LinkedHashMap<>();
        object.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void attributeSet(final ConfiguredObject<?> object,
                                     final String attributeName,
                                     final Object oldAttributeValue,
                                     final Object newAttributeValue)
            {
                listenerCount.incrementAndGet();
                String delta = String.valueOf(oldAttributeValue) + "=>" + String.valueOf(newAttributeValue);
                updates.put(attributeName, delta);
            }
        });

        // Set updated value (should cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "second"));

        assertEquals((long) 1, (long) listenerCount.get());
        String delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("first=>second", delta);

        // Set unchanged value (should not cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "second"));
        assertEquals((long) 1, (long) listenerCount.get());

        // Set value to null (should cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, null));
        assertEquals((long) 2, (long) listenerCount.get());
        delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("second=>null", delta);

        // Set to null again (should not cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, null));
        assertEquals((long) 2, (long) listenerCount.get());

        // Set updated value (should cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "third"));
        assertEquals((long) 3, (long) listenerCount.get());
        delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("null=>third", delta);
    }

    @Test
    public void testSetAttributesInterpolateValues()
    {
        setTestSystemProperty("foo1", "myValue1");
        setTestSystemProperty("foo2", "myValue2");
        setTestSystemProperty("foo3", null);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(TestSingleton.STRING_VALUE, "${foo1}");

        final TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        final AtomicInteger listenerCount = new AtomicInteger();
        object.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void attributeSet(final ConfiguredObject<?> object,
                                     final String attributeName,
                                     final Object oldAttributeValue,
                                     final Object newAttributeValue)
            {
                listenerCount.incrementAndGet();
            }
        });

        assertEquals("myValue1", object.getStringValue());
        assertEquals("${foo1}", object.getActualAttributes().get(TestSingleton.STRING_VALUE));

        // Update the actual value ${foo1} => ${foo2}
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "${foo2}"));
        assertEquals((long) 1, (long) listenerCount.get());

        assertEquals("myValue2", object.getStringValue());
        assertEquals("${foo2}", object.getActualAttributes().get(TestSingleton.STRING_VALUE));

        // No change
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "${foo2}"));
        assertEquals((long) 1, (long) listenerCount.get());

        // Update the actual value ${foo2} => ${foo3} (which doesn't have a value)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, "${foo3}"));
        assertEquals((long) 2, (long) listenerCount.get());
        assertEquals("${foo3}", object.getStringValue());
        assertEquals("${foo3}", object.getActualAttributes().get(TestSingleton.STRING_VALUE));
    }

    @Test
    public void testCreateAndLastUpdateDate() throws Exception
    {
        final String objectName = "myName";
        final Date now = new Date();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        Date createdTime = object.getCreatedTime();
        assertTrue("Create date not populated", createdTime.compareTo(now) >= 0);
        assertEquals("Last updated not populated", createdTime, object.getLastUpdatedTime());

        Thread.sleep(10);
        object.setAttributes(Collections.singletonMap(TestSingleton.DESCRIPTION, "desc"));
        assertEquals("Created time should not be updated by update", createdTime, object.getCreatedTime());
        assertTrue("Last update time should be updated by update",
                          object.getLastUpdatedTime().compareTo(createdTime) > 0);
    }

    @Test
    public void testStatistics() throws Exception
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);

        final Map<String, Object> stats = object.getStatistics();
        assertEquals("Unexpected number of statistics", (long) 1, (long) stats.size());
        assertTrue("Expected statistic not found", stats.containsKey("longStatistic"));
    }

    @Test
    public void testAuditInformation() throws Exception
    {
        final String creatingUser = "creatingUser";
        final String updatingUser = "updatingUser";
        final Subject creatorSubject = createTestAuthenticatedSubject(creatingUser);
        final Subject updaterSubject = createTestAuthenticatedSubject(updatingUser);
        final Date now = new Date();

        Thread.sleep(5);  // Let a small amount of time pass

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, "myName");

        final TestSingleton object = Subject.doAs(creatorSubject,
                     new PrivilegedAction<TestSingleton>()
                     {
                         @Override
                         public TestSingleton run()
                         {
                             return _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
                         }
                     });

        assertEquals("Unexpected creating user after object creation", creatingUser, object.getCreatedBy());
        assertEquals("Unexpected last updating user after object creation",
                            creatingUser,
                            object.getLastUpdatedBy());

        final Date originalCreatedTime = object.getCreatedTime();
        final Date originalLastUpdatedTime = object.getLastUpdatedTime();
        assertTrue("Unexpected created time", originalCreatedTime.after(now));
        assertEquals("Unexpected created and updated time", originalCreatedTime, originalLastUpdatedTime);

        Thread.sleep(5);  // Let a small amount of time pass

        Subject.doAs(updaterSubject,
                     new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             object.setAttributes(Collections.singletonMap(TestSingleton.INT_VALUE, 5));
                             return null;
                         }
                     });

        assertEquals("Creating user should not be changed by update", creatingUser, object.getCreatedBy());
        assertEquals("Created time should not be changed by update",
                            originalCreatedTime,
                            object.getCreatedTime());

        assertEquals("Last updated by should be changed by update", updatingUser, object.getLastUpdatedBy());
        assertTrue("Last updated time by should be changed by update",
                          originalLastUpdatedTime.before(object.getLastUpdatedTime()));
    }

    @Test
    public void testAuditInformationIgnoresUserSuppliedAttributes() throws Exception
    {
        final String user = "user";
        final Subject userSubject = createTestAuthenticatedSubject(user);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, "myName");
        attributes.put(TestSingleton.CREATED_BY, "bogusCreator");
        attributes.put(TestSingleton.CREATED_TIME, new Date(0));
        attributes.put(TestSingleton.LAST_UPDATED_BY, "bogusUpdater");
        attributes.put(TestSingleton.LAST_UPDATED_TIME, new Date(0));

        final Date now = new Date();
        Thread.sleep(5);  // Let a small amount of time pass

        final TestSingleton object = Subject.doAs(userSubject,
                                                  new PrivilegedAction<TestSingleton>()
                                                  {
                                                      @Override
                                                      public TestSingleton run()
                                                      {
                                                          return _model.getObjectFactory().create(TestSingleton.class,
                                                                    attributes, null);
                                                      }
                                                  });

        assertEquals("Unexpected creating user after object creation", user, object.getCreatedBy());
        assertEquals("Unexpected last updating user after object creation", user, object.getLastUpdatedBy());

        final Date originalCreatedTime = object.getCreatedTime();
        assertTrue("Unexpected created time", originalCreatedTime.after(now));
        final Date originalLastUpdatedTime = object.getLastUpdatedTime();
        assertEquals("Unexpected created and updated time", originalCreatedTime, originalLastUpdatedTime);

        // Let a small amount of time pass before we update
        Thread.sleep(50);

        Subject.doAs(userSubject,
                     new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final Map<String, Object> updateMap = new HashMap<>();
                             updateMap.put(TestSingleton.INT_VALUE, 5);
                             updateMap.put(TestSingleton.CREATED_BY, "bogusCreator");
                             updateMap.put(TestSingleton.CREATED_TIME, new Date(0));
                             updateMap.put(TestSingleton.LAST_UPDATED_BY, "bogusUpdater");
                             updateMap.put(TestSingleton.LAST_UPDATED_TIME, new Date(0));

                             object.setAttributes(updateMap);
                             return null;
                         }
                     });

        assertEquals("Creating user should not be changed by update", user, object.getCreatedBy());
        assertEquals("Created time should not be changed by update",
                            originalCreatedTime,
                            object.getCreatedTime());

        assertEquals("Last updated by should be changed by update", user, object.getLastUpdatedBy());
        assertTrue("Last updated time by should be changed by update",
                          originalLastUpdatedTime.before(object.getLastUpdatedTime()));
    }


    @Test
    public void testAuditInformationPersistenceAndRecovery() throws Exception
    {
        final String creatingUser = "creatingUser";
        final Subject creatorSubject = createTestAuthenticatedSubject(creatingUser);
        final String objectName = "myName";

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);

        final TestSingleton object = Subject.doAs(creatorSubject,
                                                  new PrivilegedAction<TestSingleton>()
                                                  {
                                                      @Override
                                                      public TestSingleton run()
                                                      {
                                                          return _model.getObjectFactory()
                                                                  .create(TestSingleton.class,
                                                                    attributes, null);
                                                      }
                                                  });

        final ConfiguredObjectRecord cor = object.asObjectRecord();
        final Map<String, Object> recordedAttributes = cor.getAttributes();

        assertTrue(recordedAttributes.containsKey(ConfiguredObject.LAST_UPDATED_BY));
        assertTrue(recordedAttributes.containsKey(ConfiguredObject.LAST_UPDATED_TIME));
        assertTrue(recordedAttributes.containsKey(ConfiguredObject.CREATED_BY));
        assertTrue(recordedAttributes.containsKey(ConfiguredObject.CREATED_TIME));

        assertEquals(creatingUser, recordedAttributes.get(ConfiguredObject.CREATED_BY));
        assertEquals(creatingUser, recordedAttributes.get(ConfiguredObject.LAST_UPDATED_BY));

        // Now recover the object

        final SystemConfig mockSystemConfig = mock(SystemConfig.class);
        when(mockSystemConfig.getId()).thenReturn(UUID.randomUUID());
        when(mockSystemConfig.getModel()).thenReturn(TestModel.getInstance());

        final TestSingleton recovered = (TestSingleton) _model.getObjectFactory().recover(cor, mockSystemConfig).resolve();
        recovered.open();

        assertEquals("Unexpected recovered object created by", object.getCreatedBy(), recovered.getCreatedBy());
        assertEquals("Unexpected recovered object created time",
                            object.getCreatedTime(),
                            recovered.getCreatedTime());

        assertEquals("Unexpected recovered object updated by",
                            object.getLastUpdatedBy(),
                            recovered.getLastUpdatedBy());
        assertEquals("Unexpected recovered object updated time",
                            object.getLastUpdatedTime(),
                            recovered.getLastUpdatedTime());
    }

    @Test
    public void testPostSetAttributesReportsChanges()
    {
        final String objectName = "myName";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                attributes, null);

        assertEquals(objectName, object.getName());

        object.setAttributes(Collections.emptyMap());
        assertTrue("Unexpected member of update set for empty update",
                          object.takeLastReportedSetAttributes().isEmpty());

        Map<String, Object> update = new HashMap<>();
        update.put(TestSingleton.NAME, objectName);
        update.put(TestSingleton.DESCRIPTION, "an update");

        object.setAttributes(update);
        assertEquals("Unexpected member of update set",
                            Sets.newHashSet(TestSingleton.DESCRIPTION),
                            object.takeLastReportedSetAttributes());
    }

    @Test
    public void testSetContextVariable()
    {
        final String objectName = "myName";
        final String contextVariableName = "myContextVariable";
        final String contextVariableValue = "myContextVariableValue";

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class,
                                                                Collections.singletonMap(TestSingleton.NAME, objectName),
                                                                null);

        String previousValue = object.setContextVariable(contextVariableName, contextVariableValue);

        assertNull("Previous value should be null", previousValue);

        Map<String, String> context = object.getContext();
        assertTrue("Context variable should be present in context", context.containsKey(contextVariableName));
        assertEquals("Unexpected context variable", contextVariableValue, context.get(contextVariableName));

        previousValue = object.setContextVariable(contextVariableName, "newValue");

        assertEquals("Unexpected previous value", contextVariableValue, previousValue);
    }

    @Test
    public void testRemoveContextVariable()
    {
        final String objectName = "myName";
        final String contextVariableName = "myContextVariable";
        final String contextVariableValue = "myContextVariableValue";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TestSingleton.NAME, objectName);
        attributes.put(TestSingleton.CONTEXT, Collections.singletonMap(contextVariableName, contextVariableValue));

        TestSingleton object = _model.getObjectFactory().create(TestSingleton.class, attributes, null);

        Map<String, String> context = object.getContext();
        assertEquals("Unexpected context variable", contextVariableValue, context.get(contextVariableName));

        String previousValue = object.removeContextVariable(contextVariableName);
        assertEquals("Unexpected context variable value", contextVariableValue, previousValue);

        context = object.getContext();
        assertFalse("Context variable should not be present in context",
                           context.containsKey(contextVariableName));


        previousValue = object.removeContextVariable(contextVariableName);
        assertNull("Previous value should be null", previousValue);
    }

    private Subject createTestAuthenticatedSubject(final String username)
    {
        return new Subject(true,
                           Collections.singleton(new AuthenticatedPrincipal(new UsernamePrincipal(username, null))),
                           Collections.emptySet(),
                           Collections.emptySet());
    }

}
