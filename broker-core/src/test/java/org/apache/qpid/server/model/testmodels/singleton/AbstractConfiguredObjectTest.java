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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
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
@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractConfiguredObjectTest extends UnitTestBase
{
    private final Model _model = TestModel.getInstance();
    private final ConfiguredObjectFactory _objectFactory = _model.getObjectFactory();

    @Test
    public void testAttributePersistence()
    {
        final String objectName = "testNonPersistAttributes";
        final TestSingleton object =
                _objectFactory.create(TestSingleton.class, Map.of(ConfiguredObject.NAME, objectName), null);

        assertEquals(objectName, object.getName());
        assertNull(object.getAutomatedNonPersistedValue());
        assertNull(object.getAutomatedPersistedValue());
        assertEquals(TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());

        ConfiguredObjectRecord record = object.asObjectRecord();

        assertEquals(objectName, record.getAttributes().get(ConfiguredObject.NAME));

        assertFalse(record.getAttributes().containsKey(TestSingleton.AUTOMATED_PERSISTED_VALUE));
        assertFalse(record.getAttributes().containsKey(TestSingleton.AUTOMATED_NONPERSISTED_VALUE));
        assertFalse(record.getAttributes().containsKey(TestSingleton.DERIVED_VALUE));

        final String newValue = "newValue";
        final Map<String, Object> updatedAttributes = Map.of(TestSingleton.AUTOMATED_PERSISTED_VALUE, newValue,
                TestSingleton.AUTOMATED_NONPERSISTED_VALUE, newValue,
                TestSingleton.DERIVED_VALUE, System.currentTimeMillis());  // Will be ignored
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
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestSingleton.DEFAULTED_VALUE_DEFAULT, object1.getDefaultedValue());
    }

    @Test
    public void testOverriddenDefaultedAttributeValue()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.DEFAULTED_VALUE, "override");
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals("override", object.getDefaultedValue());
    }

    @Test
    public void testOverriddenDefaultedAttributeValueRevertedToDefault()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.DEFAULTED_VALUE, "override");
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals("override", object.getDefaultedValue());

        object.setAttributes(Collections.singletonMap(TestSingleton.DEFAULTED_VALUE, null));

        assertEquals(TestSingleton.DEFAULTED_VALUE_DEFAULT, object.getDefaultedValue());
    }

    @Test
    public void testDefaultInitialization()
    {
        TestSingleton object =
                _objectFactory.create(TestSingleton.class, Map.of(ConfiguredObject.NAME, "testDefaultInitialization"), null);
        assertEquals(object.getAttrWithDefaultFromContextNoInit(), TestSingleton.testGlobalDefault);
        assertEquals(object.getAttrWithDefaultFromContextCopyInit(), TestSingleton.testGlobalDefault);
        assertEquals(object.getAttrWithDefaultFromContextMaterializeInit(), TestSingleton.testGlobalDefault);

        assertFalse(object.getActualAttributes().containsKey("attrWithDefaultFromContextNoInit"));
        assertEquals("${" + TestSingleton.TEST_CONTEXT_DEFAULT + "}",
                                object.getActualAttributes().get("attrWithDefaultFromContextCopyInit"));
        assertEquals(TestSingleton.testGlobalDefault,
                                object.getActualAttributes().get("attrWithDefaultFromContextMaterializeInit"));

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "testDefaultInitialization2",
                ConfiguredObject.CONTEXT, Map.of(TestSingleton.TEST_CONTEXT_DEFAULT, "foo"));
        object = _objectFactory.create(TestSingleton.class, attributes, null);
        assertEquals("foo", object.getAttrWithDefaultFromContextNoInit());
        assertEquals("foo", object.getAttrWithDefaultFromContextCopyInit());
        assertEquals("foo", object.getAttrWithDefaultFromContextMaterializeInit());

        assertFalse(object.getActualAttributes().containsKey("attrWithDefaultFromContextNoInit"));
        assertEquals("${" + TestSingleton.TEST_CONTEXT_DEFAULT + "}",
                                object.getActualAttributes().get("attrWithDefaultFromContextCopyInit"));
        assertEquals("foo", object.getActualAttributes().get("attrWithDefaultFromContextMaterializeInit"));

        setTestSystemProperty(TestSingleton.TEST_CONTEXT_DEFAULT, "bar");
        object = _objectFactory.create(TestSingleton.class,
                Map.of(ConfiguredObject.NAME, "testDefaultInitialization3"), null);

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
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.ENUM_VALUE, TestEnum.TEST_ENUM1.name());
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestEnum.TEST_ENUM1, object1.getEnumValue());
    }

    @Test
    public void testEnumAttributeValueFromEnum()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.ENUM_VALUE, TestEnum.TEST_ENUM1);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(TestEnum.TEST_ENUM1, object1.getEnumValue());
    }

    @Test
    public void testIntegerAttributeValueFromString()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.INT_VALUE, "-4");
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(-4, (long) object1.getIntValue());
    }

    @Test
    public void testIntegerAttributeValueFromInteger()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.INT_VALUE, 5);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(5, (long) object1.getIntValue());
    }

    @Test
    public void testIntegerAttributeValueFromDouble()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.INT_VALUE, 6.1);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals(6, (long) object.getIntValue());
    }

    @Test
    public void testDateAttributeFromMillis()
    {
        final String objectName = "myName";
        final long now = System.currentTimeMillis();
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.DATE_VALUE, now);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals(new Date(now), object.getDateValue());
    }

    @Test
    public void testDateAttributeFromIso8601()
    {
        final String objectName = "myName";
        final String date = "1970-01-01";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.DATE_VALUE, date);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());
        assertEquals(new Date(0), object.getDateValue());
    }

    @Test
    public void testStringAttributeValueFromContextVariableProvidedBySystemProperty()
    {
        final String sysPropertyName = "testStringAttributeValueFromContextVariableProvidedBySystemProperty";
        final String contextToken = "${" + sysPropertyName + "}";

        System.setProperty(sysPropertyName, "myValue");

        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.STRING_VALUE, contextToken);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals("myValue", object1.getStringValue());

        // System property set empty string

        System.setProperty(sysPropertyName, "");
        final TestSingleton object2 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals("", object2.getStringValue());

        // System property not set
        System.clearProperty(sysPropertyName);

        final TestSingleton object3 = _objectFactory.create(TestSingleton.class, attributes, null);

        // yields the unexpanded token - not sure if this is really useful behaviour?
        assertEquals(contextToken, object3.getStringValue());
    }

    @Test
    public void testMapAttributeValueFromContextVariableProvidedBySystemProperty()
    {
        final String sysPropertyName = "testMapAttributeValueFromContextVariableProvidedBySystemProperty";
        final String contextToken = "${" + sysPropertyName + "}";
        final Map<String,String> expectedMap = Map.of("field1", "value1",
                "field2", "value2");

        System.setProperty(sysPropertyName, "{ \"field1\" : \"value1\", \"field2\" : \"value2\"}");

        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.MAP_VALUE, contextToken);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object1.getName());
        assertEquals(expectedMap, object1.getMapValue());

        // System property not set
        System.clearProperty(sysPropertyName);
    }

    @Test
    public void testStringAttributeValueFromContextVariableProvidedObjectsContext()
    {
        final String contextToken = "${myReplacement}";
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                ConfiguredObject.CONTEXT, Map.of("myReplacement", "myValue"),
                TestSingleton.STRING_VALUE, contextToken);
        final TestSingleton object1 = _objectFactory.create(TestSingleton.class, attributes, null);

        // Check the object's context itself
        assertTrue(object1.getContext().containsKey("myReplacement"));
        assertEquals("myValue", object1.getContext().get("myReplacement"));

        assertEquals(objectName, object1.getName());
        assertEquals("myValue", object1.getStringValue());
    }

    @Test
    public void testInvalidIntegerAttributeValueFromContextVariable()
    {
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, "myName",
                TestSingleton.TYPE, TestSingletonImpl.TEST_SINGLETON_TYPE,
                TestSingleton.CONTEXT, Map.of("contextVal", "notAnInteger"),
                TestSingleton.INT_VALUE, "${contextVal}");

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> _objectFactory.create(TestSingleton.class, attributes, null),
                "Creation of child object should have failed due to invalid value");
        final String message = thrown.getMessage();
        assertTrue(message.contains("intValue"), "Message does not contain the attribute name");
        assertTrue(message.contains("contextVal"), "Message does not contain the non-interpolated value");
        assertTrue(message.contains("contextVal"), "Message does not contain the interpolated value");
    }

    @Test
    public void testCreateEnforcesAttributeValidValues()
    {
        final String objectName = getTestName();
        final Map<String, Object> illegalCreateAttributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALID_VALUE, "illegal");

        assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(TestSingleton.class, illegalCreateAttributes, null),
                "Exception not thrown");

        final Map<String, Object> legalCreateAttributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE1);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals(TestSingleton.VALID_VALUE1, object.getValidValue());
    }

    @Test
    public void testCreateEnforcesAttributeValidValuePattern()
    {
        final String objectName = getTestName();
        final Map<String, Object> illegalCreateAttributes1 = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALUE_WITH_PATTERN, "illegal");

        assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(TestSingleton.class, illegalCreateAttributes1, null),
                "Exception not thrown");

        final Map<String, Object> illegalCreateAttributes2 = Map.of(
                ConfiguredObject.NAME, objectName,
                TestSingleton.LIST_VALUE_WITH_PATTERN, List.of("1.1.1.1", "1"));

        assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(TestSingleton.class, illegalCreateAttributes2, null),
                "Exception not thrown");

        final Map<String, Object> legalCreateAttributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALUE_WITH_PATTERN, "foozzzzzbar",
                TestSingleton.LIST_VALUE_WITH_PATTERN, List.of("1.1.1.1", "255.255.255.255"));

        final TestSingleton object = _objectFactory.create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals("foozzzzzbar", object.getValueWithPattern());
    }

    @Test
    public void testChangeEnforcesAttributeValidValues()
    {
        final String objectName = getTestName();
        final Map<String, Object> legalCreateAttributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE1);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals(TestSingleton.VALID_VALUE1, object.getValidValue());

        object.setAttributes(Map.of(TestSingleton.VALID_VALUE, TestSingleton.VALID_VALUE2));
        assertEquals(TestSingleton.VALID_VALUE2, object.getValidValue());

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.VALID_VALUE, "illegal")),
                "Exception not thrown");

        assertEquals(TestSingleton.VALID_VALUE2, object.getValidValue());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALID_VALUE, null));
        assertNull(object.getValidValue());
    }

    @Test
    public void testCreateEnforcesAttributeValidValuesWithSets()
    {
        final String objectName = getTestName();
        final Map<String, Object> name = Map.of(ConfiguredObject.NAME, objectName);

        final Map<String, Object> illegalCreateAttributes = new HashMap<>(name);
        illegalCreateAttributes.put(TestSingleton.ENUMSET_VALUES, Set.of(TestEnum.TEST_ENUM3));

        assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(TestSingleton.class, illegalCreateAttributes, null),
                "Exception not thrown");


        {
            final Map<String, Object> legalCreateAttributesEnums = new HashMap<>(name);
            legalCreateAttributesEnums.put(TestSingleton.ENUMSET_VALUES,
                    List.of(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3));

            final TestSingleton obj = _objectFactory.create(TestSingleton.class, legalCreateAttributesEnums, null);
            assertTrue(obj.getEnumSetValues().containsAll(List.of(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3)));
        }

        {
            final Map<String, Object> legalCreateAttributesStrings = new HashMap<>(name);
            legalCreateAttributesStrings.put(TestSingleton.ENUMSET_VALUES,
                    List.of(TestEnum.TEST_ENUM2.name(), TestEnum.TEST_ENUM3.name()));

            final TestSingleton obj =
                    _objectFactory.create(TestSingleton.class, legalCreateAttributesStrings, null);
            assertTrue(obj.getEnumSetValues().containsAll(List.of(TestEnum.TEST_ENUM2, TestEnum.TEST_ENUM3)));
        }
    }

    @Test
    public void testChangeEnforcesAttributeValidValuePatterns()
    {
        final String objectName = getTestName();
        final Map<String, Object> legalCreateAttributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.VALUE_WITH_PATTERN, "foozzzzzbar",
                TestSingleton.LIST_VALUE_WITH_PATTERN, List.of("1.1.1.1", "255.255.255.255"));

        final TestSingleton object = _objectFactory.create(TestSingleton.class, legalCreateAttributes, null);
        assertEquals("foozzzzzbar", object.getValueWithPattern());
        assertEquals(List.of("1.1.1.1", "255.255.255.255"), object.getListValueWithPattern());

        object.setAttributes(Map.of(TestSingleton.VALUE_WITH_PATTERN, "foobar"));
        assertEquals("foobar", object.getValueWithPattern());

        object.setAttributes(Map.of(TestSingleton.LIST_VALUE_WITH_PATTERN, Collections.singletonList("1.2.3.4")));
        assertEquals(Collections.singletonList("1.2.3.4"), object.getListValueWithPattern());

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.VALUE_WITH_PATTERN, "foobaz")),
                "Exception not thrown");
        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.LIST_VALUE_WITH_PATTERN, List.of("1.1.1.1", "1"))),
                "Exception not thrown");

        assertEquals("foobar", object.getValueWithPattern());
        assertEquals(Collections.singletonList("1.2.3.4"), object.getListValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.VALUE_WITH_PATTERN, null));
        assertNull(object.getValueWithPattern());

        object.setAttributes(Map.of(TestSingleton.LIST_VALUE_WITH_PATTERN, List.of()));
        assertEquals(List.of(), object.getListValueWithPattern());

        object.setAttributes(Collections.singletonMap(TestSingleton.LIST_VALUE_WITH_PATTERN, null));
        assertNull(object.getListValueWithPattern());
    }

    @Test
    public void testDefaultContextIsInContextKeys()
    {
        final String objectName = "myName";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertTrue(object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT),
                "context default not in contextKeys");
        assertEquals("default", object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT));

        setTestSystemProperty(TestSingleton.TEST_CONTEXT_DEFAULT, "notdefault");
        assertTrue(object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT),
                "context default not in contextKeys");
        assertEquals("notdefault", object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT));
    }

    @Test
    public void testDefaultContextVariableWhichRefersToThis()
    {
        final String objectName = "myName";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertTrue(object.getContextKeys(true).contains(TestSingleton.TEST_CONTEXT_DEFAULT_WITH_THISREF),
                "context default not in contextKeys");

        final String expected = "a context var that refers to an attribute " + objectName;
        assertEquals(expected, object.getContextValue(String.class, TestSingleton.TEST_CONTEXT_DEFAULT_WITH_THISREF));
    }

    @Test
    public void testDerivedAttributeValue()
    {
        final String objectName = "myName";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);
        assertEquals(TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());

        // Check that update is ignored
        object.setAttributes(Map.of(TestSingleton.DERIVED_VALUE, System.currentTimeMillis()));

        assertEquals(TestSingletonImpl.DERIVED_VALUE, object.getDerivedValue());
    }

    @Test
    public void testSecureValueRetrieval()
    {
        final String objectName = "myName";
        final String secret = "secret";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.SECURE_VALUE, secret);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(AbstractConfiguredObject.SECURED_STRING_VALUE, object.getAttribute(TestSingleton.SECURE_VALUE));
        assertEquals(secret, object.getSecureValue());

        //verify we can retrieve the actual secure value using system rights
        object.doAsSystem((PrivilegedAction<Object>) () ->
        {
            assertEquals(secret, object.getAttribute(TestSingleton.SECURE_VALUE));
            assertEquals(secret, object.getSecureValue());
            return null;
        });
    }

    @Test
    public void testImmutableAttribute()
    {
        final String originalValue = "myvalue";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "myName",
                TestSingleton.IMMUTABLE_VALUE, originalValue);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(originalValue, object.getImmutableValue(), "Immutable value unexpectedly changed");

        // Update to the same value is allowed
        object.setAttributes(Map.of(TestSingleton.IMMUTABLE_VALUE, originalValue));

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.IMMUTABLE_VALUE, "newvalue")),
                "Exception not thrown");
        assertEquals(originalValue, object.getImmutableValue());

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, null)),
                "Exception not thrown");
        assertEquals(originalValue, object.getImmutableValue(), "Immutable value unexpectedly changed");
    }

    @Test
    public void testImmutableAttributeNullValue()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "myName");
        attributes.put(TestSingleton.IMMUTABLE_VALUE, null);

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertNull(object.getImmutableValue());

        // Update to the same value is allowed
        object.setAttributes(Collections.singletonMap(TestSingleton.IMMUTABLE_VALUE, null));

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.IMMUTABLE_VALUE, "newvalue")),
                "Exception not thrown");
        assertNull(object.getImmutableValue(), "Immutable value unexpectedly changed");
    }

    /** Id and Type are key attributes in the model and are thus worthy of test of their own */
    @Test
    public void testIdAndTypeAreImmutableAttribute()
    {
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "myName");
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);
        final UUID originalUuid = object.getId();
        final String originalType = object.getType();

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.ID, randomUUID())),
                "Exception not thrown");
        assertEquals(originalUuid, object.getId());

        assertThrows(IllegalConfigurationException.class,
                () -> object.setAttributes(Map.of(TestSingleton.TYPE, "newtype")),
                "Exception not thrown");
        assertEquals(originalType, object.getType());
    }

    @Test
    public void testSetAttributesFiresListener()
    {
        final String objectName = "listenerFiring";

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                TestSingleton.STRING_VALUE, "first");

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

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
                String delta = oldAttributeValue + "=>" + newAttributeValue;
                updates.put(attributeName, delta);
            }
        });

        // Set updated value (should cause listener to fire)
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "second"));

        assertEquals(1, (long) listenerCount.get());
        String delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("first=>second", delta);

        // Set unchanged value (should not cause listener to fire)
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "second"));
        assertEquals(1, (long) listenerCount.get());

        // Set value to null (should cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, null));
        assertEquals(2, (long) listenerCount.get());
        delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("second=>null", delta);

        // Set to null again (should not cause listener to fire)
        object.setAttributes(Collections.singletonMap(TestSingleton.STRING_VALUE, null));
        assertEquals(2, (long) listenerCount.get());

        // Set updated value (should cause listener to fire)
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "third"));
        assertEquals(3, (long) listenerCount.get());
        delta = updates.remove(TestSingleton.STRING_VALUE);
        assertEquals("null=>third", delta);
    }

    @Test
    public void testSetAttributesInterpolateValues()
    {
        setTestSystemProperty("foo1", "myValue1");
        setTestSystemProperty("foo2", "myValue2");
        setTestSystemProperty("foo3", null);

        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, getTestName(),
                TestSingleton.STRING_VALUE, "${foo1}");

        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

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
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "${foo2}"));
        assertEquals(1, (long) listenerCount.get());

        assertEquals("myValue2", object.getStringValue());
        assertEquals("${foo2}", object.getActualAttributes().get(TestSingleton.STRING_VALUE));

        // No change
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "${foo2}"));
        assertEquals(1, (long) listenerCount.get());

        // Update the actual value ${foo2} => ${foo3} (which doesn't have a value)
        object.setAttributes(Map.of(TestSingleton.STRING_VALUE, "${foo3}"));
        assertEquals(2, (long) listenerCount.get());
        assertEquals("${foo3}", object.getStringValue());
        assertEquals("${foo3}", object.getActualAttributes().get(TestSingleton.STRING_VALUE));
    }

    @Test
    public void testCreateAndLastUpdateDate() throws Exception
    {
        final String objectName = "myName";
        final Date now = new Date();
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);
        final Date createdTime = object.getCreatedTime();
        assertTrue(createdTime.compareTo(now) >= 0, "Create date not populated");
        assertEquals(createdTime, object.getLastUpdatedTime(), "Last updated not populated");

        Thread.sleep(10);
        object.setAttributes(Map.of(TestSingleton.DESCRIPTION, "desc"));
        assertEquals(createdTime, object.getCreatedTime(), "Created time should not be updated by update");
        assertTrue(object.getLastUpdatedTime().compareTo(createdTime) > 0,
                "Last update time should be updated by update");
    }

    @Test
    public void testStatistics()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);
        final Map<String, Object> stats = object.getStatistics();
        assertEquals(1, (long) stats.size(), "Unexpected number of statistics");
        assertTrue(stats.containsKey("longStatistic"), "Expected statistic not found");
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

        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, "myName");

        final TestSingleton object = Subject.doAs(creatorSubject, (PrivilegedAction<TestSingleton>) () ->
                _objectFactory.create(TestSingleton.class, attributes, null));

        assertEquals(creatingUser, object.getCreatedBy(), "Unexpected creating user after object creation");
        assertEquals(creatingUser, object.getLastUpdatedBy(), "Unexpected last updating user after object creation");

        final Date originalCreatedTime = object.getCreatedTime();
        final Date originalLastUpdatedTime = object.getLastUpdatedTime();
        assertTrue(originalCreatedTime.after(now), "Unexpected created time");
        assertEquals(originalCreatedTime, originalLastUpdatedTime, "Unexpected created and updated time");

        Thread.sleep(5);  // Let a small amount of time pass

        Subject.doAs(updaterSubject, (PrivilegedAction<Void>) () ->
        {
            object.setAttributes(Map.of(TestSingleton.INT_VALUE, 5));
            return null;
        });

        assertEquals(creatingUser, object.getCreatedBy(), "Creating user should not be changed by update");
        assertEquals(originalCreatedTime, object.getCreatedTime(),
                "Created time should not be changed by update");

        assertEquals(updatingUser, object.getLastUpdatedBy(), "Last updated by should be changed by update");
        assertTrue(originalLastUpdatedTime.before(object.getLastUpdatedTime()),
                "Last updated time by should be changed by update");
    }

    @Test
    public void testAuditInformationIgnoresUserSuppliedAttributes() throws Exception
    {
        final String user = "user";
        final Subject userSubject = createTestAuthenticatedSubject(user);
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, "myName",
                TestSingleton.CREATED_BY, "bogusCreator",
                TestSingleton.CREATED_TIME, new Date(0),
                TestSingleton.LAST_UPDATED_BY, "bogusUpdater",
                TestSingleton.LAST_UPDATED_TIME, new Date(0));
        final Date now = new Date();
        Thread.sleep(5);  // Let a small amount of time pass

        final TestSingleton object = Subject.doAs(userSubject, (PrivilegedAction<TestSingleton>) () ->
                _objectFactory.create(TestSingleton.class, attributes, null));

        assertEquals(user, object.getCreatedBy(), "Unexpected creating user after object creation");
        assertEquals(user, object.getLastUpdatedBy(), "Unexpected last updating user after object creation");

        final Date originalCreatedTime = object.getCreatedTime();
        assertTrue(originalCreatedTime.after(now), "Unexpected created time");
        final Date originalLastUpdatedTime = object.getLastUpdatedTime();
        assertEquals(originalCreatedTime, originalLastUpdatedTime, "Unexpected created and updated time");

        // Let a small amount of time pass before we update
        Thread.sleep(50);

        Subject.doAs(userSubject, (PrivilegedAction<Void>) () ->
        {
            final Map<String, Object> updateMap = Map.of(TestSingleton.INT_VALUE, 5,
                    TestSingleton.CREATED_BY, "bogusCreator",
                    TestSingleton.CREATED_TIME, new Date(0),
                    TestSingleton.LAST_UPDATED_BY, "bogusUpdater",
                    TestSingleton.LAST_UPDATED_TIME, new Date(0));
            object.setAttributes(updateMap);
            return null;
        });

        assertEquals(user, object.getCreatedBy(), "Creating user should not be changed by update");
        assertEquals(originalCreatedTime, object.getCreatedTime(), "Created time should not be changed by update");

        assertEquals(user, object.getLastUpdatedBy(), "Last updated by should be changed by update");
        assertTrue(originalLastUpdatedTime.before(object.getLastUpdatedTime()),
                "Last updated time by should be changed by update");
    }

    @Test
    public void testAuditInformationPersistenceAndRecovery()
    {
        final String creatingUser = "creatingUser";
        final Subject creatorSubject = createTestAuthenticatedSubject(creatingUser);
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName);
        final TestSingleton object = Subject.doAs(creatorSubject, (PrivilegedAction<TestSingleton>) () ->
                _objectFactory.create(TestSingleton.class, attributes, null));
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
        when(mockSystemConfig.getId()).thenReturn(randomUUID());
        when(mockSystemConfig.getModel()).thenReturn(TestModel.getInstance());

        final TestSingleton recovered = (TestSingleton) _objectFactory.recover(cor, mockSystemConfig).resolve();
        recovered.open();

        assertEquals(object.getCreatedBy(), recovered.getCreatedBy(), "Unexpected recovered object created by");
        assertEquals(object.getCreatedTime(), recovered.getCreatedTime(), "Unexpected recovered object created time");

        assertEquals(object.getLastUpdatedBy(), recovered.getLastUpdatedBy(), "Unexpected recovered object updated by");
        assertEquals(object.getLastUpdatedTime(), recovered.getLastUpdatedTime(), "Unexpected recovered object updated time");
    }

    @Test
    public void testPostSetAttributesReportsChanges()
    {
        final String objectName = "myName";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName);
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        assertEquals(objectName, object.getName());

        object.setAttributes(Map.of());
        assertTrue(object.takeLastReportedSetAttributes().isEmpty(), "Unexpected member of update set for empty update");

        final Map<String, Object> update = Map.of(TestSingleton.NAME, objectName, TestSingleton.DESCRIPTION, "an update");

        object.setAttributes(update);
        assertEquals(Sets.newHashSet(TestSingleton.DESCRIPTION), object.takeLastReportedSetAttributes(),
                "Unexpected member of update set");
    }

    @Test
    public void testSetContextVariable()
    {
        final String objectName = "myName";
        final String contextVariableName = "myContextVariable";
        final String contextVariableValue = "myContextVariableValue";
        final TestSingleton object = _objectFactory.create(TestSingleton.class, Map.of(TestSingleton.NAME, objectName),
                null);

        String previousValue = object.setContextVariable(contextVariableName, contextVariableValue);

        assertNull(previousValue, "Previous value should be null");

        final Map<String, String> context = object.getContext();
        assertTrue(context.containsKey(contextVariableName), "Context variable should be present in context");
        assertEquals(contextVariableValue, context.get(contextVariableName), "Unexpected context variable");

        previousValue = object.setContextVariable(contextVariableName, "newValue");

        assertEquals(contextVariableValue, previousValue, "Unexpected previous value");
    }

    @Test
    public void testRemoveContextVariable()
    {
        final String objectName = "myName";
        final String contextVariableName = "myContextVariable";
        final String contextVariableValue = "myContextVariableValue";
        final Map<String, Object> attributes = Map.of(TestSingleton.NAME, objectName,
                TestSingleton.CONTEXT, Map.of(contextVariableName, contextVariableValue));
        final TestSingleton object = _objectFactory.create(TestSingleton.class, attributes, null);

        Map<String, String> context = object.getContext();
        assertEquals(contextVariableValue, context.get(contextVariableName), "Unexpected context variable");

        String previousValue = object.removeContextVariable(contextVariableName);
        assertEquals(contextVariableValue, previousValue, "Unexpected context variable value");

        context = object.getContext();
        assertFalse(context.containsKey(contextVariableName), "Context variable should not be present in context");

        previousValue = object.removeContextVariable(contextVariableName);
        assertNull(previousValue, "Previous value should be null");
    }

    private Subject createTestAuthenticatedSubject(final String username)
    {
        final UsernamePrincipal usernamePrincipal = new UsernamePrincipal(username, null);
        final AuthenticatedPrincipal principal = new AuthenticatedPrincipal(usernamePrincipal);
        return new Subject(true, Set.of(principal), Set.of(), Set.of());
    }
}
