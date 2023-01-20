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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.ConfiguredSettableAttribute;
import org.apache.qpid.server.model.ManagedInterface;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectTypeRegistryTest extends UnitTestBase
{
    private final ConfiguredObjectTypeRegistry _typeRegistry = TestModel.getInstance().getTypeRegistry();

    @Test
    @SuppressWarnings("rawtypes")
    public void testTypeSpecialisations()
    {
        final Collection<Class<? extends ConfiguredObject>> types =
                _typeRegistry.getTypeSpecialisations(TestEngine.class);

        assertEquals(3, (long) types.size(), "Unexpected number of specialisations for " +
                TestEngine.class + " Found : " + types);
        assertTrue(types.contains(TestPetrolEngineImpl.class));
        assertTrue(types.contains(TestHybridEngineImpl.class));
        assertTrue(types.contains(TestElecEngineImpl.class));
    }

    @Test
    public void testGetValidChildTypes()
    {
        // The standard car restricts its engine type
        final Collection<String> standardCarValidEnginesTypes =
                _typeRegistry.getValidChildTypes(TestStandardCarImpl.class, TestEngine.class);
        assertThat(standardCarValidEnginesTypes, hasItem(TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE));
        assertThat(standardCarValidEnginesTypes, hasItem(TestHybridEngineImpl.TEST_HYBRID_ENGINE_TYPE));
        assertThat(standardCarValidEnginesTypes.size(), is(2));

        final Collection<String> kitCarValidEngineTypes =
                _typeRegistry.getValidChildTypes(TestKitCarImpl.class, TestEngine.class);
        // Would it be more useful to producers of management UIs if this were populated with all possible types?
        assertNull(kitCarValidEngineTypes);
    }

    @Test
    public void testManagedInterfaces()
    {
        // The electric engine is rechargable
        final Set<Class<? extends ManagedInterface>> elecEngIntfcs =
                _typeRegistry.getManagedInterfaces(TestElecEngine.class);
        assertThat(elecEngIntfcs, hasItem(TestRechargeable.class));
        assertThat(elecEngIntfcs.size(), is(1));

        // The petrol engine implements no additional interfaces
        final Set<Class<? extends ManagedInterface>> stdCarIntfcs =
                _typeRegistry.getManagedInterfaces(TestPetrolEngine.class);
        assertThat(stdCarIntfcs.size(), is(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOperations()
    {
        final String objectName = "testKitCar";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        final TestCar<?> object =
                TestModel.getInstance().getObjectFactory().create(TestCar.class, attributes, null);

        assertEquals(TestKitCarImpl.class, object.getTypeClass());

        final Map<String, ConfiguredObjectOperation<?>> kitCarOperations =
                _typeRegistry.getOperations(object.getClass());
        assertTrue(kitCarOperations.containsKey("openDoor"));
        final ConfiguredObjectOperation<TestCar<?>> operation =
                (ConfiguredObjectOperation<TestCar<?>>) kitCarOperations.get("openDoor");

        // test explicitly setting parameter
        Object returnVal = operation.perform(object, Map.of("door", "DRIVER"));
        assertEquals(TestCar.Door.DRIVER, returnVal);

        // test default parameter
        returnVal = operation.perform(object, Map.of());
        assertEquals(TestCar.Door.PASSENGER, returnVal);

        assertThrows(IllegalArgumentException.class,
                () -> operation.perform(object, Map.of("seat", "DRIVER")),
                "Should not be able to pass in an unused parameter");
        assertThrows(IllegalArgumentException.class,
                () -> operation.perform(object, Map.of("door", "[\"eggs\", \"flour\", \"milk\"]")),
                "Should not be able to pass in a parameter of the wrong type");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOperationWithMandatoryParameter_RejectsNullParameter()
    {
        final String objectName = "testKitCar";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar<?> object = TestModel.getInstance().getObjectFactory().create(TestCar.class, attributes, null);
        final Map<String, ConfiguredObjectOperation<?>> operations = _typeRegistry.getOperations(object.getClass());
        final ConfiguredObjectOperation<TestCar<?>> operation = (ConfiguredObjectOperation<TestCar<?>>) operations.get("startEngine");

        assertThrows(IllegalArgumentException.class,
                () -> operation.perform(object, Map.of()),
                "Exception not thrown");
        assertThrows(IllegalArgumentException.class,
                () -> operation.perform(object, Collections.singletonMap("keyCode", null)),
                "Exception not thrown");
    }

    @Test
    public void testEnumValidValues_UnrestrictedSet()
    {
        final Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes =
                _typeRegistry.getAttributeTypes(TestCar.class);
        final ConfiguredSettableAttribute<?, ?> attribute =
                (ConfiguredSettableAttribute<?, ?>) attributeTypes.get("bodyColour");

        assertEquals(Lists.newArrayList("BLACK", "RED", "BLUE", "GREY"), attribute.validValues(),
                "The attribute's valid values should match the set of the enum");
    }

    @Test
    public void testEnumValidValues_RestrictedSet()
    {
        final Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes =
                _typeRegistry.getAttributeTypes(TestCar.class);
        final ConfiguredSettableAttribute<?, ?> attribute =
                (ConfiguredSettableAttribute<?, ?>) attributeTypes.get("interiorColour");

        assertEquals(Lists.newArrayList("GREY", "BLACK"), attribute.validValues(),
                "The attribute's valid values should match the restricted set defined on the attribute itself");
    }
}
