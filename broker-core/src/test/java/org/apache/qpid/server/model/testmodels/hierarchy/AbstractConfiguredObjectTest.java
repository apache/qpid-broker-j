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

package org.apache.qpid.server.model.testmodels.hierarchy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.test.utils.QpidTestCase;

/**
 * Tests behaviour of AbstractConfiguredObjects when hierarchies of objects are used together.
 * Responsibilities to include adding/removing of children and correct firing of listeners.
 */
public class AbstractConfiguredObjectTest extends QpidTestCase
{
    private final Model _model = TestModel.getInstance();

    public void testCreateCategoryDefault()
    {
        final String objectName = "testCreateCategoryDefault";
        Map<String, Object> attributes = Collections.<String, Object>singletonMap(ConfiguredObject.NAME, objectName);

        TestCar object = _model.getObjectFactory().create(TestCar.class, attributes);

        assertEquals(objectName, object.getName());
        assertEquals(TestStandardCarImpl.TEST_STANDARD_CAR_TYPE, object.getType());
        assertTrue(object instanceof TestStandardCar);
    }

    public void testCreateUnrecognisedType()
    {
        final String objectName = "testCreateCategoryDefault";
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, objectName);
        attributes.put(ConfiguredObject.TYPE, "notatype");

        try
        {
            _model.getObjectFactory().create(TestCar.class, attributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }
    }

    public void testCreateCarWithEngine()
    {
        final String carName = "myCar";
        Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, carName);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes);

        assertEquals(carName, car.getName());

        assertEquals(0, car.getChildren(TestEngine.class).size());

        String engineName = "myEngine";

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertEquals(1, car.getChildren(TestEngine.class).size());

        assertEquals(engineName, engine.getName());
        assertEquals(TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE, engine.getType());

    }

    public void testDefaultContextVariableWhichRefersToAncestor()
    {
        final String carName = "myCar";
        Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, carName);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes);

        assertEquals(carName, car.getName());

        assertEquals(0, car.getChildren(TestEngine.class).size());

        String engineName = "myEngine";

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);


        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertTrue("context var not in contextKeys",
                   car.getContextKeys(true).contains(TestCar.TEST_CONTEXT_VAR));

        String expected = "a value " + carName;
        assertEquals(expected, car.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR));

        assertTrue("context var not in contextKeys",
                   engine.getContextKeys(true).contains(TestCar.TEST_CONTEXT_VAR));
        assertEquals(expected, engine.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR));

    }

    public void testDuplicateChildRejected_Name()
    {
        doDuplicateChildCheck(ConfiguredObject.NAME);
    }

    public void testDuplicateChildRejected_Id()
    {
        doDuplicateChildCheck(ConfiguredObject.ID);
    }

    private void doDuplicateChildCheck(final String attrToDuplicate)
    {
        final String carName = "myCar";

        Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, carName);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes);
        assertEquals(0, car.getChildren(TestEngine.class).size());

        String engineName = "myEngine";
        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);
        assertEquals(1, car.getChildren(TestEngine.class).size());


        Map<String, Object> secondEngineNameAttribute = new HashMap<>();
        secondEngineNameAttribute.put(ConfiguredObject.TYPE, TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE);

        if (attrToDuplicate.equals(ConfiguredObject.NAME))
        {
            String secondEngineName = "myEngine";
            secondEngineNameAttribute.put(ConfiguredObject.NAME, secondEngineName);
        }
        else
        {
            String secondEngineName = "myPetrolEngine";
            secondEngineNameAttribute.put(ConfiguredObject.ID, engine.getId());
            secondEngineNameAttribute.put(ConfiguredObject.NAME, secondEngineName);
        }

        try
        {
            car.createChild(TestEngine.class, secondEngineNameAttribute);
            fail("exception not thrown");
        }
        catch (AbstractConfiguredObject.DuplicateNameException dne)
        {
            assertEquals(ConfiguredObject.NAME, attrToDuplicate);
            // PASS
        }
        catch (AbstractConfiguredObject.DuplicateIdException die)
        {
            // PASS
            assertEquals(ConfiguredObject.ID, attrToDuplicate);

        }

        assertEquals("Unexpected number of children after rejected duplicate", 1, car.getChildren(TestEngine.class).size());
        assertSame(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertSame(engine, car.getChildByName(TestEngine.class, engine.getName()));
    }


}
