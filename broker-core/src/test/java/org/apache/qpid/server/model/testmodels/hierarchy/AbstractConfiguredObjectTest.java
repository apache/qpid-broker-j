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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
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

        TestCar object = _model.getObjectFactory().create(TestCar.class, attributes, null);

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
            _model.getObjectFactory().create(TestCar.class, attributes, null);
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

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

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

    public void testGetChildren_NewChild()
    {
        TestCar car = _model.getObjectFactory().create(TestCar.class, Collections.<String, Object>singletonMap(ConfiguredObject.NAME, "myCar"), null);

        String engineName = "myEngine";
        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);

        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        // Check we can observe the new child from the parent

        assertEquals(1, car.getChildren(TestEngine.class).size());
        assertEquals(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertEquals(engine, car.getChildByName(TestEngine.class, engine.getName()));

        ListenableFuture attainedChild = car.getAttainedChildByName(TestEngine.class, engine.getName());
        assertNotNull(attainedChild);
        assertTrue("Engine should have already attained state", attainedChild.isDone());
    }

    public void testGetChildren_RecoveredChild() throws Exception
    {
        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, car.getChildren(TestEngine.class).size());
        TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();

        assertEquals(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertEquals(engine, car.getChildByName(TestEngine.class, engine.getName()));

        ListenableFuture attainedChild = car.getAttainedChildByName(TestEngine.class, engine.getName());
        assertNotNull(attainedChild);
        assertFalse("Engine should not have yet attained state", attainedChild.isDone());

        car.open();

        assertTrue("Engine should have now attained state", attainedChild.isDone());
        assertEquals(engine, attainedChild.get());
    }

    public void testOpenAwaitsChildToAttainState() throws Exception
    {
        SettableFuture<Void> engineStateChangeControllingFuture = SettableFuture.create();

        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, car.getChildren(TestEngine.class).size());
        TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();
        engine.setStateChangeFuture(engineStateChangeControllingFuture);

        ListenableFuture carFuture = car.openAsync();
        assertFalse("car open future has completed before engine has attained state", carFuture.isDone());

        engineStateChangeControllingFuture.set(null);

        assertTrue("car open future has not completed", carFuture.isDone());
        carFuture.get();
    }

    public void testOpenAwaitsChildToAttainState_ChildStateChangeAsyncErrors() throws Exception
    {
        SettableFuture<Void> engineStateChangeControllingFuture = SettableFuture.create();

        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, car.getChildren(TestEngine.class).size());
        TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();
        engine.setStateChangeFuture(engineStateChangeControllingFuture);

        ListenableFuture carFuture = car.openAsync();
        assertFalse("car open future has completed before engine has attained state", carFuture.isDone());

        engineStateChangeControllingFuture.setException(new RuntimeException("child attain state exception"));

        assertTrue("car open future has not completed", carFuture.isDone());
        carFuture.get();

        assertEquals(State.ERRORED, engine.getState());
    }

    public void testOpenAwaitsChildToAttainState_ChildStateChangeSyncErrors() throws Exception
    {
        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, car.getChildren(TestEngine.class).size());
        TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();

        engine.setStateChangeException(new RuntimeException("child attain state exception"));

        ListenableFuture carFuture = car.openAsync();

        assertTrue("car open future has not completed", carFuture.isDone());
        carFuture.get();

        assertEquals(State.ERRORED, engine.getState());
    }

    public void testCreateAwaitsAttainState()
    {
        SettableFuture stateChangeFuture = SettableFuture.create();

        TestCar car = _model.getObjectFactory().create(TestCar.class, Collections.<String, Object>singletonMap(ConfiguredObject.NAME, "myCar"), null);

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(TestEngine.STATE_CHANGE_FUTURE, stateChangeFuture);

        ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertFalse("create child has completed before state change completes", engine.isDone());

        stateChangeFuture.set(null);

        assertTrue("create child has not completed", engine.isDone());
    }

    public void testCreateAwaitsAttainState_StateChangeAsyncErrors() throws Exception
    {
        SettableFuture stateChangeFuture = SettableFuture.create();
        RuntimeException stateChangeException = new RuntimeException("state change error");

        TestCar car = _model.getObjectFactory().create(TestCar.class, Collections.<String, Object>singletonMap(ConfiguredObject.NAME, "myCar"), null);

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(TestEngine.STATE_CHANGE_FUTURE, stateChangeFuture);

        ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertFalse("create child has completed before state change completes", engine.isDone());

        stateChangeFuture.setException(stateChangeException);

        assertTrue("create child has not completed", engine.isDone());
        try
        {
            engine.get();
            fail("Exception not thrown");
        }
        catch (ExecutionException ee)
        {
            assertSame(stateChangeException, ee.getCause());
        }
    }

    public void testCreateAwaitsAttainState_StateChangeSyncErrors() throws Exception
    {
        RuntimeException stateChangeException = new RuntimeException("state change error");

        TestCar car = _model.getObjectFactory().create(TestCar.class, Collections.<String, Object>singletonMap(ConfiguredObject.NAME, "myCar"), null);

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(TestEngine.STATE_CHANGE_EXCEPTION, stateChangeException);

        ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertTrue("create child has not completed", engine.isDone());

        try
        {
            engine.get();
            fail("Exception not thrown");
        }
        catch (ExecutionException ee)
        {
            assertSame(stateChangeException, ee.getCause());
        }
    }

    public void testCloseAwaitsChildCloseCompletion()
    {
        SettableFuture<Void> engineCloseControllingFuture = SettableFuture.create();

        TestCar car = _model.getObjectFactory().create(TestCar.class,
                                                       Collections.<String, Object>singletonMap(ConfiguredObject.NAME,
                                                                                                "myCar"), null);

        String engineName = "myEngine";
        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(TestEngine.BEFORE_CLOSE_FUTURE, engineCloseControllingFuture);

        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        ListenableFuture carListenableFuture = car.closeAsync();
        assertFalse("car close future has completed before engine closed", carListenableFuture.isDone());
        assertSame("engine deregistered from car too early", engine, car.getChildById(TestEngine.class, engine.getId()));

        engineCloseControllingFuture.set(null);

        assertTrue("car close future has not completed", carListenableFuture.isDone());
        assertNull("engine not deregistered", car.getChildById(TestEngine.class, engine.getId()));
    }

    public void testDefaultContextVariableWhichRefersToAncestor()
    {
        final String carName = "myCar";
        Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, carName);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

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

    public void testUserPreferencesCreatedOnEngineCreation()
    {
        Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, "myCar");
        carAttributes.put(ConfiguredObject.TYPE, TestStandardCarImpl.TEST_STANDARD_CAR_TYPE);

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, "myEngine");
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertNotNull("Unexpected user preferences", engine.getUserPreferences());
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

        TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);
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

    /** Simulates recovery of a parent/child from a store.  Neither will be open yet. */
    private TestCar recoverParentAndChild()
    {
        final SystemConfig mockSystemConfig = mock(SystemConfig.class);
        when(mockSystemConfig.getId()).thenReturn(UUID.randomUUID());
        when(mockSystemConfig.getModel()).thenReturn(TestModel.getInstance());

        final String carName = "myCar";
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, carName);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        ConfiguredObjectRecord carCor = new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return UUID.randomUUID();
            }

            @Override
            public String getType()
            {
                return TestCar.class.getSimpleName();
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return carAttributes;
            }

            @Override
            public Map<String, UUID> getParents()
            {
                return Collections.singletonMap(SystemConfig.class.getSimpleName(), mockSystemConfig.getId());
            }
        };

        final TestCar car = (TestCar) _model.getObjectFactory().recover(carCor, mockSystemConfig).resolve();

        String engineName = "myEngine";
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        ConfiguredObjectRecord engineCor = new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return UUID.randomUUID();
            }

            @Override
            public String getType()
            {
                return TestEngine.class.getSimpleName();
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return engineAttributes;
            }

            @Override
            public Map<String, UUID> getParents()
            {
                return Collections.singletonMap(TestCar.class.getSimpleName(), car.getId());
            }
        };

        _model.getObjectFactory().recover(engineCor, car).resolve();
        return car;
    }


}
