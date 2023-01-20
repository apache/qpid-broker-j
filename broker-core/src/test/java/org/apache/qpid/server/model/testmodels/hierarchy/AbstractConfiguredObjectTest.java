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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.test.utils.UnitTestBase;


/**
 * Tests behaviour of AbstractConfiguredObjects when hierarchies of objects are used together.
 * Responsibilities to include adding/removing of children and correct firing of listeners.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractConfiguredObjectTest extends UnitTestBase
{
    private final Model _model = TestModel.getInstance();

    @Test
    public void testCreateCategoryDefault()
    {
        final String objectName = "testCreateCategoryDefault";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName);
        final TestCar object = _model.getObjectFactory().create(TestCar.class, attributes, null);
        assertEquals(objectName, object.getName());
        assertEquals(TestStandardCarImpl.TEST_STANDARD_CAR_TYPE, object.getType());
        final boolean condition = object instanceof TestStandardCar;
        assertTrue(condition);
    }

    @Test
    public void testCreateUnrecognisedType()
    {
        final String objectName = "testCreateCategoryDefault";
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, objectName,
                ConfiguredObject.TYPE, "notatype");
        assertThrows(IllegalConfigurationException.class,
                () -> _model.getObjectFactory().create(TestCar.class, attributes, null),
                "Exception not thrown");
    }

    @Test
    public void testCreateCarWithEngine()
    {
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);
        
        assertEquals(carName, car.getName());
        assertEquals(0, (long) car.getChildren(TestEngine.class).size());

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertEquals(1, (long) car.getChildren(TestEngine.class).size());
        assertEquals(engineName, engine.getName());
        assertEquals(TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE, engine.getType());
    }

    @Test
    public void testGetChildren_NewChild()
    {
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "myCar"), null);
        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        // Check we can observe the new child from the parent
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());
        assertEquals(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertEquals(engine, car.getChildByName(TestEngine.class, engine.getName()));

        final ListenableFuture attainedChild = car.getAttainedChildByName(TestEngine.class, engine.getName());
        assertNotNull(attainedChild);
        assertTrue(attainedChild.isDone(), "Engine should have already attained state");
    }

    @Test
    public void testGetChildren_RecoveredChild() throws Exception
    {
        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());

        final TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();

        assertEquals(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertEquals(engine, car.getChildByName(TestEngine.class, engine.getName()));

        final ListenableFuture attainedChild = car.getAttainedChildByName(TestEngine.class, engine.getName());
        assertNotNull(attainedChild);
        assertFalse(attainedChild.isDone(), "Engine should not have yet attained state");

        car.open();

        assertTrue(attainedChild.isDone(), "Engine should have now attained state");
        assertEquals(engine, attainedChild.get());
    }

    @Test
    public void testOpenAwaitsChildToAttainState() throws Exception
    {
        final SettableFuture<Void> engineStateChangeControllingFuture = SettableFuture.create();
        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());
        final TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();
        engine.setStateChangeFuture(engineStateChangeControllingFuture);

        final ListenableFuture carFuture = car.openAsync();
        assertFalse(carFuture.isDone(), "car open future has completed before engine has attained state");

        engineStateChangeControllingFuture.set(null);

        assertTrue(carFuture.isDone(), "car open future has not completed");
        carFuture.get();
    }

    @Test
    public void testOpenAwaitsChildToAttainState_ChildStateChangeAsyncErrors() throws Exception
    {
        final SettableFuture<Void> engineStateChangeControllingFuture = SettableFuture.create();

        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());
        final TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();
        engine.setStateChangeFuture(engineStateChangeControllingFuture);

        final ListenableFuture carFuture = car.openAsync();
        assertFalse(carFuture.isDone(), "car open future has completed before engine has attained state");

        engineStateChangeControllingFuture.setException(new RuntimeException("child attain state exception"));

        assertTrue(carFuture.isDone(), "car open future has not completed");
        carFuture.get();

        assertEquals(State.ERRORED, engine.getState());
    }

    @Test
    public void testOpenAwaitsChildToAttainState_ChildStateChangeSyncErrors() throws Exception
    {
        final TestCar car = recoverParentAndChild();

        // Check we can observe the recovered child from the parent
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());
        final TestEngine engine = (TestEngine) car.getChildren(TestEngine.class).iterator().next();

        engine.setStateChangeException(new RuntimeException("child attain state exception"));

        final ListenableFuture carFuture = car.openAsync();

        assertTrue(carFuture.isDone(), "car open future has not completed");
        carFuture.get();

        assertEquals(State.ERRORED, engine.getState());
    }

    @Test
    public void testCreateAwaitsAttainState()
    {
        final SettableFuture<Void> stateChangeFuture = SettableFuture.create();
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "myCar"), null);
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "myEngine",
                TestEngine.STATE_CHANGE_FUTURE, stateChangeFuture);
        final ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertFalse(engine.isDone(), "create child has completed before state change completes");

        stateChangeFuture.set(null);

        assertTrue(engine.isDone(), "create child has not completed");
    }

    @Test
    public void testCreateAwaitsAttainState_StateChangeAsyncErrors()
    {
        final SettableFuture stateChangeFuture = SettableFuture.create();
        final RuntimeException stateChangeException = new RuntimeException("state change error");
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "myCar"), null);
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "myEngine",
                TestEngine.STATE_CHANGE_FUTURE, stateChangeFuture);
        final ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertFalse(engine.isDone(), "create child has completed before state change completes");

        stateChangeFuture.setException(stateChangeException);

        assertTrue(engine.isDone(), "create child has not completed");
        final ExecutionException ee = assertThrows(ExecutionException.class, engine::get, "Exception not thrown");
        assertSame(stateChangeException, ee.getCause());
        assertEquals(0, (long) car.getChildren(TestEngine.class).size(),
                     "Failed engine should not be registered with parent");
    }

    @Test
    public void testCreateAwaitsAttainState_StateChangeSyncErrors()
    {
        final RuntimeException stateChangeException = new RuntimeException("state change error");
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "myCar"), null);
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "myEngine",
                TestEngine.STATE_CHANGE_EXCEPTION, stateChangeException);
        final ListenableFuture engine = car.createChildAsync(TestEngine.class, engineAttributes);
        assertTrue(engine.isDone(), "create child has not completed");
        final ExecutionException ee = assertThrows(ExecutionException.class, engine::get, "Exception not thrown");
        assertSame(stateChangeException, ee.getCause());
        assertEquals(0, (long) car.getChildren(TestEngine.class).size(),
                     "Failed engine should not be registered with parent");
    }

    @Test
    public void testCloseAwaitsChildCloseCompletion()
    {
        final SettableFuture<Void> engineCloseControllingFuture = SettableFuture.create();
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "myCar"), null);
        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                TestEngine.BEFORE_CLOSE_FUTURE, engineCloseControllingFuture);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);
        final ListenableFuture carListenableFuture = car.closeAsync();
        assertFalse(carListenableFuture.isDone(), "car close future has completed before engine closed");
        assertSame(engine, car.getChildById(TestEngine.class, engine.getId()),
                   "engine deregistered from car too early");


        engineCloseControllingFuture.set(null);

        assertTrue(carListenableFuture.isDone(), "car close future has not completed");
        assertNull(car.getChildById(TestEngine.class, engine.getId()), "engine not deregistered");
    }

    @Test
    public void testGlobalContextDefault()
    {
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        assertTrue(car.getContextKeys(true).contains(TestCar.TEST_CONTEXT_VAR),
                "context var not in contextKeys");

        final String expected = "a value";
        assertEquals(expected, car.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR),
                "Context variable has unexpected value");

    }

    @Test
    public void testGlobalContextDefaultWithThisRef()
    {
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        assertEquals("a value myCar", car.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR_WITH_THIS_REF),
                     "Context variable has unexpected value");

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertEquals("a value myEngine", engine.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR_WITH_THIS_REF),
                "Context variable has unexpected value");
    }

    @Test
    public void testHierarchyContextVariableWithThisRef()
    {
        final String contentVarName = "contentVar";
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE,
                ConfiguredObject.CONTEXT, Map.of(contentVarName, "name ${this:name}"));
        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        assertEquals("name myCar", car.getContextValue(String.class, contentVarName),
                     "Context variable has unexpected value");

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        // TODO: we have different behaviour depending on whether the variable is a  global context default or hierarchy context variable.
        assertEquals("name myCar", engine.getContextValue(String.class, contentVarName),
                "Context variable has unexpected value");
    }

    @Test
    public void testGlobalContextDefaultWithAncestorRef()
    {
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);
        final String expected = "a value " + carName;
        assertEquals(expected, car.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR_WITH_ANCESTOR_REF),
                                "Context variable has unexpected value");

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertEquals(expected, engine.getContextValue(String.class, TestCar.TEST_CONTEXT_VAR_WITH_ANCESTOR_REF),
                "Context variable has unexpected value");
    }

    @Test
    public void testHierarchyContextVariableWithAncestorRef()
    {
        final String contentVarName = "contentVar";
        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE,
                ConfiguredObject.CONTEXT, Map.of(contentVarName, "name ${ancestor:testcar:name}"));

        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        assertEquals("name myCar", car.getContextValue(String.class, contentVarName),
                     "Context variable has unexpected value");

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertEquals("name myCar", engine.getContextValue(String.class, contentVarName),
                "Context variable has unexpected value");
    }

    @Test
    public void testUserPreferencesCreatedOnEngineCreation()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "myCar",
                ConfiguredObject.TYPE, TestStandardCarImpl.TEST_STANDARD_CAR_TYPE);

        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "myEngine",
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);

        assertNotNull(engine.getUserPreferences(), "Unexpected user preferences");
    }

    @Test
    public void testDuplicateChildRejected_Name()
    {
        doDuplicateChildCheck(ConfiguredObject.NAME);
    }

    @Test
    public void testDuplicateChildRejected_Id()
    {
        doDuplicateChildCheck(ConfiguredObject.ID);
    }

    @Test
    public void testParentDeletePropagatesToChild()
    {
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "car"), null);

        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, Map.of(ConfiguredObject.NAME, "engine"));

        final StateChangeCapturingListener listener = new StateChangeCapturingListener();
        engine.addChangeListener(listener);

        assertEquals(State.ACTIVE, engine.getState(), "Unexpected child state before parent delete");

        car.delete();

        assertEquals(State.DELETED, engine.getState(), "Unexpected child state after parent delete");
        final List<State> newStates = listener.getNewStates();
        assertEquals(1, (long) newStates.size(), "Child heard an unexpected number of state chagnes");
        assertEquals(State.DELETED, newStates.get(0), "Child heard listener has unexpected state");
    }

    @Test
    public void testParentDeleteValidationFailureLeavesChildreIntact()
    {
        final TestCar car = _model.getObjectFactory().create(TestCar.class, Map.of(ConfiguredObject.NAME, "car"), null);

        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, Map.of(ConfiguredObject.NAME, "engine"));

        final StateChangeCapturingListener listener = new StateChangeCapturingListener();
        engine.addChangeListener(listener);

        assertEquals(State.ACTIVE, engine.getState(), "Unexpected child state before parent delete");

        car.setRejectStateChange(true);

        assertThrows(IllegalConfigurationException.class, car::delete, "Exception not thrown");

        assertEquals(State.ACTIVE, engine.getState(), "Unexpected child state after failed parent deletion");
        final List<State> newStates = listener.getNewStates();
        assertEquals(0, (long) newStates.size(), "Child heard an unexpected number of state changes");
    }

    @Test
    public void testDeleteConfiguredObjectReferredFromAttribute()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "car",
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car1 = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> instrumentPanelAttributes = Map.of(ConfiguredObject.NAME, "instrumentPanel",
                ConfiguredObject.TYPE, TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel instrumentPanel = (TestInstrumentPanel) car1.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        final Map<String, Object> sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "engine",
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE,
                "temperatureSensor", sensor.getName());
        car1.createChild(TestEngine.class, engineAttributes);

        assertThrows(IntegrityViolationException.class, sensor::delete, "Referred sensor cannot be deleted");
    }

    @Test
    public void testDeleteConfiguredObjectReferredFromCollection()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "car",
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car1 = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> instrumentPanelAttributes = Map.of(ConfiguredObject.NAME, "instrumentPanel",
                ConfiguredObject.TYPE, TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel instrumentPanel = (TestInstrumentPanel) car1.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        Map<String, Object> sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor1",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor1 = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor2",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor2 = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "engine",
                ConfiguredObject.TYPE, TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE,
                "temperatureSensors", new String[]{sensor1.getName(), sensor2.getName()});
        car1.createChild(TestEngine.class, engineAttributes);

        assertThrows(IntegrityViolationException.class, sensor1::delete, "Referred sensor cannot be deleted");
    }

    @Test
    public void testDeleteConfiguredObject()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "car",
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car1 = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> instrumentPanelAttributes = Map.of(ConfiguredObject.NAME, "instrumentPanel",
                ConfiguredObject.TYPE, TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel instrumentPanel = (TestInstrumentPanel) car1.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        final Map<String, Object> sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor1",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor1 = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        assertEquals(1, (long) instrumentPanel.getChildren(TestSensor.class).size(),
                "Unexpected number of sensors after creation");

        sensor1.delete();

        assertEquals(0, (long) instrumentPanel.getChildren(TestSensor.class).size(),
                "Unexpected number of sensors after deletion");
    }

    @Test
    public void testDeleteConfiguredObjectWithReferredChildren()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "car",
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car1 = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> instrumentPanelAttributes = Map.of(ConfiguredObject.NAME, "instrumentPanel",
                ConfiguredObject.TYPE, TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel instrumentPanel = (TestInstrumentPanel) car1.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        final Map<String, Object> sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, "engine",
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE,
                "temperatureSensor", sensor.getName());
        car1.createChild(TestEngine.class, engineAttributes);

        assertThrows(IntegrityViolationException.class, instrumentPanel::delete,
                "Instrument panel cannot be deleted as it has referenced children");
    }

    @Test
    public void testDeleteConfiguredObjectWithChildrenReferringEachOther()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, "car",
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        final TestCar car1 = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        final Map<String, Object> instrumentPanelAttributes = Map.of(ConfiguredObject.NAME, "instrumentPanel",
                ConfiguredObject.TYPE, TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel instrumentPanel = (TestInstrumentPanel) car1.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        final Map<String, Object> sensorAttributes = Map.of(ConfiguredObject.NAME, "sensor",
                ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        final TestSensor sensor = (TestSensor) instrumentPanel.createChild(TestSensor.class, sensorAttributes);

        final Map<String, Object> gaugeAttributes = Map.of(ConfiguredObject.NAME, "gauge",
                ConfiguredObject.TYPE, TestTemperatureGaugeImpl.TEST_TEMPERATURE_GAUGE_TYPE,
                "sensor", "sensor");
        final TestGauge gauge = (TestGauge) instrumentPanel.createChild(TestGauge.class, gaugeAttributes);

        instrumentPanel.delete();

        assertEquals(State.DELETED, sensor.getState(), "Unexpected sensor state");
        assertEquals(State.DELETED, gauge.getState(), "Unexpected gauge state");
    }

    private void doDuplicateChildCheck(final String attrToDuplicate)
    {
        final String carName = "myCar";

        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        final TestCar car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);
        assertEquals(0, (long) car.getChildren(TestEngine.class).size());

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        final TestEngine engine = (TestEngine) car.createChild(TestEngine.class, engineAttributes);
        assertEquals(1, (long) car.getChildren(TestEngine.class).size());

        final Map<String, Object> secondEngineNameAttribute = new HashMap<>();
        secondEngineNameAttribute.put(ConfiguredObject.TYPE, TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE);

        if (attrToDuplicate.equals(ConfiguredObject.NAME))
        {
            final String secondEngineName = "myEngine";
            secondEngineNameAttribute.put(ConfiguredObject.NAME, secondEngineName);
        }
        else
        {
            final String secondEngineName = "myPetrolEngine";
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

        assertEquals(1, (long) car.getChildren(TestEngine.class).size(),
                "Unexpected number of children after rejected duplicate");
        assertSame(engine, car.getChildById(TestEngine.class, engine.getId()));
        assertSame(engine, car.getChildByName(TestEngine.class, engine.getName()));
    }

    /** Simulates recovery of a parent/child from a store.  Neither will be open yet. */
    private TestCar recoverParentAndChild()
    {
        final SystemConfig mockSystemConfig = mock(SystemConfig.class);
        when(mockSystemConfig.getId()).thenReturn(randomUUID());
        when(mockSystemConfig.getModel()).thenReturn(TestModel.getInstance());

        final String carName = "myCar";
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, carName,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        final ConfiguredObjectRecord carCor = new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return randomUUID();
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
                return Map.of(SystemConfig.class.getSimpleName(), mockSystemConfig.getId());
            }
        };

        final TestCar car = (TestCar) _model.getObjectFactory().recover(carCor, mockSystemConfig).resolve();

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        final ConfiguredObjectRecord engineCor = new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return randomUUID();
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
                return Map.of(TestCar.class.getSimpleName(), car.getId());
            }
        };

        _model.getObjectFactory().recover(engineCor, car).resolve();
        return car;
    }


    private static class StateChangeCapturingListener extends AbstractConfigurationChangeListener
    {
        private final List<State> _newStates = new LinkedList<>();

        @Override
        public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
        {
            super.stateChanged(object, oldState, newState);
            _newStates.add(newState);
        }

        public List<State> getNewStates()
        {
            return new LinkedList<>(_newStates);
        }
    }
}
