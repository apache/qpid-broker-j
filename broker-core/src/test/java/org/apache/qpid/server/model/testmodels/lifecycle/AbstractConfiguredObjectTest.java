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
package org.apache.qpid.server.model.testmodels.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("unchecked")
public class AbstractConfiguredObjectTest extends UnitTestBase
{
    @Test
    public void testOpeningResultsInErroredStateWhenResolutionFails()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnPostResolve(true);
        object.open();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.setThrowExceptionOnPostResolve(false);
        object.setAttributes(Map.of(Port.DESIRED_STATE, State.ACTIVE));
        assertTrue(object.isOpened(), "Unexpected opened");
        assertEquals(State.ACTIVE, object.getState(), "Unexpected state");
    }

    @Test
    public void testOpeningResultsInErroredStateWhenActivationFails()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnActivate(true);
        object.open();
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.setThrowExceptionOnActivate(false);
        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.ACTIVE));
        assertEquals(State.ACTIVE, object.getState(), "Unexpected state");
    }

    @Test
    public void testOpeningInERROREDStateAfterFailedOpenOnDesiredStateChangeToActive()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.setThrowExceptionOnOpen(false);
        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.ACTIVE));
        assertTrue(object.isOpened(), "Unexpected opened");
        assertEquals(State.ACTIVE, object.getState(), "Unexpected state");
    }

    @Test
    public void testOpeningInERROREDStateAfterFailedOpenOnStart()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.setThrowExceptionOnOpen(false);
        object.start();
        assertTrue(object.isOpened(), "Unexpected opened");
        assertEquals(State.ACTIVE, object.getState(), "Unexpected state");
    }

    @Test
    public void testDeletionERROREDStateAfterFailedOpenOnDelete()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.delete();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.DELETED, object.getState(), "Unexpected state");
    }

    @Test
    public void testDeletionInERROREDStateAfterFailedOpenOnDesiredStateChangeToDelete()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.ERRORED, object.getState(), "Unexpected state");

        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.DELETED));
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.DELETED, object.getState(), "Unexpected state");
    }

    @Test
    public void testCreationWithExceptionThrownFromValidationOnCreate()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnValidationOnCreate(true);
        assertThrows(IllegalConfigurationException.class, object::create, "IllegalConfigurationException is expected to be thrown");
        assertFalse(object.isOpened(), "Unexpected opened");
    }

    @Test
    public void testCreationWithoutExceptionThrownFromValidationOnCreate()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnValidationOnCreate(false);
        object.create();
        assertTrue(object.isOpened(), "Unexpected opened");
        assertEquals(State.ACTIVE, object.getState(), "Unexpected state");
    }

    @Test
    public void testCreationWithExceptionThrownFromOnOpen()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        assertThrows(IllegalConfigurationException.class, object::create, "Exception should have been re-thrown");
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.DELETED, object.getState(), "Unexpected state");
    }

    @Test
    public void testCreationWithExceptionThrownFromOnCreate()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnCreate(true);
        assertThrows(IllegalConfigurationException.class, object::create, "Exception should have been re-thrown");
        assertFalse(object.isOpened(), "Unexpected opened");
        assertEquals(State.DELETED, object.getState(), "Unexpected state");
    }

    @Test
    public void testCreationWithExceptionThrownFromActivate()
    {
        final TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnActivate(true);
        assertThrows(IllegalConfigurationException.class, object::create, "Exception should have been re-thrown");
        assertEquals(State.DELETED, object.getState(), "Unexpected state");
    }

    @Test
    public void testUnresolvedChildInERROREDStateIsNotValidatedOrOpenedOrAttainedDesiredStateOnParentOpen()
    {
        final TestConfiguredObject parent = new TestConfiguredObject("parent");
        final TestConfiguredObject child1 = new TestConfiguredObject("child1", parent, parent.getTaskExecutor());
        child1.registerWithParents();
        final TestConfiguredObject child2 = new TestConfiguredObject("child2", parent, parent.getTaskExecutor());
        child2.registerWithParents();

        child1.setThrowExceptionOnPostResolve(true);

        parent.open();

        assertTrue(parent.isResolved(), "Parent should be resolved");
        assertTrue(parent.isValidated(), "Parent should be validated");
        assertTrue(parent.isOpened(), "Parent should be opened");
        assertEquals(State.ACTIVE, parent.getState(), "Unexpected parent state");

        assertTrue(child2.isResolved(), "Child2 should be resolved");
        assertTrue(child2.isValidated(), "Child2 should be validated");
        assertTrue(child2.isOpened(), "Child2 should be opened");
        assertEquals(State.ACTIVE, child2.getState(), "Unexpected child2 state");

        assertFalse(child1.isResolved(), "Child2 should not be resolved");
        assertFalse(child1.isValidated(), "Child1 should not be validated");
        assertFalse(child1.isOpened(), "Child1 should not be opened");
        assertEquals(State.ERRORED, child1.getState(), "Unexpected child1 state");
    }

    @Test
    public void testUnvalidatedChildInERROREDStateIsNotOpenedOrAttainedDesiredStateOnParentOpen()
    {
        final TestConfiguredObject parent = new TestConfiguredObject("parent");
        final TestConfiguredObject child1 = new TestConfiguredObject("child1", parent, parent.getTaskExecutor());
        child1.registerWithParents();
        final TestConfiguredObject child2 = new TestConfiguredObject("child2", parent, parent.getTaskExecutor());
        child2.registerWithParents();

        child1.setThrowExceptionOnValidate(true);

        parent.open();

        assertTrue(parent.isResolved(), "Parent should be resolved");
        assertTrue(parent.isValidated(), "Parent should be validated");
        assertTrue(parent.isOpened(), "Parent should be opened");
        assertEquals(State.ACTIVE, parent.getState(), "Unexpected parent state");

        assertTrue(child2.isResolved(), "Child2 should be resolved");
        assertTrue(child2.isValidated(), "Child2 should be validated");
        assertTrue(child2.isOpened(), "Child2 should be opened");
        assertEquals(State.ACTIVE, child2.getState(), "Unexpected child2 state");

        assertTrue(child1.isResolved(), "Child1 should be resolved");
        assertFalse(child1.isValidated(), "Child1 should not be validated");
        assertFalse(child1.isOpened(), "Child1 should not be opened");
        assertEquals(State.ERRORED, child1.getState(), "Unexpected child1 state");
    }

    @Test
    public void testSuccessfulStateTransitionInvokesListener()
    {
        final TestConfiguredObject parent = new TestConfiguredObject("parent");
        parent.create();

        final AtomicReference<State> newState = new AtomicReference<>();
        final AtomicInteger callCounter = new AtomicInteger();
        parent.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State old, final State state)
            {
                super.stateChanged(object, old, state);
                callCounter.incrementAndGet();
                newState.set(state);
            }
        });

        parent.delete();
        assertEquals(State.DELETED, newState.get());
        assertEquals(1, (long) callCounter.get());
    }

    // TODO - not sure if I want to keep the state transition methods on delete
    public void XtestUnsuccessfulStateTransitionDoesNotInvokesListener()
    {
        final IllegalStateTransitionException expectedException =
                new IllegalStateTransitionException("This test fails the state transition.");
        final TestConfiguredObject parent = new TestConfiguredObject("parent")
        {
            @Override
            protected ListenableFuture<Void> doDelete()
            {
                throw expectedException;
            }
        };
        parent.create();

        final AtomicInteger callCounter = new AtomicInteger();
        parent.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State old, final State state)
            {
                super.stateChanged(object, old, state);
                callCounter.incrementAndGet();
            }

            @Override
            public void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue)
            {
                super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
                callCounter.incrementAndGet();
            }
        });

        final RuntimeException thrown = assertThrows(RuntimeException.class, parent::delete, "Exception not thrown");
        assertSame(expectedException, thrown, "State transition threw unexpected exception");
        assertEquals(0, (long) callCounter.get());
        assertEquals(State.ACTIVE, parent.getDesiredState());
        assertEquals(State.ACTIVE, parent.getState());
    }


    @Test
    public void testSuccessfulDeletion()
    {
        final TestConfiguredObject configuredObject = new TestConfiguredObject("configuredObject");
        configuredObject.create();

        final List<ChangeEvent> events = new ArrayList<>();
        configuredObject.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue)
            {
                events.add(new ChangeEvent(EventType.ATTRIBUTE_SET,
                                           object,
                                           attributeName,
                                           oldAttributeValue,
                                           newAttributeValue));
            }

            @Override
            public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
            {
                events.add(new ChangeEvent(EventType.STATE_CHANGED,
                                           object,
                                           ConfiguredObject.DESIRED_STATE,
                                           oldState,
                                           newState));
            }
        });

        configuredObject.delete();
        assertEquals(2, (long) events.size());
        assertEquals(State.DELETED, configuredObject.getDesiredState());
        assertEquals(State.DELETED, configuredObject.getState());

        assertEquals(2, (long) events.size(), "Unexpected events number");
        final ChangeEvent event0 = events.get(0);
        final ChangeEvent event1 = events.get(1);

        assertEquals(new ChangeEvent(EventType.STATE_CHANGED, configuredObject, Exchange.DESIRED_STATE, State.ACTIVE,
                State.DELETED), event0, "Unexpected first event: " + event0);

        assertEquals(new ChangeEvent(EventType.ATTRIBUTE_SET, configuredObject, Exchange.DESIRED_STATE, State.ACTIVE,
                State.DELETED), event1, "Unexpected second event: " + event1);
    }

    private enum EventType
    {
        ATTRIBUTE_SET,
        STATE_CHANGED
    }

    private static class ChangeEvent
    {
        private final ConfiguredObject<?> _source;
        private final String _attributeName;
        private final Object _oldValue;
        private final Object _newValue;
        private final EventType _eventType;

        public ChangeEvent(EventType eventType, ConfiguredObject<?> source, String attributeName, Object oldValue, Object newValue)
        {
            _source = source;
            _attributeName = attributeName;
            _oldValue = oldValue;
            _newValue = newValue;
            _eventType = eventType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChangeEvent that = (ChangeEvent) o;

            return (Objects.equals(_source, that._source)) &&
                   (Objects.equals(_attributeName, that._attributeName)) &&
                   (Objects.equals(_oldValue, that._oldValue)) &&
                   (Objects.equals(_newValue, that._newValue)) &&
                   _eventType == that._eventType;

        }

        @Override
        public String toString()
        {
            return "ChangeEvent{" +
                    "_source=" + _source +
                    ", _attributeName='" + _attributeName + '\'' +
                    ", _oldValue=" + _oldValue +
                    ", _newValue=" + _newValue +
                    ", _eventType=" + _eventType +
                    '}';
        }
    }
}
