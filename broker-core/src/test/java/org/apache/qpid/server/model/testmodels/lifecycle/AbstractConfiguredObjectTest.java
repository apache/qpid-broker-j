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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractConfiguredObjectTest extends UnitTestBase
{

    @Test
    public void testOpeningResultsInErroredStateWhenResolutionFails() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnPostResolve(true);
        object.open();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.setThrowExceptionOnPostResolve(false);
        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.ACTIVE));
        assertTrue("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ACTIVE, object.getState());
    }

    @Test
    public void testOpeningResultsInErroredStateWhenActivationFails() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnActivate(true);
        object.open();
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.setThrowExceptionOnActivate(false);
        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.ACTIVE));
        assertEquals("Unexpected state", State.ACTIVE, object.getState());
    }

    @Test
    public void testOpeningInERROREDStateAfterFailedOpenOnDesiredStateChangeToActive() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.setThrowExceptionOnOpen(false);
        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.ACTIVE));
        assertTrue("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ACTIVE, object.getState());
    }

    @Test
    public void testOpeningInERROREDStateAfterFailedOpenOnStart() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.setThrowExceptionOnOpen(false);
        object.start();
        assertTrue("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ACTIVE, object.getState());
    }

    @Test
    public void testDeletionERROREDStateAfterFailedOpenOnDelete() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.delete();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.DELETED, object.getState());
    }

    @Test
    public void testDeletionInERROREDStateAfterFailedOpenOnDesiredStateChangeToDelete() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        object.open();
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ERRORED, object.getState());

        object.setAttributes(Collections.<String, Object>singletonMap(Port.DESIRED_STATE, State.DELETED));
        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.DELETED, object.getState());
    }


    @Test
    public void testCreationWithExceptionThrownFromValidationOnCreate() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnValidationOnCreate(true);
        try
        {
            object.create();
            fail("IllegalConfigurationException is expected to be thrown");
        }
        catch(IllegalConfigurationException e)
        {
            //pass
        }
        assertFalse("Unexpected opened", object.isOpened());
    }

    @Test
    public void testCreationWithoutExceptionThrownFromValidationOnCreate() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnValidationOnCreate(false);
        object.create();
        assertTrue("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.ACTIVE, object.getState());
    }

    @Test
    public void testCreationWithExceptionThrownFromOnOpen() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnOpen(true);
        try
        {
            object.create();
            fail("Exception should have been re-thrown");
        }
        catch (RuntimeException re)
        {
            // pass
        }

        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.DELETED, object.getState());
    }

    @Test
    public void testCreationWithExceptionThrownFromOnCreate() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnCreate(true);
        try
        {
            object.create();
            fail("Exception should have been re-thrown");
        }
        catch (RuntimeException re)
        {
            // pass
        }

        assertFalse("Unexpected opened", object.isOpened());
        assertEquals("Unexpected state", State.DELETED, object.getState());
    }

    @Test
    public void testCreationWithExceptionThrownFromActivate() throws Exception
    {
        TestConfiguredObject object = new TestConfiguredObject(getTestName());
        object.setThrowExceptionOnActivate(true);
        try
        {
            object.create();
            fail("Exception should have been re-thrown");
        }
        catch (RuntimeException re)
        {
            // pass
        }

        assertEquals("Unexpected state", State.DELETED, object.getState());
    }

    @Test
    public void testUnresolvedChildInERROREDStateIsNotValidatedOrOpenedOrAttainedDesiredStateOnParentOpen() throws Exception
    {
        TestConfiguredObject parent = new TestConfiguredObject("parent");
        TestConfiguredObject child1 = new TestConfiguredObject("child1", parent, parent.getTaskExecutor());
        child1.registerWithParents();
        TestConfiguredObject child2 = new TestConfiguredObject("child2", parent, parent.getTaskExecutor());
        child2.registerWithParents();

        child1.setThrowExceptionOnPostResolve(true);

        parent.open();

        assertTrue("Parent should be resolved", parent.isResolved());
        assertTrue("Parent should be validated", parent.isValidated());
        assertTrue("Parent should be opened", parent.isOpened());
        assertEquals("Unexpected parent state", State.ACTIVE, parent.getState());

        assertTrue("Child2 should be resolved", child2.isResolved());
        assertTrue("Child2 should be validated", child2.isValidated());
        assertTrue("Child2 should be opened", child2.isOpened());
        assertEquals("Unexpected child2 state", State.ACTIVE, child2.getState());

        assertFalse("Child2 should not be resolved", child1.isResolved());
        assertFalse("Child1 should not be validated", child1.isValidated());
        assertFalse("Child1 should not be opened", child1.isOpened());
        assertEquals("Unexpected child1 state", State.ERRORED, child1.getState());
    }

    @Test
    public void testUnvalidatedChildInERROREDStateIsNotOpenedOrAttainedDesiredStateOnParentOpen() throws Exception
    {
        TestConfiguredObject parent = new TestConfiguredObject("parent");
        TestConfiguredObject child1 = new TestConfiguredObject("child1", parent, parent.getTaskExecutor());
        child1.registerWithParents();
        TestConfiguredObject child2 = new TestConfiguredObject("child2", parent, parent.getTaskExecutor());
        child2.registerWithParents();

        child1.setThrowExceptionOnValidate(true);

        parent.open();

        assertTrue("Parent should be resolved", parent.isResolved());
        assertTrue("Parent should be validated", parent.isValidated());
        assertTrue("Parent should be opened", parent.isOpened());
        assertEquals("Unexpected parent state", State.ACTIVE, parent.getState());

        assertTrue("Child2 should be resolved", child2.isResolved());
        assertTrue("Child2 should be validated", child2.isValidated());
        assertTrue("Child2 should be opened", child2.isOpened());
        assertEquals("Unexpected child2 state", State.ACTIVE, child2.getState());

        assertTrue("Child1 should be resolved", child1.isResolved());
        assertFalse("Child1 should not be validated", child1.isValidated());
        assertFalse("Child1 should not be opened", child1.isOpened());
        assertEquals("Unexpected child1 state", State.ERRORED, child1.getState());
    }

    @Test
    public void testSuccessfulStateTransitionInvokesListener() throws Exception
    {
        TestConfiguredObject parent = new TestConfiguredObject("parent");
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
        assertEquals((long) 1, (long) callCounter.get());
    }

    // TODO - not sure if I want to keep the state transition methods on delete
    public void XtestUnsuccessfulStateTransitionDoesNotInvokesListener() throws Exception
    {
        final IllegalStateTransitionException expectedException =
                new IllegalStateTransitionException("This test fails the state transition.");
        TestConfiguredObject parent = new TestConfiguredObject("parent")
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

        try
        {
            parent.delete();
            fail("Exception not thrown.");
        }
        catch (RuntimeException e)
        {
            assertSame("State transition threw unexpected exception.", expectedException, e);
        }
        assertEquals((long) 0, (long) callCounter.get());
        assertEquals(State.ACTIVE, parent.getDesiredState());
        assertEquals(State.ACTIVE, parent.getState());
    }


    @Test
    public void testSuccessfulDeletion() throws Exception
    {
        TestConfiguredObject configuredObject = new TestConfiguredObject("configuredObject");
        configuredObject.create();

        final List<ChangeEvent> events = new ArrayList<>();
        configuredObject.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue)
            {
                events.add(new ChangeEvent(EventType.ATTRIBUTE_SET, object, attributeName, oldAttributeValue, newAttributeValue));
            }

            @Override
            public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
            {
                events.add(new ChangeEvent(EventType.STATE_CHANGED, object, ConfiguredObject.DESIRED_STATE, oldState, newState));
            }
        });

        configuredObject.delete();
        assertEquals((long) 2, (long) events.size());
        assertEquals(State.DELETED, configuredObject.getDesiredState());
        assertEquals(State.DELETED, configuredObject.getState());

        assertEquals("Unexpected events number", (long) 2, (long) events.size());
        ChangeEvent event0 = events.get(0);
        ChangeEvent event1 = events.get(1);

        assertEquals("Unexpected first event: " + event0,
                            new ChangeEvent(EventType.STATE_CHANGED, configuredObject, Exchange.DESIRED_STATE, State.ACTIVE, State.DELETED),
                            event0);

        assertEquals("Unexpected second event: " + event1,
                            new ChangeEvent(EventType.ATTRIBUTE_SET, configuredObject, Exchange.DESIRED_STATE, State.ACTIVE, State.DELETED),
                            event1);
    }

    private enum EventType
    {
        ATTRIBUTE_SET,
        STATE_CHANGED
    }

    private class ChangeEvent
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

            return (_source != null ? _source.equals(that._source) : that._source == null)
                    && (_attributeName != null ? _attributeName.equals(that._attributeName) : that._attributeName == null)
                    && (_oldValue != null ? _oldValue.equals(that._oldValue) : that._oldValue == null)
                    && (_newValue != null ? _newValue.equals(that._newValue) : that._newValue == null)
                    && _eventType == that._eventType;

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
