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

package org.apache.qpid.server.model.testmodels.hierarchy;

import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Model;

public abstract class TestAbstractCarImpl<X extends TestAbstractCarImpl<X>> extends AbstractConfiguredObject<X> implements TestCar<X>
{
    @ManagedAttributeField
    private Colour _bodyColour;
    @ManagedAttributeField
    private Colour _interiorColour;

    private volatile boolean _rejectStateChange;

    public TestAbstractCarImpl(final Map<String, Object> attributes)
    {
        super(null, attributes, newTaskExecutor(), TestModel.getInstance());
    }

    public TestAbstractCarImpl(final Map<String, Object> attributes, Model model)
    {
        super(null, attributes, newTaskExecutor(), model);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        if (changedAttributes.contains(DESIRED_STATE) && _rejectStateChange)
        {
            throw new IllegalConfigurationException("This object is rejecting state changes just now, please"
                                                    + " try again later.");
        }
    }

    @Override
    public Colour getBodyColour()
    {
        return _bodyColour;
    }

    @Override
    public Colour getInteriorColour()
    {
        return _interiorColour;
    }

    @Override
    public void startEngine(final String keyCode)
    {
    }

    @Override
    public Door openDoor(final Door door)
    {
        return door;
    }

    private static CurrentThreadTaskExecutor newTaskExecutor()
    {
        CurrentThreadTaskExecutor currentThreadTaskExecutor = new CurrentThreadTaskExecutor();
        currentThreadTaskExecutor.start();
        return currentThreadTaskExecutor;
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    @Override
    public void setRejectStateChange(final boolean rejectStateChange)
    {
        _rejectStateChange = rejectStateChange;
    }
}
