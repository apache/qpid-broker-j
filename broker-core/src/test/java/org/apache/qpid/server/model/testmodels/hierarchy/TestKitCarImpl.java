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

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

@ManagedObject( category = false,
                type = TestKitCarImpl.TEST_KITCAR_TYPE)
public class TestKitCarImpl extends AbstractConfiguredObject<TestKitCarImpl>
        implements TestKitCar<TestKitCarImpl>
{
    public static final String TEST_KITCAR_TYPE = "testkitcar";

    @ManagedAttributeField
    private Colour _bodyColour;

    @ManagedAttributeField
    private Colour _interiorColour;

    @ManagedAttributeField
    private Map<String,Object> _parameters;

    @ManagedAttributeField
    private TestEngine _alternateEngine;

    @ManagedObjectFactoryConstructor
    public TestKitCarImpl(final Map<String, Object> attributes)
    {
        super(null, attributes, newTaskExecutor(), TestModel.getInstance());
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                             final Map<String, Object> attributes)
    {
        return getObjectFactory().createAsync(childClass, attributes, this);
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    private static CurrentThreadTaskExecutor newTaskExecutor()
    {
        CurrentThreadTaskExecutor currentThreadTaskExecutor = new CurrentThreadTaskExecutor();
        currentThreadTaskExecutor.start();
        return currentThreadTaskExecutor;
    }

    @Override
    public Door openDoor(final Door door)
    {
        return door;
    }

    @Override
    public void startEngine(final String keyCode)
    {
    }

    @Override
    public Map<String, Object> getParameters()
    {
        return _parameters;
    }

    @Override
    public TestEngine getAlternateEngine()
    {
        return _alternateEngine;
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

}
