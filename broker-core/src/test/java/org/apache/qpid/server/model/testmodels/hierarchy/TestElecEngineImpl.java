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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

@ManagedObject( category = false, type = TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE)
public class TestElecEngineImpl
        extends TestAbstractEngineImpl<TestElecEngineImpl> implements TestElecEngine<TestElecEngineImpl>
{
    public static final String TEST_ELEC_ENGINE_TYPE = "ELEC";

    @ManagedAttributeField
    private TestSensor<?> _temperatureSensor;

    @ManagedObjectFactoryConstructor
    public TestElecEngineImpl(final Map<String, Object> attributes, TestCar<?> parent)
    {
        super(parent, attributes);
    }

    @SuppressWarnings("unused")
    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        final Collection<String> types = List.of(TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        return Map.of(TestEngine.class.getSimpleName(), types);
    }

    @Override
    public TestSensor<?> getTemperatureSensor()
    {
        return _temperatureSensor;
    }
}
