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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;

@ManagedObject( category = false,
        type = TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE)
public class TestTemperatureSensorImpl extends TestAbstractSensorImpl<TestTemperatureSensorImpl> implements TestSensor<TestTemperatureSensorImpl>
{

    public static final String TEST_TEMPERATURE_SENSOR_TYPE = "temperature";

    private final AtomicInteger _alertCount = new AtomicInteger();

    @ManagedObjectFactoryConstructor
    protected TestTemperatureSensorImpl(final Map<String, Object> attributes,final TestInstrumentPanel<?> parent)
    {
        super(parent, attributes);
    }

    @Override
    public int getNumberOfAlerts()
    {
        return _alertCount.getAndIncrement();
    }
}
