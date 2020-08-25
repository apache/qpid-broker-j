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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.ManagedStatistic;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;

@ManagedObject( defaultType = TestStandardCarImpl.TEST_STANDARD_CAR_TYPE)
public interface TestCar<X extends TestCar<X>> extends ConfiguredObject<X>
{
    enum Colour { BLACK, RED, BLUE, GREY };

    String TEST_CONTEXT_VAR = "TEST_CONTEXT_VAR";
    @ManagedContextDefault(name = TEST_CONTEXT_VAR)
    String testContextVar = "a value";

    String TEST_CONTEXT_VAR_WITH_ANCESTOR_REF = "TEST_CONTEXT_VAR_WITH_ANCESTOR_REF";
    @ManagedContextDefault(name = TEST_CONTEXT_VAR_WITH_ANCESTOR_REF)
    String testContextVarWithAncestorRef = "a value ${ancestor:testcar:name}";

    String TEST_CONTEXT_VAR_WITH_THIS_REF = "TEST_CONTEXT_VAR_WITH_THIS_REF";
    @ManagedContextDefault(name = TEST_CONTEXT_VAR_WITH_THIS_REF)
    String testContextVarWithThisRef = "a value ${this:name}";

    @ManagedAttribute
    Colour getBodyColour();


    @ManagedAttribute(validValues = {"GREY", "BLACK"})
    Colour getInteriorColour();

    enum Door { DRIVER, PASSENGER }

    Door openDoor(@Param(name = "door") Door door);

    @ManagedOperation(changesConfiguredObjectState = false)
    void startEngine(@Param(name = "keyCode", mandatory = true) String keyCode);

    void setRejectStateChange(boolean b);

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.COUNT)
    int getMileage();

    @ManagedStatistic(metricName = "age",
            statisticType = StatisticType.CUMULATIVE,
            units = StatisticUnit.TIME_DURATION,
            metricDisabled = true)
    int getAge();

    @ManagedOperation(changesConfiguredObjectState = false)
    int move(@Param(name = "mileage", mandatory = true) int mileage);
}
