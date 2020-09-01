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

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedStatistic;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;

@ManagedObject(category = true, defaultType = TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE)
public interface TestEngine<X extends TestEngine<X>> extends ConfiguredObject<X>
{
    String BEFORE_CLOSE_FUTURE = "beforeCloseFuture";
    String STATE_CHANGE_FUTURE = "stateChangeFuture";
    String STATE_CHANGE_EXCEPTION = "stateChangeException";

    /* Injectable close future, used to control when/how close completes during the test */
    @ManagedAttribute
    Object getBeforeCloseFuture();

    void setBeforeCloseFuture(ListenableFuture<Void> listenableFuture);

    /* Injectable state change future, used to control when/how asynch state transition completes during the test */

    @ManagedAttribute
    Object getStateChangeFuture();

    void setStateChangeFuture(ListenableFuture<Void> listenableFuture);

    /* Injectable exception, used to introduce an exception into a state change transition */

    @ManagedAttribute
    Object getStateChangeException();
    void setStateChangeException(RuntimeException exception);

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT)
    int getTemperature();

}
