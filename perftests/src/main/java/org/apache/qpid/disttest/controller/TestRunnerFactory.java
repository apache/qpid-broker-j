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
package org.apache.qpid.disttest.controller;

import org.apache.qpid.disttest.controller.config.TestInstance;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;

import java.util.Map;

import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMB;

public class TestRunnerFactory
{
    public ITestRunner createTestRunner(ParticipatingClients participatingClients,
                                        TestInstance testInstance,
                                        ControllerJmsDelegate jmsDelegate,
                                        long commandResponseTimeout,
                                        long testResultTimeout,
                                        final Map<String, String> options)
    {

        if (Boolean.valueOf(options.get(HILL_CLIMB)))
        {
            return new HillClimbingTestRunner(participatingClients,
                                              testInstance,
                                              jmsDelegate,
                                              commandResponseTimeout,
                                              testResultTimeout, options);
        }
        else
        {
            return new OrdinaryTestRunner(participatingClients,
                                          testInstance,
                                          jmsDelegate,
                                          commandResponseTimeout,
                                          testResultTimeout);
        }
    }
}
