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
package org.apache.qpid.server.management.plugin.controller.v7_0;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementControllerFactory;
import org.apache.qpid.server.management.plugin.controller.v7_0.LegacyManagementController;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class LegacyManagementControllerFactory_v7_1 implements ManagementControllerFactory
{
    public static final String MODEL_VERSION = "7.1";

    @Override
    public String getType()
    {
        return "org.apache.qpid.server.management.plugin.model.v7_1";
    }

    @Override
    public String getVersion()
    {
        return MODEL_VERSION;
    }

    @Override
    public String getPreviousVersion()
    {
        return "7.0";
    }

    @Override
    public ManagementController createManagementController(final HttpManagementConfiguration<?> httpManagement,
                                                           final ManagementController nextVersionManagementController)
    {

        LegacyManagementController controller = new LegacyManagementController(nextVersionManagementController, MODEL_VERSION);
        controller.initialize();
        return controller;
    }
}
