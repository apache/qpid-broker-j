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
package org.apache.qpid.server.management.plugin.controller;

import java.util.List;
import java.util.Map;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.model.ConfiguredObject;

public interface CategoryController
{
    String getCategory();

    String getNextVersionCategory();

    String getDefaultType();

    String[] getParentCategories();

    LegacyManagementController getManagementController();

    Object get(ConfiguredObject<?> root,
               List<String> path,
               Map<String, List<String>> parameters) throws ManagementException;

    LegacyConfiguredObject createOrUpdate(ConfiguredObject<?> root,
                                          List<String> path,
                                          Map<String, Object> attributes,
                                          boolean isPost) throws ManagementException;

    int delete(ConfiguredObject<?> root,
               List<String> path,
               Map<String, List<String>> parameters) throws ManagementException;

    ManagementResponse invoke(ConfiguredObject<?> root,
                              List<String> path,
                              String operation,
                              Map<String, Object> parameters,
                              boolean isPost,
                              final boolean isSecure) throws ManagementException;

    Object getPreferences(ConfiguredObject<?> root,
                          List<String> path,
                          Map<String, List<String>> parameters) throws ManagementException;

    void setPreferences(ConfiguredObject<?> root,
                        List<String> path,
                        Object preferences,
                        Map<String, List<String>> parameters,
                        boolean isPost) throws ManagementException;

    int deletePreferences(ConfiguredObject<?> root,
                          List<String> path,
                          Map<String, List<String>> parameters) throws ManagementException;

    LegacyConfiguredObject convertFromNextVersion(LegacyConfiguredObject nextVersionObject);
}
