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
package org.apache.qpid.server.management.plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;

public interface ManagementController
{
    String getVersion();

    Collection<String> getCategories();

    String getCategoryMapping(String category);

    String getCategory(ConfiguredObject<?> managedObject);

    Collection<String> getCategoryHierarchy(ConfiguredObject<?> root, String category);

    ManagementController getNextVersionManagementController();

    ManagementResponse handleGet(ManagementRequest request) throws ManagementException;

    ManagementResponse handlePut(ManagementRequest request) throws ManagementException;

    ManagementResponse handlePost(ManagementRequest request) throws ManagementException;

    ManagementResponse handleDelete(ManagementRequest request) throws ManagementException;

    Object get(ConfiguredObject<?> root,
               String category,
               List<String> path,
               Map<String, List<String>> parameters) throws ManagementException;

    Object createOrUpdate(ConfiguredObject<?> root,
                          String category,
                          List<String> path,
                          Map<String, Object> attributes,
                          boolean isPost) throws ManagementException;

    int delete(ConfiguredObject<?> root,
               String category,
               List<String> path,
               Map<String, List<String>> parameters) throws ManagementException;

    ManagementResponse invoke(ConfiguredObject<?> root,
                              String category,
                              List<String> path,
                              String operation,
                              Map<String, Object> parameters,
                              boolean isPost,
                              final boolean isSecureOrAllowedOnInsecureChannel) throws ManagementException;

    Object getPreferences(ConfiguredObject<?> root,
                          String category,
                          List<String> path,
                          Map<String, List<String>> parameters) throws ManagementException;

    void setPreferences(ConfiguredObject<?> root,
                        String category,
                        List<String> path,
                        Object preferences,
                        Map<String, List<String>> parameters,
                        boolean isPost) throws ManagementException;

    int deletePreferences(ConfiguredObject<?> root,
                          String category,
                          List<String> path,
                          Map<String, List<String>> parameters) throws ManagementException;

    Object formatConfiguredObject(Object content,
                                  Map<String, List<String>> parameters,
                                  boolean isSecureOrAllowedOnInsecureChannel);
}
