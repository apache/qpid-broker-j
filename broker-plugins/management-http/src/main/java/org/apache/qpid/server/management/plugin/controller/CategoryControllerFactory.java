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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.plugin.Pluggable;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public interface CategoryControllerFactory extends Pluggable
{
    CategoryController createController(String type, LegacyManagementController managementController);

    Set<String> getSupportedCategories();

    String getModelVersion();

    static Set<CategoryControllerFactory> findFactories(String version)
    {
        final Iterable<CategoryControllerFactory> factories =
                new QpidServiceLoader().atLeastOneInstanceOf(CategoryControllerFactory.class);

        return StreamSupport.stream(factories.spliterator(), false)
                            .filter(f -> version.equals(f.getModelVersion()))
                            .collect(Collectors.toSet());
    }
}
