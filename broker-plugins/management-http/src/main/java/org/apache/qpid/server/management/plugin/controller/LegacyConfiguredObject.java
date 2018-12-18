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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.management.plugin.ManagementResponse;

public interface LegacyConfiguredObject
{
    String ID = "id";
    String NAME = "name";
    String TYPE = "type";
    String DESCRIPTION = "description";
    String DURABLE = "durable";
    String CONTEXT = "context";
    String LIFETIME_POLICY = "lifetimePolicy";
    String STATE = "state";
    String DESIRED_STATE = "desiredState";

    String LAST_UPDATED_BY = "lastUpdatedBy";
    String LAST_UPDATED_TIME = "lastUpdatedTime";
    String CREATED_BY = "createdBy";
    String CREATED_TIME = "createdTime";
    String LAST_OPENED_TIME = "lastOpenedTime";

    Set<String> AVAILABLE_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            ID,
            NAME,
            TYPE,
            DESCRIPTION,
            DURABLE,
            CONTEXT,
            LIFETIME_POLICY,
            STATE,
            DESIRED_STATE,
            LAST_OPENED_TIME,
            LAST_UPDATED_BY,
            LAST_UPDATED_TIME,
            CREATED_BY,
            CREATED_TIME)));

    Collection<String> getAttributeNames();

    Object getAttribute(String name);

    Map<String, Object> getStatistics();

    Object getActualAttribute(String name);

    boolean isSecureAttribute(String name);

    boolean isOversizedAttribute(String name);

    String getCategory();

    Collection<LegacyConfiguredObject> getChildren(String category);

    LegacyConfiguredObject getParent(String category);

    String getContextValue(String contextKey);

    ManagementResponse invoke(String operation, Map<String, Object> parameters, boolean isSecure);

    LegacyConfiguredObject getNextVersionConfiguredObject();
}
