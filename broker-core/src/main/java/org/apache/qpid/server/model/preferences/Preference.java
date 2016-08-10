/*
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

package org.apache.qpid.server.model.preferences;

import java.security.Principal;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.server.model.ConfiguredObject;

public interface Preference
{
    String ID_ATTRIBUTE = "id";
    String NAME_ATTRIBUTE = "name";
    String TYPE_ATTRIBUTE = "type";
    String DESCRIPTION_ATTRIBUTE = "description";
    String OWNER_ATTRIBUTE = "owner";
    String ASSOCIATED_OBJECT_ATTRIBUTE = "associatedObject";
    String VISIBILITY_LIST_ATTRIBUTE = "visibilityList";
    String LAST_UPDATED_DATE_ATTRIBUTE = "lastUpdatedDate";
    String CREATED_DATE_ATTRIBUTE = "createdDate";
    String VALUE_ATTRIBUTE = "value";

    UUID getId();

    String getName();

    String getType();

    String getDescription();

    Principal getOwner();

    ConfiguredObject<?> getAssociatedObject();

    Set<Principal> getVisibilityList();

    Date getLastUpdatedDate();

    Date getCreatedDate();

    PreferenceValue getValue();

    Map<String, Object> getAttributes();
}
