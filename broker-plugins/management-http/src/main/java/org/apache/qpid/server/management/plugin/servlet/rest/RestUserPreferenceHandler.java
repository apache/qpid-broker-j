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
 */

package org.apache.qpid.server.management.plugin.servlet.rest;

import java.security.AccessController;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.base.Joiner;

import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.UserPreferences;

public class RestUserPreferenceHandler
{
    public void handleDELETE(final UserPreferences userPreferences, final RequestInfo requestInfo)
    {
        final List<String> preferencesParts = requestInfo.getPreferencesParts();
        final Map<String, List<String>> queryParameters = requestInfo.getQueryParameters();
        String id = getIdFromQueryParameters(queryParameters);

        if (id != null)
        {
            final Set<Preference> allPreferences = userPreferences.getPreferences();
            for (Preference preference : allPreferences)
            {
                if (id.equals(preference.getId().toString()))
                {
                    String type = null;
                    String name = null;
                    if (preferencesParts.size() == 2)
                    {
                        type = preferencesParts.get(0);
                        name = preferencesParts.get(1);
                    }
                    else if (preferencesParts.size() == 1)
                    {
                        type = preferencesParts.get(0);
                    }
                    if ((type == null || type.equals(preference.getType()))
                        && (name == null || name.equals(preference.getName())))
                    {
                        userPreferences.replaceByTypeAndName(preference.getType(), preference.getName(), null);
                    }
                    return;
                }
            }
        }

        if (preferencesParts.size() == 2)
        {
            String type = preferencesParts.get(0);
            String name = preferencesParts.get(1);
            userPreferences.replaceByTypeAndName(type, name, null);
        }
        else if (preferencesParts.size() == 1)
        {
            String type = preferencesParts.get(0);
            userPreferences.replaceByType(type, Collections.<Preference>emptySet());
        }
        else if (preferencesParts.size() == 0)
        {
            userPreferences.replace(Collections.<Preference>emptySet());
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }
    }

    ActionTaken handlePUT(UserPreferences userPreferences, RequestInfo requestInfo, Object providedObject)
    {
        final List<String> preferencesParts = requestInfo.getPreferencesParts();

        if (!(providedObject instanceof Map))
        {
            throw new IllegalArgumentException("expected object");
        }
        Map<String, Object> providedAttributes = (Map<String, Object>) providedObject;

        if (preferencesParts.size() == 2)
        {
            String type = preferencesParts.get(0);
            String name = preferencesParts.get(1);

            ensureAttributeMatches(providedAttributes, "name", name);
            ensureAttributeMatches(providedAttributes, "type", type);

            String providedDescription = getProvidedAttributeAsString(providedAttributes, "description");
            UUID providedUuid = getProvidedUuid(providedAttributes);

            Set<Principal> visibilityList = getProvidedVisibilityList(providedAttributes);
            Map<String, Object> providedValueAttributes = getProvidedValueAttributes(providedAttributes);

            final Preference newPref = userPreferences.createPreference(providedUuid,
                                                                        type,
                                                                        name,
                                                                        providedDescription,
                                                                        visibilityList,
                                                                        providedValueAttributes);
            userPreferences.updateOrAppend(Collections.singleton(newPref));

            return providedUuid == null ? ActionTaken.CREATED : ActionTaken.UPDATED;
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }
    }

    void handlePOST(UserPreferences userPreferences, RequestInfo requestInfo, Object providedObject)
    {
        final List<String> preferencesParts = requestInfo.getPreferencesParts();

        if (preferencesParts.size() == 1)
        {
            String type = preferencesParts.get(0);
            if (!(providedObject instanceof List))
            {
                throw new IllegalArgumentException("expected a list of objects");
            }
            List<Object> providedObjects = (List<Object>) providedObject;

            Set<Preference> preferences = new HashSet<>(providedObjects.size());
            for (Object preferenceObject : providedObjects)
            {
                if (!(preferenceObject instanceof Map))
                {
                    throw new IllegalArgumentException("expected a list of objects");
                }
                Map<String, Object> preferenceAttributes = (Map<String, Object>) preferenceObject;

                ensureAttributeMatches(preferenceAttributes, "type", type);

                String providedName = getProvidedAttributeAsString(preferenceAttributes, "name");
                String providedDescription = getProvidedAttributeAsString(preferenceAttributes, "description");
                UUID providedUuid = getProvidedUuid(preferenceAttributes);
                Set<Principal> principals = getProvidedVisibilityList(preferenceAttributes);
                Map<String, Object> providedValueAttributes = getProvidedValueAttributes(preferenceAttributes);

                Preference preference = userPreferences.createPreference(providedUuid,
                                                                         type,
                                                                         providedName,
                                                                         providedDescription,
                                                                         principals,
                                                                         providedValueAttributes);
                preferences.add(preference);
            }

            userPreferences.updateOrAppend(preferences);
        }
        else if (preferencesParts.size() == 0)
        {
            if (!(providedObject instanceof Map))
            {
                throw new IllegalArgumentException("expected object");
            }
            Map<String, Object> providedObjectMap = (Map<String, Object>) providedObject;

            Set<Preference> preferences = new HashSet<>();
            for (String type : providedObjectMap.keySet())
            {
                if (!(providedObjectMap.get(type) instanceof List))
                {
                    final String errorMessage = String.format("expected a list of objects for attribute '%s'", type);
                    throw new IllegalArgumentException(errorMessage);
                }

                for (Object preferenceObject : (List<Object>) providedObjectMap.get(type))
                {
                    if (!(preferenceObject instanceof Map))
                    {
                        final String errorMessage =
                                String.format("encountered non preference object in list of type '%s'", type);
                        throw new IllegalArgumentException(errorMessage);
                    }
                    Map<String, Object> preferenceAttributes = (Map<String, Object>) preferenceObject;

                    ensureAttributeMatches(preferenceAttributes, "type", type);

                    String providedName = getProvidedAttributeAsString(preferenceAttributes, "name");
                    String providedDescription = getProvidedAttributeAsString(preferenceAttributes, "description");
                    UUID providedUuid = getProvidedUuid(preferenceAttributes);
                    Set<Principal> principals = getProvidedVisibilityList(preferenceAttributes);
                    Map<String, Object> providedValueAttributes = getProvidedValueAttributes(preferenceAttributes);

                    Preference preference = userPreferences.createPreference(providedUuid,
                                                                             type,
                                                                             providedName,
                                                                             providedDescription,
                                                                             principals,
                                                                             providedValueAttributes);
                    preferences.add(preference);
                }
            }

            userPreferences.updateOrAppend(preferences);
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }
    }

    Object handleGET(UserPreferences userPreferences, RequestInfo requestInfo)
    {
        final List<String> preferencesParts = requestInfo.getPreferencesParts();
        final Map<String, List<String>> queryParameters = requestInfo.getQueryParameters();
        String id = getIdFromQueryParameters(queryParameters);

        final Set<Preference> allPreferences;
        if (requestInfo.getType() == RequestInfo.RequestType.USER_PREFERENCES)
        {
            allPreferences = userPreferences.getPreferences();
        }
        else if (requestInfo.getType() == RequestInfo.RequestType.VISIBLE_PREFERENCES)
        {
            allPreferences = userPreferences.getVisiblePreferences();
        }
        else
        {
            throw new IllegalStateException(String.format(
                    "RestUserPreferenceHandler called with a unsupported request type: %s", requestInfo.getType()));
        }

        if (preferencesParts.size() == 2)
        {
            String type = preferencesParts.get(0);
            String name = preferencesParts.get(1);


            Preference foundPreference = null;
            for (Preference preference : allPreferences)
            {
                if (preference.getType().equals(type) && preference.getName().equals(name))
                {
                    if (id == null || id.equals(preference.getId().toString()))
                    {
                        foundPreference = preference;
                    }
                    break;
                }
            }

            if (foundPreference != null)
            {
                return foundPreference.getAttributes();
            }
            else
            {
                String errorMessage;
                if (id == null)
                {
                    errorMessage = String.format("Preference with name '%s' of type '%s' cannot be found",
                                                 name,
                                                 type);
                }
                else
                {
                    errorMessage = String.format("Preference with name '%s' of type '%s' and id '%s' cannot be found",
                                                 name,
                                                 type,
                                                 id);
                }
                throw new NotFoundException(errorMessage);
            }
        }
        else if (preferencesParts.size() == 1)
        {
            String type = preferencesParts.get(0);

            List<Map<String, Object>> preferences = new ArrayList<>();
            for (Preference preference : allPreferences)
            {
                if (preference.getType().equals(type))
                {
                    if (id == null || id.equals(preference.getId().toString()))
                    {
                        preferences.add(preference.getAttributes());
                    }
                }
            }
            return preferences;
        }
        else if (preferencesParts.size() == 0)
        {
            final Map<String, List<Map<String, Object>>> preferences = new HashMap<>();

            for (Preference preference : allPreferences)
            {
                if (id == null || id.equals(preference.getId().toString()))
                {
                    final String type = preference.getType();
                    if (!preferences.containsKey(type))
                    {
                        preferences.put(type, new ArrayList<Map<String, Object>>());
                    }

                    preferences.get(type).add(preference.getAttributes());
                }
            }

            return preferences;
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }
    }

    private String getIdFromQueryParameters(final Map<String, List<String>> queryParameters)
    {
        final String id;
        List<String> ids = queryParameters.get("id");
        if (ids != null && ids.size() > 1)
        {
            throw new IllegalArgumentException("Multiple ids in query string are not allowed");
        }
        return (ids == null ? null : ids.get(0));
    }

    private Map<String, Object> getProvidedValueAttributes(final Map<String, Object> preferenceAttributes)
    {
        Object providedValueAttributes = preferenceAttributes.get("value");

        if (providedValueAttributes == null)
        {
            return Collections.emptyMap();
        }

        if (!(providedValueAttributes instanceof Map))
        {
            final String errorMessage = String.format(
                    "Invalid preference value ('%s') found in payload, expected to be Map",
                    providedValueAttributes);
            throw new IllegalArgumentException(errorMessage);
        }

        for (Object key : ((Map) providedValueAttributes).keySet())
        {
            if (!(key instanceof String))
            {
                String errorMessage = String.format(
                        "The keys of the preference value object must be of type String,  Found key (%s)", key);
                throw new IllegalArgumentException(errorMessage);
            }
        }
        return (Map<String, Object>) providedValueAttributes;
    }

    private UUID getProvidedUuid(final Map<String, Object> providedObjectMap)
    {
        String providedId = getProvidedAttributeAsString(providedObjectMap, "id");
        try
        {
            return providedId != null ? UUID.fromString(providedId) : null;
        }
        catch (IllegalArgumentException iae)
        {
            throw new IllegalArgumentException(String.format("Invalid UUID ('%s') found in payload", providedId), iae);
        }
    }

    private Set<Principal> getProvidedVisibilityList(final Map<String, Object> providedAttributes)
    {
        Object visibilityListObject = providedAttributes.get("visibilityList");

        if (visibilityListObject == null)
        {
            return null;
        }

        if (!(visibilityListObject instanceof Collection))
        {
            String errorMessage = String.format("Invalid visibilityList ('%s') found in payload", visibilityListObject);
            throw new IllegalArgumentException(errorMessage);
        }

        Subject currentSubject = Subject.getSubject(AccessController.getContext());
        if (currentSubject == null)
        {
            throw new SecurityException("Current thread does not have a user");
        }

        HashMap<String, Principal> principalNameMap = new HashMap<>(currentSubject.getPrincipals().size());
        for (Principal principal : currentSubject.getPrincipals())
        {
            principalNameMap.put(principal.getName(), principal);
        }

        Collection visibilityList = (Collection) visibilityListObject;
        Set<Principal> principals = new HashSet<>(visibilityList.size());

        for (Object visibilityObject : visibilityList)
        {
            if (!(visibilityObject instanceof String))
            {
                String errorMessage = String.format("Invalid visibilityList, '%s' is not a string",
                                                    visibilityObject);
                throw new IllegalArgumentException(errorMessage);
            }

            Principal principal = principalNameMap.get(visibilityObject);
            if (principal == null)
            {
                String errorMessage = String.format("Invalid visibilityList, this user does not hold principal '%s'",
                                                    visibilityObject);
                throw new IllegalArgumentException(errorMessage);
            }
            principals.add(principal);
        }
        return principals;
    }

    private void ensureAttributeMatches(final Map<String, Object> preferenceAttributes,
                                        final String attributeName,
                                        final String expectedValue)
    {
        final Object providedValue = preferenceAttributes.get(attributeName);
        if (providedValue != null && !Objects.equals(providedValue, expectedValue))
        {
            final String errorMessage = String.format(
                    "The attribute '%s' within the payload ('%s') contradicts the value implied by the url ('%s')",
                    attributeName,
                    providedValue,
                    expectedValue);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private String getProvidedAttributeAsString(final Map<String, Object> preferenceAttributes,
                                                final String attributeName)
    {
        Object providedValue = preferenceAttributes.get(attributeName);
        if (providedValue != null && !(providedValue instanceof String))
        {
            final String errorMessage = String.format("Attribute '%s' must be of type string. Found : '%s'",
                                                      attributeName, providedValue);
            throw new IllegalArgumentException(errorMessage);
        }
        return (String) providedValue;
    }

    enum ActionTaken
    {
        CREATED,
        UPDATED
    }
}
