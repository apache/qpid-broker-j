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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.util.FutureHelper;

public class RestUserPreferenceHandler
{
    private final long _preferenceOperationTimeout;

    public RestUserPreferenceHandler(final long preferenceOperationTimeout)
    {
        _preferenceOperationTimeout = preferenceOperationTimeout;
    }

    public void handleDELETE(final UserPreferences userPreferences, final RequestInfo requestInfo)
    {
        if (userPreferences == null)
        {
            throw new NotFoundException("User preferences are not available");
        }

        final List<String> preferencesParts = requestInfo.getPreferencesParts();
        final Map<String, List<String>> queryParameters = requestInfo.getQueryParameters();
        UUID id = getIdFromQueryParameters(queryParameters);

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
        else if (preferencesParts.size() == 0)
        {
            // pass
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }

        awaitFuture(userPreferences.delete(type, name, id));
    }

    public void handlePUT(ConfiguredObject<?> target, RequestInfo requestInfo, Object providedObject)
    {
        UserPreferences userPreferences = target.getUserPreferences();
        if (userPreferences == null)
        {
            throw new NotFoundException("User preferences are not available");
        }

        final List<String> preferencesParts = requestInfo.getPreferencesParts();

        if (preferencesParts.size() == 2)
        {
            if (!(providedObject instanceof Map))
            {
                throw new IllegalArgumentException("expected object");
            }
            Map<String, Object> providedAttributes = (Map<String, Object>) providedObject;

            String type = preferencesParts.get(0);
            String name = preferencesParts.get(1);

            ensureAttributeMatches(providedAttributes, "name", name);
            ensureAttributeMatches(providedAttributes, "type", type);

            Preference preference = PreferenceFactory.fromAttributes(target, providedAttributes);

            awaitFuture(userPreferences.replaceByTypeAndName(type, name , preference));
        }
        else if (preferencesParts.size() == 1)
        {
            String type = preferencesParts.get(0);

            if (!(providedObject instanceof List))
            {
                throw new IllegalArgumentException("expected a list of objects");
            }

            List<Preference> replacementPreferences = validateAndConvert(target, type, (List<Object>) providedObject);
            awaitFuture(userPreferences.replaceByType(type, replacementPreferences));
        }
        else if (preferencesParts.size() == 0)
        {
            if (!(providedObject instanceof Map))
            {
                throw new IllegalArgumentException("expected object");
            }

            List<Preference> replacementPreferences = validateAndConvert(target, (Map<String, Object>) providedObject);

            awaitFuture(userPreferences.replace(replacementPreferences));
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }
    }

    public void handlePOST(ConfiguredObject<?> target, RequestInfo requestInfo, Object providedObject)
    {
        UserPreferences userPreferences = target.getUserPreferences();
        if (userPreferences == null)
        {
            throw new NotFoundException("User preferences are not available");
        }

        final List<String> preferencesParts = requestInfo.getPreferencesParts();
        final Set<Preference> preferences = new LinkedHashSet<>();

        if (preferencesParts.size() == 1)
        {
            String type = preferencesParts.get(0);
            if (!(providedObject instanceof List))
            {
                throw new IllegalArgumentException("expected a list of objects");
            }
            preferences.addAll(validateAndConvert(target, type, (List<Object>) providedObject));

        }
        else if (preferencesParts.size() == 0)
        {
            if (!(providedObject instanceof Map))
            {
                throw new IllegalArgumentException("expected object");
            }

            preferences.addAll(validateAndConvert(target, (Map<String, Object>) providedObject));
        }
        else
        {
            throw new IllegalArgumentException(String.format("unexpected path '%s'",
                                                             Joiner.on("/").join(preferencesParts)));
        }

        awaitFuture(userPreferences.updateOrAppend(preferences));
    }

    public Object handleGET(UserPreferences userPreferences, RequestInfo requestInfo)
    {
        if (userPreferences == null)
        {
            throw new NotFoundException("User preferences are not available");
        }

        final List<String> preferencesParts = requestInfo.getPreferencesParts();
        final Map<String, List<String>> queryParameters = requestInfo.getQueryParameters();
        UUID id = getIdFromQueryParameters(queryParameters);

        final ListenableFuture<Set<Preference>> allPreferencesFuture;
        if (requestInfo.getType() == RequestType.USER_PREFERENCES)
        {
            allPreferencesFuture = userPreferences.getPreferences();
        }
        else if (requestInfo.getType() == RequestType.VISIBLE_PREFERENCES)
        {
            allPreferencesFuture = userPreferences.getVisiblePreferences();
        }
        else
        {
            throw new IllegalStateException(String.format(
                    "RestUserPreferenceHandler called with a unsupported request type: %s", requestInfo.getType()));
        }
        final Set<Preference> allPreferences;
        allPreferences = awaitFuture(allPreferencesFuture);

        if (preferencesParts.size() == 2)
        {
            String type = preferencesParts.get(0);
            String name = preferencesParts.get(1);


            Preference foundPreference = null;
            for (Preference preference : allPreferences)
            {
                if (preference.getType().equals(type) && preference.getName().equals(name))
                {
                    if (id == null || id.equals(preference.getId()))
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
                    if (id == null || id.equals(preference.getId()))
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
                if (id == null || id.equals(preference.getId()))
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

    private <T> T awaitFuture(final ListenableFuture<T> future)
    {
        return FutureHelper.<T, RuntimeException>await(future, _preferenceOperationTimeout, TimeUnit.MILLISECONDS);
    }

    private List<Preference> validateAndConvert(final ConfiguredObject<?> target, final Map<String, Object> providedObjectMap)
    {
        List<Preference> replacementPreferences = new ArrayList<>();
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

                Preference preference = PreferenceFactory.fromAttributes(target, preferenceAttributes);

                replacementPreferences.add(preference);
            }
        }
        return replacementPreferences;
    }

    private List<Preference> validateAndConvert(final ConfiguredObject<?> target,
                                                final String type,
                                                final List<Object> providedObjects)
    {
        List<Preference> replacementPreferences = new ArrayList<>();

        for (Object preferenceObject : providedObjects)
        {
            if (!(preferenceObject instanceof Map))
            {
                throw new IllegalArgumentException("expected a list of objects");
            }
            Map<String, Object> preferenceAttributes = (Map<String, Object>) preferenceObject;

            ensureAttributeMatches(preferenceAttributes, "type", type);

            Preference preference = PreferenceFactory.fromAttributes(target, preferenceAttributes);

            replacementPreferences.add(preference);
        }
        return replacementPreferences;
    }

    private UUID getIdFromQueryParameters(final Map<String, List<String>> queryParameters)
    {
        List<String> ids = queryParameters.get("id");
        if (ids != null && ids.size() > 1)
        {
            throw new IllegalArgumentException("Multiple ids in query string are not allowed");
        }
        return (ids == null ? null : UUID.fromString(ids.get(0)));
    }

    private void ensureAttributeMatches(final Map<String, Object> preferenceAttributes,
                                        final String attributeName,
                                        final String expectedValue)
    {
        final Object providedValue = preferenceAttributes.get(attributeName);
        if (providedValue != null)
        {
            if (!Objects.equals(providedValue, expectedValue))
            {
                final String errorMessage = String.format(
                        "The attribute '%s' within the payload ('%s') contradicts the value implied by the url ('%s')",
                        attributeName,
                        providedValue,
                        expectedValue);
                throw new IllegalArgumentException(errorMessage);
            }
        }
        else
        {
            preferenceAttributes.put(attributeName, expectedValue);
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
}
