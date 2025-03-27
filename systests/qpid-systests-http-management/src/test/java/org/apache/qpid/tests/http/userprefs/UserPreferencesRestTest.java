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

package org.apache.qpid.tests.http.userprefs;

import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.preferences.QueryPreferenceValue;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig()
public class UserPreferencesRestTest extends HttpTestBase
{
    private static final TypeReference<Map<String, List<Map<String, Object>>>>
            MAP_TYPE_REF = new TypeReference<Map<String, List<Map<String, Object>>>>() {};

    @Test
    public void putSinglePreferenceRoundTrip() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        Map<String, Object> prefValueAttributes = new HashMap<>();
        prefValueAttributes.put("valueAttrName", "valueAttrValue");
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        Map<String, Object> prefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals(prefName, prefDetails.get(Preference.NAME_ATTRIBUTE), "Unexpected pref name");
        assertEquals(prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected pref description");
        assertEquals(prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE), "Unexpected pref type");
        assertEquals(prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE),
                "Unexpected pref value");
        assertTrue(((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"),
                "Unexpected pref owner");

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals(prefDetails, getHelper().getJsonAsSingletonList(typeUrl),
                "Unexpected preference returned from type url");

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals(1, allMap.size(), "Unexpected number of types in all url response");
        assertTrue(allMap.containsKey(prefType),
                "Expected type not found in all url response. Found : " + allMap.keySet());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals(1, prefs.size(), "Unexpected number of preferences");

        assertEquals(prefDetails, prefs.get(0), "Unexpected preference returned from all url");
    }

    @Test
    public void putQueryPreferenceRoundTrip() throws Exception
    {
        final String prefName = "myquery";
        final String prefDescription = "myquerydesc";
        final String prefType = "query";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        Map<String, Object> prefValueAttributes = new HashMap<>();
        prefValueAttributes.put(QueryPreferenceValue.SCOPE_ATTRIBUTE, "");
        prefValueAttributes.put(QueryPreferenceValue.CATEGORY_ATTRIBUTE, "queue");
        prefValueAttributes.put(QueryPreferenceValue.SELECT_ATTRIBUTE, "id,name,queueDepthMessages");
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        Map<String, Object> prefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals(prefName, prefDetails.get(Preference.NAME_ATTRIBUTE), "Unexpected pref name");
        assertEquals(prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected pref description");
        assertEquals(prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE), "Unexpected pref type");
        assertEquals(prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE),
                "Unexpected pref value");
        assertTrue(((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"),
                "Unexpected pref owner");

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals(prefDetails, getHelper().getJsonAsSingletonList(typeUrl),
                "Unexpected preference returned from type url");

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals(1, allMap.size(), "Unexpected number of types in all url response");
        assertTrue(allMap.containsKey(prefType),
                "Expected type not found in all url response. Found : " + allMap.keySet());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals(1, prefs.size(), "Unexpected number of preferences");

        assertEquals(prefDetails, prefs.get(0), "Unexpected preference returned from all url");
    }


    @Test
    public void postSinglePreferenceRoundTrip() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.NAME_ATTRIBUTE, prefName);
        prefAttributes.put(Preference.TYPE_ATTRIBUTE, prefType);
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        Map<String, Object> prefValueAttributes = new HashMap<>();
        prefValueAttributes.put("valueAttrName", "valueAttrValue");
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String rootUrl = "virtualhost/userpreferences";
        Map<String, List<Map<String, Object>>> payload = Map.of(prefType, List.of(prefAttributes));
        getHelper().submitRequest(rootUrl, "POST", payload, SC_OK);

        Map<String, List<Map<String, Object>>> allPrefs = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);

        Map<String, Object> prefDetails = allPrefs.get(prefType).get(0);
        assertEquals(prefName, prefDetails.get(Preference.NAME_ATTRIBUTE), "Unexpected pref name");
        assertEquals(prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected pref description");
        assertEquals(prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE), "Unexpected pref type");
        assertEquals(prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE),
                "Unexpected pref value");
        assertTrue(((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"),
                "Unexpected pref owner");

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals(prefDetails, getHelper().getJsonAsSingletonList(typeUrl),
                "Unexpected preference returned from type url");

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals(1, allMap.size(), "Unexpected number of types in all url response");
        assertTrue(allMap.containsKey(prefType),
                "Expected type not found in all url response. Found : " + allMap.keySet());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals(1, prefs.size(), "Unexpected number of preferences");

        assertEquals(prefDetails, prefs.get(0), "Unexpected preference returned from all url");
    }

    @Test
    public void postManyPreferences() throws Exception
    {
        final String pref1Name = "pref1";
        final String pref2Name = "pref2Name";
        final String pref3Name = "pref3";
        final String prefType1 = "X-prefType1";
        final String prefType2 = "X-prefType2";

        Map<String, Object> pref1Attributes = new HashMap<>();
        pref1Attributes.put(Preference.NAME_ATTRIBUTE, pref1Name);
        pref1Attributes.put(Preference.TYPE_ATTRIBUTE, prefType1);
        pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());

        Map<String, Object> pref2Attributes = new HashMap<>();
        pref2Attributes.put(Preference.NAME_ATTRIBUTE, pref2Name);
        pref2Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        pref2Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());

        Map<String, Object> payload = new HashMap<>();
        payload.put(prefType1, List.of(pref1Attributes));
        payload.put(prefType2, List.of(pref2Attributes));
        String url = "virtualhost/userpreferences";
        getHelper().submitRequest(url, "POST", payload, SC_OK);

        Map<String, Object> pref3Attributes = new HashMap<>();
        pref3Attributes.put(Preference.NAME_ATTRIBUTE, pref3Name);
        pref3Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        pref3Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());

        String url2 = String.format("virtualhost/userpreferences/%s", prefType2);
        getHelper().submitRequest(url2, "POST", List.of(pref3Attributes), SC_OK);

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals(2, allMap.size(), "Unexpected number of types in all url response");
        assertTrue(allMap.containsKey(prefType1) && allMap.containsKey(prefType2),
                "Expected type not found in all url response. Found : " + allMap.keySet());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pref1s = (List<Map<String, Object>>) allMap.get(prefType1);
        assertEquals(1, pref1s.size(), "Unexpected number of preferences");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pref2s = (List<Map<String, Object>>) allMap.get(prefType2);
        assertEquals(2, pref2s.size(), "Unexpected number of preferences");

        assertEquals(pref1Name, pref1s.get(0).get(Preference.NAME_ATTRIBUTE),
                "Unexpected preference returned from all url for type1. Found : " + pref1s.get(0).get(Preference.NAME_ATTRIBUTE));
        Set<String> pref2Names = new HashSet<>();
        pref2Names.add((String) pref2s.get(0).get(Preference.NAME_ATTRIBUTE));
        pref2Names.add((String) pref2s.get(1).get(Preference.NAME_ATTRIBUTE));
        assertTrue(pref2Names.contains(pref2Name) && pref2Names.contains(pref3Name),
                "Unexpected preference returned from all url for type2. Found : " + pref2Names);
    }

    @Test
    public void putReplaceOne() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        prefAttributes.put("value", Map.of());
        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        Map<String, Object> storedPreference = getHelper().getJsonAsMap(fullUrl);

        assertEquals(prefName, storedPreference.get(Preference.NAME_ATTRIBUTE), "Unexpected pref name");
        assertEquals(prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected pref description");

        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "new description");
        getHelper().submitRequest(fullUrl, "PUT", updatePreference, SC_OK);

        Map<String, Object> rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals(storedPreference.get(Preference.ID_ATTRIBUTE), rereadPrefDetails.get(Preference.ID_ATTRIBUTE),
                "Unexpected id on updated pref");
        assertEquals("new description", rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected description on updated pref");
    }

    @Test
    public void putReplaceMany() throws Exception
    {
        final String pref1Name = "mypref1";
        final String pref1Type = "X-testtype1";
        final String pref2Name = "mypref2";
        final String pref2Type = "X-testtype2";

        String rootUrl = "virtualhost/userpreferences";

        {
            // Create two preferences (of different types)

            Map<String, Object> pref1Attributes = new HashMap<>();
            pref1Attributes.put(Preference.NAME_ATTRIBUTE, pref1Name);
            pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
            pref1Attributes.put(Preference.TYPE_ATTRIBUTE, pref1Type);

            Map<String, Object> pref2Attributes = new HashMap<>();
            pref2Attributes.put(Preference.NAME_ATTRIBUTE, pref2Name);
            pref2Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
            pref2Attributes.put(Preference.TYPE_ATTRIBUTE, pref2Type);

            final Map<String, List<Map<String, Object>>> payload = new HashMap<>();
            payload.put(pref1Type, new ArrayList<>(List.of(pref1Attributes)));
            payload.put(pref2Type, new ArrayList<>(List.of(pref2Attributes)));

            getHelper().submitRequest(rootUrl, "PUT", payload, SC_OK);
        }

        Map<String, List<Map<String, Object>>> original = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);
        assertEquals(2, original.size(), "Unexpected number of types in root map");

        assertEquals(1, original.get(pref1Type).size(),
                "Unexpected number of " + pref1Type + " preferences");
        assertEquals(pref1Name, original.get(pref1Type).iterator().next().get(Preference.NAME_ATTRIBUTE),
                pref1Type + " preference has unexpected name");

        assertEquals(1, original.get(pref2Type).size(),
                "Unexpected number of " + pref2Type + " preferences");
        assertEquals(pref2Name, original.get(pref2Type).iterator().next().get(Preference.NAME_ATTRIBUTE),
                pref2Type + " preference has unexpected name");

        final String pref3Name = "mypref3";
        final String pref4Name = "mypref4";
        final String pref3Type = "X-testtype3";

        {
            // Replace all the preferences with ones that partially overlap the existing set:
            // The preference of type X-testtype1 is replaced
            // The preference of type X-testtype2 is removed
            // A preference of type X-testtype3 is added

            Map<String, Object> pref3Attributes = new HashMap<>();
            pref3Attributes.put(Preference.NAME_ATTRIBUTE, pref3Name);
            pref3Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
            pref3Attributes.put(Preference.TYPE_ATTRIBUTE, pref1Type);

            Map<String, Object> pref4Attributes = new HashMap<>();
            pref4Attributes.put(Preference.NAME_ATTRIBUTE, pref4Name);
            pref4Attributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
            pref4Attributes.put(Preference.TYPE_ATTRIBUTE, pref3Type);

            final Map<String, List<Map<String, Object>>> payload = new HashMap<>();
            payload.put(pref1Type, new ArrayList<>(List.of(pref3Attributes)));
            payload.put(pref3Type, new ArrayList<>(List.of(pref4Attributes)));

            getHelper().submitRequest(rootUrl, "PUT", payload, SC_OK);
        }

        Map<String, List<Map<String, Object>>> reread = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);
        assertEquals(2, reread.size(), "Unexpected number of types in root map after replacement");

        assertEquals(1, reread.get(pref1Type).size(), "Unexpected number of " + pref1Type + " preferences");
        assertEquals(pref3Name, reread.get(pref1Type).iterator().next().get(Preference.NAME_ATTRIBUTE),
                pref1Type + " preference has unexpected name");

        assertEquals(1, reread.get(pref3Type).size(), "Unexpected number of " + pref3Type + " preferences");
        assertEquals(pref4Name, reread.get(pref3Type).iterator().next().get(Preference.NAME_ATTRIBUTE),
                pref3Type + " preference has unexpected name");
    }

    @Test
    public void postUpdate() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        String rootUrl = "virtualhost/userpreferences";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.NAME_ATTRIBUTE, prefName);
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
        final List<Map<String, Object>> payloadCreate = List.of(prefAttributes);
        getHelper().submitRequest(typeUrl, "POST", payloadCreate, SC_OK);

        Map<String, Object> storedPreference = getHelper().getJsonAsMap(fullUrl);

        assertEquals(prefName, storedPreference.get(Preference.NAME_ATTRIBUTE), "Unexpected pref name");
        assertEquals(prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected pref description");

        // Update via url to type
        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "update 1");
        final List<Map<String, Object>> payloadUpdate1 = List.of(updatePreference);
        getHelper().submitRequest(typeUrl, "POST", payloadUpdate1, SC_OK);

        Map<String, Object> rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals(storedPreference.get(Preference.ID_ATTRIBUTE), rereadPrefDetails.get(Preference.ID_ATTRIBUTE),
                "Unexpected id on updated pref, update 1");
        assertEquals("update 1", rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected description on updated pref, update 1");

        // Update via url to root
        updatePreference = new HashMap<>(rereadPrefDetails);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "update 2");
        Map<String, List<Map<String, Object>>> payloadUpdate2 = Map.of(prefType, List.of(updatePreference));
        getHelper().submitRequest(rootUrl, "POST", payloadUpdate2, SC_OK);

        rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals("update 2", rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE),
                "Unexpected description on updated pref, update 2");
    }

    @Test
    public void delete() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Map.of());
        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        getHelper().getJsonAsMap(fullUrl);

        getHelper().submitRequest(fullUrl, "DELETE", SC_OK);
        getHelper().submitRequest(fullUrl, "GET", SC_NOT_FOUND);
    }
}
