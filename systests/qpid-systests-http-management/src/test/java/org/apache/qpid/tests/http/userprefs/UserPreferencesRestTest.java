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

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.junit.Test;

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

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"));

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                              prefDetails,
                              getHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                            allMap.containsKey(prefType));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
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

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"));

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                              prefDetails,
                              getHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                            allMap.containsKey(prefType));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
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
        Map<String, List<Map<String, Object>>> payload =
                Collections.singletonMap(prefType, Collections.singletonList(prefAttributes));
        getHelper().submitRequest(rootUrl, "POST", payload, SC_OK);

        Map<String, List<Map<String, Object>>> allPrefs = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);

        Map<String, Object> prefDetails = allPrefs.get(prefType).get(0);
        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(getBrokerAdmin().getValidUsername() + "@"));

        String typeUrl = String.format("virtualhost/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                              prefDetails,
                              getHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                            allMap.containsKey(prefType));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
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
        pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Map<String, Object> pref2Attributes = new HashMap<>();
        pref2Attributes.put(Preference.NAME_ATTRIBUTE, pref2Name);
        pref2Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        pref2Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Map<String, Object> payload = new HashMap<>();
        payload.put(prefType1, Collections.singletonList(pref1Attributes));
        payload.put(prefType2, Collections.singletonList(pref2Attributes));
        String url = "virtualhost/userpreferences";
        getHelper().submitRequest(url, "POST", payload, SC_OK);

        Map<String, Object> pref3Attributes = new HashMap<>();
        pref3Attributes.put(Preference.NAME_ATTRIBUTE, pref3Name);
        pref3Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        pref3Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        String url2 = String.format("virtualhost/userpreferences/%s", prefType2);
        getHelper().submitRequest(url2,
                                  "POST",
                                  Collections.singletonList(pref3Attributes),
                                  SC_OK);

        String allUrl = "virtualhost/userpreferences";
        final Map<String, Object> allMap = getHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 2, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                   allMap.containsKey(prefType1) && allMap.containsKey(prefType2));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pref1s = (List<Map<String, Object>>) allMap.get(prefType1);
        assertEquals("Unexpected number of preferences", 1, pref1s.size());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pref2s = (List<Map<String, Object>>) allMap.get(prefType2);
        assertEquals("Unexpected number of preferences", 2, pref2s.size());

        assertEquals("Unexpected preference returned from all url for type1. Found : " + pref1s.get(0).get(Preference.NAME_ATTRIBUTE),
                              pref1Name,
                              pref1s.get(0).get(Preference.NAME_ATTRIBUTE));
        Set<String> pref2Names = new HashSet<>();
        pref2Names.add((String) pref2s.get(0).get(Preference.NAME_ATTRIBUTE));
        pref2Names.add((String) pref2s.get(1).get(Preference.NAME_ATTRIBUTE));
        assertTrue("Unexpected preference returned from all url for type2. Found : " + pref2Names,
                   pref2Names.contains(pref2Name) && pref2Names.contains(pref3Name));
    }

    @Test
    public void putReplaceOne() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        prefAttributes.put("value", Collections.emptyMap());
        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        Map<String, Object> storedPreference = getHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, storedPreference.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE));

        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "new description");
        getHelper().submitRequest(fullUrl, "PUT", updatePreference, SC_OK);

        Map<String, Object> rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected id on updated pref", storedPreference.get(Preference.ID_ATTRIBUTE), rereadPrefDetails.get(Preference.ID_ATTRIBUTE));
        assertEquals("Unexpected description on updated pref", "new description", rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
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
            pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
            pref1Attributes.put(Preference.TYPE_ATTRIBUTE, pref1Type);

            Map<String, Object> pref2Attributes = new HashMap<>();
            pref2Attributes.put(Preference.NAME_ATTRIBUTE, pref2Name);
            pref2Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
            pref2Attributes.put(Preference.TYPE_ATTRIBUTE, pref2Type);

            final Map<String, List<Map<String, Object>>> payload = new HashMap<>();
            payload.put(pref1Type, Lists.newArrayList(pref1Attributes));
            payload.put(pref2Type, Lists.newArrayList(pref2Attributes));

            getHelper().submitRequest(rootUrl, "PUT", payload, SC_OK);
        }

        Map<String, List<Map<String, Object>>> original = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);
        assertEquals("Unexpected number of types in root map", 2, original.size());

        assertEquals("Unexpected number of " + pref1Type + " preferences", 1, original.get(pref1Type).size());
        assertEquals(pref1Type + " preference has unexpected name", pref1Name, original.get(pref1Type).iterator().next().get(Preference.NAME_ATTRIBUTE));

        assertEquals("Unexpected number of " + pref2Type + " preferences", 1, original.get(pref2Type).size());
        assertEquals(pref2Type + " preference has unexpected name", pref2Name, original.get(pref2Type).iterator().next().get(Preference.NAME_ATTRIBUTE));

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
            pref3Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
            pref3Attributes.put(Preference.TYPE_ATTRIBUTE, pref1Type);

            Map<String, Object> pref4Attributes = new HashMap<>();
            pref4Attributes.put(Preference.NAME_ATTRIBUTE, pref4Name);
            pref4Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
            pref4Attributes.put(Preference.TYPE_ATTRIBUTE, pref3Type);

            final Map<String, List<Map<String, Object>>> payload = new HashMap<>();
            payload.put(pref1Type, Lists.newArrayList(pref3Attributes));
            payload.put(pref3Type, Lists.newArrayList(pref4Attributes));

            getHelper().submitRequest(rootUrl, "PUT", payload, SC_OK);
        }

        Map<String, List<Map<String, Object>>> reread = getHelper().getJson(rootUrl, MAP_TYPE_REF, SC_OK);
        assertEquals("Unexpected number of types in root map after replacement", 2, reread.size());

        assertEquals("Unexpected number of " + pref1Type + " preferences", 1, reread.get(pref1Type).size());
        assertEquals(pref1Type + " preference has unexpected name", pref3Name, reread.get(pref1Type).iterator().next().get(Preference.NAME_ATTRIBUTE));

        assertEquals("Unexpected number of " + pref3Type + " preferences", 1, reread.get(pref3Type).size());
        assertEquals(pref3Type + " preference has unexpected name", pref4Name, reread.get(pref3Type).iterator().next().get(Preference.NAME_ATTRIBUTE));
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
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        final List<Map<String, Object>> payloadCreate = Collections.singletonList(prefAttributes);
        getHelper().submitRequest(typeUrl, "POST", payloadCreate, SC_OK);

        Map<String, Object> storedPreference = getHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, storedPreference.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE));

        // Update via url to type
        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "update 1");
        final List<Map<String, Object>> payloadUpdate1 = Collections.singletonList(updatePreference);
        getHelper().submitRequest(typeUrl, "POST", payloadUpdate1, SC_OK);

        Map<String, Object> rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected id on updated pref, update 1",
                              storedPreference.get(Preference.ID_ATTRIBUTE),
                              rereadPrefDetails.get(Preference.ID_ATTRIBUTE));
        assertEquals("Unexpected description on updated pref, update 1",
                              "update 1",
                              rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));

        // Update via url to root
        updatePreference = new HashMap<>(rereadPrefDetails);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "update 2");
        Map<String, List<Map<String, Object>>> payloadUpdate2 =
                Collections.singletonMap(prefType, Collections.singletonList(updatePreference));
        getHelper().submitRequest(rootUrl, "POST", payloadUpdate2, SC_OK);

        rereadPrefDetails = getHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected description on updated pref, update 2",
                              "update 2",
                              rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
    }

    @Test
    public void delete() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        String fullUrl = String.format("virtualhost/userpreferences/%s/%s", prefType, prefName);
        getHelper().submitRequest(fullUrl, "PUT", prefAttributes, SC_OK);

        getHelper().getJsonAsMap(fullUrl);

        getHelper().submitRequest(fullUrl, "DELETE", SC_OK);
        getHelper().submitRequest(fullUrl, "GET", SC_NOT_FOUND);
    }

}
