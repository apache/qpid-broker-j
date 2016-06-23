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

package org.apache.qpid.systest.rest;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.preferences.QueryPreferenceValue;
import org.apache.qpid.server.model.preferences.Preference;

public class UserPreferencesRestTest extends QpidRestTestCase
{

    public void testPutSinglePreferenceRoundTrip() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        Map<String, Object> prefValueAttributes = new HashMap<>();
        prefValueAttributes.put("valueAttrName", "valueAttrValue");
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> prefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertEquals("Unexpected pref owner", RestTestHelper.DEFAULT_USERNAME, prefDetails.get(Preference.OWNER_ATTRIBUTE));

        String typeUrl = String.format("broker/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                     prefDetails,
                     getRestTestHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "broker/userpreferences";
        final Map<String, Object> allMap = getRestTestHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                   allMap.containsKey(prefType));
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
    }

    public void testPutQueryPreferenceRoundTrip() throws Exception
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
        prefValueAttributes.put(QueryPreferenceValue.CURRENT_PAGE_ATTRIBUTE, 7);
        prefValueAttributes.put(QueryPreferenceValue.REFRESH_INTERVAL_ATTRIBUTE, 1000);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> prefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertEquals("Unexpected pref owner", RestTestHelper.DEFAULT_USERNAME, prefDetails.get(Preference.OWNER_ATTRIBUTE));

        String typeUrl = String.format("broker/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                     prefDetails,
                     getRestTestHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "broker/userpreferences";
        final Map<String, Object> allMap = getRestTestHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                   allMap.containsKey(prefType));
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
    }


    public void testPostSinglePreferenceRoundTrip() throws Exception
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

        String rootUrl = "broker/userpreferences";
        Map<String, List<Map<String, Object>>> payload =
                Collections.singletonMap(prefType, Collections.singletonList(prefAttributes));
        getRestTestHelper().submitRequest(rootUrl, "POST", payload, HttpServletResponse.SC_OK);

        Map<String, List<Map<String, Object>>> allPrefs = (Map<String, List<Map<String, Object>>>) getRestTestHelper().getJson(rootUrl, Object.class);

        Map<String, Object> prefDetails = allPrefs.get(prefType).get(0);
        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertEquals("Unexpected pref owner", RestTestHelper.DEFAULT_USERNAME, prefDetails.get(Preference.OWNER_ATTRIBUTE));

        String typeUrl = String.format("broker/userpreferences/%s", prefType);
        assertEquals("Unexpected preference returned from type url",
                     prefDetails,
                     getRestTestHelper().getJsonAsSingletonList(typeUrl));

        String allUrl = "broker/userpreferences";
        final Map<String, Object> allMap = getRestTestHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 1, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                   allMap.containsKey(prefType));
        List<Map<String, Object>> prefs = (List<Map<String, Object>>) allMap.get(prefType);
        assertEquals("Unexpected number of preferences", 1, prefs.size());

        assertEquals("Unexpected preference returned from all url", prefDetails, prefs.get(0));
    }

    public void testPostManyPreferences() throws Exception
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
        String url = "broker/userpreferences";
        getRestTestHelper().submitRequest(url, "POST", payload, HttpServletResponse.SC_OK);

        Map<String, Object> pref3Attributes = new HashMap<>();
        pref3Attributes.put(Preference.NAME_ATTRIBUTE, pref3Name);
        pref3Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        pref3Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        String url2 = String.format("broker/userpreferences/%s", prefType2);
        getRestTestHelper().submitRequest(url2,
                                          "POST",
                                          Collections.singletonList(pref3Attributes),
                                          HttpServletResponse.SC_OK);

        String allUrl = "broker/userpreferences";
        final Map<String, Object> allMap = getRestTestHelper().getJsonAsMap(allUrl);
        assertEquals("Unexpected number of types in all url response", 2, allMap.size());
        assertTrue("Expected type not found in all url response. Found : " + allMap.keySet(),
                   allMap.containsKey(prefType1) && allMap.containsKey(prefType2));
        List<Map<String, Object>> pref1s = (List<Map<String, Object>>) allMap.get(prefType1);
        assertEquals("Unexpected number of preferences", 1, pref1s.size());
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

    public void testPutUpdate() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        prefAttributes.put("value", Collections.emptyMap());
        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> storedPreference = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, storedPreference.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE));

        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "new description");
        getRestTestHelper().submitRequest(fullUrl, "PUT", updatePreference, HttpServletResponse.SC_OK);

        Map<String, Object> rereadPrefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected id on updated pref", storedPreference.get(Preference.ID_ATTRIBUTE), rereadPrefDetails.get(Preference.ID_ATTRIBUTE));
        assertEquals("Unexpected description on updated pref", "new description", rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
    }

    public void testPostUpdate() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        String typeUrl = String.format("broker/userpreferences/%s", prefType);
        String rootUrl = "broker/userpreferences";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.NAME_ATTRIBUTE, prefName);
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        final List<Map<String, Object>> payloadCreate = Collections.singletonList(prefAttributes);
        getRestTestHelper().submitRequest(typeUrl, "POST", payloadCreate, HttpServletResponse.SC_OK);

        Map<String, Object> storedPreference = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, storedPreference.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, storedPreference.get(Preference.DESCRIPTION_ATTRIBUTE));

        // Update via url to type
        Map<String, Object> updatePreference = new HashMap<>(storedPreference);
        updatePreference.put(Preference.DESCRIPTION_ATTRIBUTE, "update 1");
        final List<Map<String, Object>> payloadUpdate1 = Collections.singletonList(updatePreference);
        getRestTestHelper().submitRequest(typeUrl, "POST", payloadUpdate1, HttpServletResponse.SC_OK);

        Map<String, Object> rereadPrefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

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
        getRestTestHelper().submitRequest(rootUrl, "POST", payloadUpdate2, HttpServletResponse.SC_OK);

        rereadPrefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected description on updated pref, update 2",
                     "update 2",
                     rereadPrefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
    }

    public void testDelete() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_CREATED);

        getRestTestHelper().getJsonAsMap(fullUrl);

        getRestTestHelper().submitRequest(fullUrl, "DELETE", HttpServletResponse.SC_OK);

        try
        {
            getRestTestHelper().getJsonAsMap(fullUrl);
            fail();
        }
        catch (Exception e)
        {
            // pass
        }
    }
}
