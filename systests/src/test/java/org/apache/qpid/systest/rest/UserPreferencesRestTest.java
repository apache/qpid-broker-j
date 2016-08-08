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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_OK);

        Map<String, Object> prefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(RestTestHelper.DEFAULT_USERNAME + "@"));

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
        prefAttributes.put(Preference.VALUE_ATTRIBUTE, prefValueAttributes);

        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_OK);

        Map<String, Object> prefDetails = getRestTestHelper().getJsonAsMap(fullUrl);

        assertEquals("Unexpected pref name", prefName, prefDetails.get(Preference.NAME_ATTRIBUTE));
        assertEquals("Unexpected pref description", prefDescription, prefDetails.get(Preference.DESCRIPTION_ATTRIBUTE));
        assertEquals("Unexpected pref type", prefType, prefDetails.get(Preference.TYPE_ATTRIBUTE));
        assertEquals("Unexpected pref value", prefValueAttributes, prefDetails.get(Preference.VALUE_ATTRIBUTE));
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(RestTestHelper.DEFAULT_USERNAME + "@"));

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
        assertTrue("Unexpected pref owner", ((String) prefDetails.get(Preference.OWNER_ATTRIBUTE)).startsWith(RestTestHelper.DEFAULT_USERNAME + "@"));

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

    public void testPutReplaceOne() throws Exception
    {
        final String prefName = "mypref";
        final String prefDescription = "mydesc";
        final String prefType = "X-testtype";

        Map<String, Object> prefAttributes = new HashMap<>();
        prefAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, prefDescription);

        prefAttributes.put("value", Collections.emptyMap());
        String fullUrl = String.format("broker/userpreferences/%s/%s", prefType, prefName);
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_OK);

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

    public void testPutReplaceMany() throws Exception
    {
        final String pref1Name = "mypref1";
        final String pref1Type = "X-testtype1";
        final String pref2Name = "mypref2";
        final String pref2Type = "X-testtype2";

        String rootUrl = "broker/userpreferences";

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

            getRestTestHelper().submitRequest(rootUrl, "PUT", payload, HttpServletResponse.SC_OK);
        }

        Map<String, List<Map<String, Object>>> original = getRestTestHelper().getJson(rootUrl, Map.class);
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

            getRestTestHelper().submitRequest(rootUrl, "PUT", payload, HttpServletResponse.SC_OK);
        }

        Map<String, List<Map<String, Object>>> reread = getRestTestHelper().getJson(rootUrl, Map.class);
        assertEquals("Unexpected number of types in root map after replacement", 2, reread.size());

        assertEquals("Unexpected number of " + pref1Type + " preferences", 1, reread.get(pref1Type).size());
        assertEquals(pref1Type + " preference has unexpected name", pref3Name, reread.get(pref1Type).iterator().next().get(Preference.NAME_ATTRIBUTE));

        assertEquals("Unexpected number of " + pref3Type + " preferences", 1, reread.get(pref3Type).size());
        assertEquals(pref3Type + " preference has unexpected name", pref4Name, reread.get(pref3Type).iterator().next().get(Preference.NAME_ATTRIBUTE));
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
        getRestTestHelper().submitRequest(fullUrl, "PUT", prefAttributes, HttpServletResponse.SC_OK);

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

    public void testWildcards() throws Exception
    {
        final String pref1Name = "pref1Name";
        final String pref2Name = "pref2Name";
        final String pref3Name = "pref3Name";
        final String prefType1 = "X-prefType1";
        final String prefType2 = "X-prefType2";

        Map<String, Object> vh1Pref1Attributes = new HashMap<>();
        vh1Pref1Attributes.put(Preference.NAME_ATTRIBUTE, pref1Name);
        vh1Pref1Attributes.put(Preference.TYPE_ATTRIBUTE, prefType1);
        vh1Pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Map<String, Object> vh2Pref1Attributes = new HashMap<>();
        vh2Pref1Attributes.put(Preference.NAME_ATTRIBUTE, pref2Name);
        vh2Pref1Attributes.put(Preference.TYPE_ATTRIBUTE, prefType1);
        vh2Pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Map<String, Object> vh2Pref2Attributes = new HashMap<>();
        vh2Pref2Attributes.put(Preference.NAME_ATTRIBUTE, pref3Name);
        vh2Pref2Attributes.put(Preference.TYPE_ATTRIBUTE, prefType2);
        vh2Pref2Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Map<String, Object> vh3Pref1Attributes = new HashMap<>();
        vh3Pref1Attributes.put(Preference.NAME_ATTRIBUTE, pref1Name);
        vh3Pref1Attributes.put(Preference.TYPE_ATTRIBUTE, prefType1);
        vh3Pref1Attributes.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        String vh1PostUrl = String.format("virtualhost/%s/%s/userpreferences", QpidRestTestCase.TEST1_VIRTUALHOST, QpidRestTestCase.TEST1_VIRTUALHOST);
        String vh2PostUrl = String.format("virtualhost/%s/%s/userpreferences", QpidRestTestCase.TEST2_VIRTUALHOST, QpidRestTestCase.TEST2_VIRTUALHOST);
        String vh3PostUrl = String.format("virtualhost/%s/%s/userpreferences", QpidRestTestCase.TEST3_VIRTUALHOST, QpidRestTestCase.TEST3_VIRTUALHOST);

        Map<String, Object> payloadVh1 = new HashMap<>();
        payloadVh1.put(prefType1, Collections.singletonList(vh1Pref1Attributes));
        getRestTestHelper().submitRequest(vh1PostUrl, "POST", payloadVh1, HttpServletResponse.SC_OK);

        Map<String, Object> payloadVh2 = new HashMap<>();
        payloadVh2.put(prefType1, Lists.newArrayList(vh2Pref1Attributes));
        payloadVh2.put(prefType2, Lists.newArrayList(vh2Pref2Attributes));
        getRestTestHelper().submitRequest(vh2PostUrl, "POST", payloadVh2, HttpServletResponse.SC_OK);

        Map<String, Object> payloadVh3 = new HashMap<>();
        payloadVh3.put(prefType1, Lists.newArrayList(vh3Pref1Attributes));
        getRestTestHelper().submitRequest(vh3PostUrl, "POST", payloadVh3, HttpServletResponse.SC_OK);

        {
            String wildGetUrlAll = "virtualhost/*/*/userpreferences";
            final List<Map<String, List<Map<String, Object>>>> vhTypeMaps =
                    getRestTestHelper().getJson(wildGetUrlAll, List.class);
            assertEquals("Unexpected number of virtualhost preference type maps", 3, vhTypeMaps.size());

            Set<Map<String, Object>> allPrefs = new HashSet<>();
            for (Map<String, List<Map<String, Object>>> vhTypeMap : vhTypeMaps)
            {
                for (List<Map<String, Object>> prefList : vhTypeMap.values())
                {
                    allPrefs.addAll(prefList);
                }
            }

            assertEquals("Unexpected number of preferences in response", 4, allPrefs.size());

            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref1Name, allPrefs, 2);
            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref2Name, allPrefs, 1);
            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref3Name, allPrefs, 1);
        }

        {
            String wildGetUrlByType = String.format("virtualhost/*/*/userpreferences/%s", prefType1);

            final List<List<Map<String, Object>>> vhListPrefs = getRestTestHelper().getJson(wildGetUrlByType, List.class);
            assertEquals("Unexpected number of virtualhost preference lists", 3, vhListPrefs.size());

            Set<Map<String, Object>> allPrefs = new HashSet<>();
            for (List<Map<String, Object>> prefList : vhListPrefs)
            {
                allPrefs.addAll(prefList);
            }

            assertEquals("Unexpected number of preferences in response", 3, allPrefs.size());

            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref1Name, allPrefs, 2);
            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref2Name, allPrefs, 1);
        }

        {
            String wildGetUrlByTypeAndName = String.format("virtualhost/*/*/userpreferences/%s/%s", prefType1, pref1Name);

            final List<Map<String, Object>> vhPrefs = getRestTestHelper().getJson(wildGetUrlByTypeAndName, List.class);
            assertEquals("Unexpected number of virtualhost preference lists", 2, vhPrefs.size());

            Set<Map<String, Object>> allPrefs = new HashSet<>();
            for (Map<String, Object> prefs : vhPrefs)
            {
                allPrefs.add(prefs);
            }

            assertEquals("Unexpected number of preferences in response", 2, allPrefs.size());

            assertContainsPreference(Preference.NAME_ATTRIBUTE, pref1Name, allPrefs, 2);
        }
    }

    private void assertContainsPreference(final String attribute, final String expected,
                                          final Set<Map<String, Object>> preferences, final int expectedCount)
    {
        Set<Map<String, Object>> found = Sets.filter(preferences, new AttributeMatchingPredicate(attribute, expected));
        assertEquals(String.format("Cannot find expected preference with attribute %s : %s", attribute, expected),
                     expectedCount, found.size());
    }

    private static class AttributeMatchingPredicate implements Predicate<Map<String, Object>>
    {
        private final String _expectedName;
        private final String _attribute;

        public AttributeMatchingPredicate(String attribute, String expectedName)
        {
            _expectedName = expectedName;
            _attribute = attribute;
        }

        @Override
        public boolean apply(final Map<String, Object> input)
        {
            return _expectedName.equals(input.get(_attribute));
        }
    }
}
