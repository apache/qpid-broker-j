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
package org.apache.qpid.disttest.controller.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class JavaScriptConfigEvaluatorTest extends UnitTestBase
{
    private void performTest(Map configAsObject)
    {
        // Tests are produced by the QPID.iterations js function
        List<?> countries = getPropertyAsList(configAsObject, "_countries");
        assertEquals(2, (long) countries.size(), "Unexpected number of countries");

        Map country0 = (Map) countries.get(0);
        assertEquals("Country", country0.get("_name"), "Unexpected country name");
        assertEquals(0, (long) ((Number) country0.get("_iterationNumber")).intValue(),
                "Unexpected country iteration number");

        List<?> regions = getPropertyAsList(country0, "_regions");
        assertEquals(2, (long) regions.size(), "Unexpected number of regions");
        // Region names are produced by the QPID.times js function
        Map region0 = (Map) regions.get(0);
        assertEquals("repeatingRegion0", region0.get("_name"), "Unexpected region name");
        assertEquals("repeatingRegion1", ((Map)regions.get(1)).get("_name"), "Unexpected region name");
        // Iterating attribute are produced by the QPID.iterations js function
        assertEquals("0", ((Map)((List)region0.get("_towns")).get(0)).get("_iteratingAttribute"),
                "Unexpected iterating attribute");

        Map country1 = (Map) countries.get(1);
        regions = getPropertyAsList(country1, "_regions");
        region0 = (Map) regions.get(0);
        assertEquals(1, ((Number) country1.get("_iterationNumber")).intValue(),
                "Unexpected country iteration number");
        assertEquals("1", ((Map)((List)region0.get("_towns")).get(0)).get("_iteratingAttribute"),
                "Unexpected iterating attribute");
    }

    @Test
    public void testEvaluateJavaScript() throws Exception
    {
        String jsFilePath = TestFileUtils.createTempFileFromResource(this, "JavaScriptConfigEvaluatorTest-test-config.js").getAbsolutePath();

        String rawConfig = new JavaScriptConfigEvaluator().evaluateJavaScript(jsFilePath);

        Map configAsObject = getObject(rawConfig);
        performTest(configAsObject);
    }

    @Test
    public void testEvaluateJavaScriptWithReader() throws Exception
    {
        String jsFilePath = TestFileUtils.createTempFileFromResource(this, "JavaScriptConfigEvaluatorTest-test-config.js").getAbsolutePath();

        FileReader fileReader = new FileReader(jsFilePath);
        String rawConfig = new JavaScriptConfigEvaluator().evaluateJavaScript(fileReader);

        Map configAsObject = getObject(rawConfig);
        performTest(configAsObject);
    }

    private List<?> getPropertyAsList(Map configAsMap, String property)
    {
        return (List<?>)configAsMap.get(property);
    }

    private Map getObject(String jsonStringIn) throws Exception
    {
        final ObjectMapper objectMapper = JsonMapper.builder()
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS, true)
                .build();
        return objectMapper.readValue(jsonStringIn, TreeMap.class);
    }
}
