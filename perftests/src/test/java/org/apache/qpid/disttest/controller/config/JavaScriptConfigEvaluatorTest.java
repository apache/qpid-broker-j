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

import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Assert;

import org.apache.qpid.test.utils.TestFileUtils;


import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JavaScriptConfigEvaluatorTest extends UnitTestBase
{
    private void performTest(Map configAsObject) throws Exception
    {
        // Tests are produced by the QPID.iterations js function
        List<?> countries = getPropertyAsList(configAsObject, "_countries");
        assertEquals("Unexpected number of countries", (long) 2, (long) countries.size());

        Map country0 = (Map) countries.get(0);
        assertEquals("Unexpected country name", "Country", country0.get("_name"));
        assertEquals("Unexpected country iteration number",
                            (long) 0,
                            (long) ((Number) country0.get("_iterationNumber")).intValue());

        List<?> regions = getPropertyAsList(country0, "_regions");
        assertEquals("Unexpected number of regions", (long) 2, (long) regions.size());
        // Region names are produced by the QPID.times js function
        Map region0 = (Map) regions.get(0);
        assertEquals("Unexpected region name", "repeatingRegion0", region0.get("_name"));
        assertEquals("Unexpected region name", "repeatingRegion1", ((Map)regions.get(1)).get("_name"));
        // Iterating attribute are produced by the QPID.iterations js function
        assertEquals("Unexpected iterating attribute",
                            "0",
                            ((Map)((List)region0.get("_towns")).get(0)).get("_iteratingAttribute"));

        Map country1 = (Map) countries.get(1);
        regions = getPropertyAsList(country1, "_regions");
        region0 = (Map) regions.get(0);
        assertEquals("Unexpected country iteration number",
                            (long) 1,
                            (long) ((Number) country1.get("_iterationNumber")).intValue());
        assertEquals("Unexpected iterating attribute",
                            "1",
                            ((Map)((List)region0.get("_towns")).get(0)).get("_iteratingAttribute"));
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
            throws Exception
    {
        return (List<?>)configAsMap.get(property);
    }

    private Map getObject(String jsonStringIn) throws Exception
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        return objectMapper.readValue(jsonStringIn, TreeMap.class);
    }
}
