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
package org.apache.qpid.tests.http.endtoend.logging;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.io.CharStreams;
import org.junit.Test;

import org.apache.qpid.server.logging.logback.VirtualHostFileLogger;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRule;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig(useVirtualHostAsHost = true)
public class VirtualHostLoggerTest extends HttpTestBase
{
    private static final String LOGGER_NAME = "testLogger1";
    private static final String LOGGER_RULE = "testRule";

    @Test
    public void createVirtualHostLoggerAndRule() throws Exception
    {
        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(ConfiguredObject.NAME, LOGGER_NAME);
        loggerAttributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        getHelper().submitRequest(String.format("virtualhostlogger/%s", LOGGER_NAME),
                                  "PUT",
                                  loggerAttributes,
                                  SC_CREATED);

        Map<String, Object> ruleAttributes = new HashMap<>();
        ruleAttributes.put(ConfiguredObject.NAME, LOGGER_RULE);
        ruleAttributes.put(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);

        String loggerRuleRestUrl = String.format("virtualhostloginclusionrule/%s/%s", LOGGER_NAME, LOGGER_RULE);

        getHelper().submitRequest(loggerRuleRestUrl, "PUT", ruleAttributes, SC_CREATED);
        getHelper().submitRequest(loggerRuleRestUrl, "GET", SC_OK);
    }

    @Test
    public void dynamicConfiguration() throws Exception
    {
        final String loggerUrl = String.format("virtualhostlogger/%s", LOGGER_NAME);
        final String loggerRuleUrl = String.format("virtualhostloginclusionrule/%s/%s", LOGGER_NAME, LOGGER_RULE);

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(ConfiguredObject.NAME, LOGGER_NAME);
        loggerAttributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        getHelper().submitRequest(loggerUrl,
                                  "PUT",
                                  loggerAttributes,
                                  SC_CREATED);

        Map<String, Object> ruleAttributes = new HashMap<>();
        ruleAttributes.put(ConfiguredObject.NAME, LOGGER_RULE);
        ruleAttributes.put(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
        ruleAttributes.put("loggerName", "qpid.message.*");

        getHelper().submitRequest(loggerRuleUrl, "PUT", ruleAttributes, SC_CREATED);
        getHelper().submitRequest(loggerRuleUrl, "GET", SC_OK);

        Map<String, Object> loggerData = getHelper().getJsonAsMap(loggerUrl);
        String logFileLocation = String.valueOf(loggerData.get(VirtualHostFileLogger.FILE_NAME));
        assertThat(logFileLocation, is(notNullValue()));
        final File logFile = new File(logFileLocation);

        final String downloadUrl = String.format("%s/getFile?fileName=%s", loggerUrl, logFile.getName());

        final int queueCreateMatchesBefore = countLogFileMatches(downloadUrl, "QUE-1001");
        assertThat("Unexpected queue matches before queue creation", queueCreateMatchesBefore, is(equalTo(0)));

        getHelper().submitRequest("queue/myqueue", "PUT", Collections.emptyMap(), SC_CREATED);

        final int queueCreateMatchesAfter = countLogFileMatches(downloadUrl, "QUE-1001");
        assertThat("Unexpected queue matches before queue creation", queueCreateMatchesAfter, is(equalTo(1)));

        ruleAttributes.put("level", "OFF");
        getHelper().submitRequest(loggerRuleUrl, "PUT", ruleAttributes, SC_OK);

        getHelper().submitRequest("queue/myqueue2", "PUT", Collections.emptyMap(), SC_CREATED);

        final int afterLevelChange = countLogFileMatches(downloadUrl, "QUE-1001");
        assertThat("Unexpected queue matches after level change", afterLevelChange, is(equalTo(queueCreateMatchesAfter)));

        ruleAttributes.put("level", "INFO");
        getHelper().submitRequest(loggerRuleUrl, "PUT", ruleAttributes, SC_OK);

        getHelper().submitRequest("queue/myqueue3", "PUT", Collections.emptyMap(), SC_CREATED);

        final int afterSecondLevelChange = countLogFileMatches(downloadUrl, "QUE-1001");
        assertThat("Unexpected queue matches after level change", afterSecondLevelChange, is(equalTo(afterLevelChange + 1)));
    }

    @Test
    public void deleteVirtualHostLogger() throws Exception
    {
        final String loggerRestUrl = String.format("virtualhostlogger/%s", LOGGER_NAME);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, LOGGER_NAME);
        attributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        getHelper().submitRequest(loggerRestUrl, "PUT", attributes, SC_CREATED);
        getHelper().submitRequest(loggerRestUrl, "GET", SC_OK);

        getHelper().submitRequest(loggerRestUrl, "DELETE", null, SC_OK);
        getHelper().submitRequest(loggerRestUrl, "GET", SC_NOT_FOUND);
    }

    @Test
    public void recoverAfterDeletedVirtualHostLoggerAndRules_QPID_8066() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        final String hostUrl = "virtualhost";
        final String loggerUrl = String.format("virtualhostlogger/%s", LOGGER_NAME);
        final String loggerRuleUrl = String.format("virtualhostloginclusionrule/%s/%s", LOGGER_NAME, LOGGER_RULE);

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(ConfiguredObject.NAME, LOGGER_NAME);
        loggerAttributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);
        getHelper().submitRequest(loggerUrl, "PUT", loggerAttributes, SC_CREATED);

        Map<String, Object> ruleAttributes = new HashMap<>();
        ruleAttributes.put(ConfiguredObject.NAME, LOGGER_RULE);
        ruleAttributes.put(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
        getHelper().submitRequest(loggerRuleUrl, "PUT", ruleAttributes, SC_CREATED);

        Map<String, Object> host = getHelper().getJsonAsMap(hostUrl);
        assertThat(host.get(ConfiguredObject.STATE), is(equalTo(State.ACTIVE.name())));
        assertThat(getHelper().getJsonAsList("virtualhostlogger").size(), is(equalTo(1)));

        getHelper().submitRequest(loggerUrl, "DELETE", SC_OK);

        getBrokerAdmin().restart();

        host = getHelper().getJsonAsMap(hostUrl);
        assertThat(host.get(ConfiguredObject.STATE), is(equalTo(State.ACTIVE.name())));

        assertThat(getHelper().getJsonAsList("virtualhostlogger").size(), is(equalTo(0)));
    }

    private int countLogFileMatches(final String url, final String searchTerm) throws Exception
    {
        HttpURLConnection httpCon = getHelper().openManagementConnection(url, "GET");
        httpCon.connect();

        try (InputStreamReader r = new InputStreamReader(httpCon.getInputStream()))
        {
            final List<String> strings = CharStreams.readLines(r);
            return strings.stream().map(line -> line.contains(searchTerm)).collect(Collectors.toList()).size();
        }
    }
}
