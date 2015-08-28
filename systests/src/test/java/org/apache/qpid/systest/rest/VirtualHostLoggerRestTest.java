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
package org.apache.qpid.systest.rest;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.logging.VirtualHostFileLogger;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHostLogger;
import org.apache.qpid.util.LogMonitor;

public class VirtualHostLoggerRestTest extends QpidRestTestCase
{

    public void testCreateVirtualHostLoggerAndRule() throws Exception
    {
        String loggerName = "testLogger1";
        String loggerRestUrlBase = "virtualhostlogger/" + TEST1_VIRTUALHOST + "/" + TEST1_VIRTUALHOST;
        String loggerRestUrl = loggerRestUrlBase + "/" + loggerName;

        Map<String, Object> virtualHostLoggerAttributes = new HashMap<>();
        virtualHostLoggerAttributes.put(VirtualHostLogger.NAME, loggerName);
        virtualHostLoggerAttributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        getRestTestHelper().submitRequest(loggerRestUrlBase, "PUT", virtualHostLoggerAttributes, HttpServletResponse.SC_CREATED);
        Map<String, Object> loggerData = getRestTestHelper().getJsonAsSingletonList(loggerRestUrl);

        String loggerRuleName = "testRule";
        Map<String, Object> virtualHostRuleAttributes = new HashMap<>();
        virtualHostRuleAttributes.put("name", loggerRuleName);
        virtualHostRuleAttributes.put("level", "DEBUG");

        String loggerRuleRestUrlBase = "virtualhostloginclusionrule/" + TEST1_VIRTUALHOST + "/" + TEST1_VIRTUALHOST + "/" + loggerName;
        String loggerRuleRestUrl = loggerRuleRestUrlBase + "/" + loggerRuleName;

        getRestTestHelper().submitRequest(loggerRuleRestUrlBase, "PUT", virtualHostRuleAttributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest(loggerRuleRestUrl, "GET", HttpServletResponse.SC_OK);

        String logFileLocation = String.valueOf(loggerData.get(VirtualHostFileLogger.FILE_NAME));
        assertNotNull("Log file attribute is not set ", logFileLocation);
        assertTrue(String.format("Log file '%s' does not exists", logFileLocation), new File(logFileLocation).exists());


        LogMonitor logMonitor = new LogMonitor(new File(logFileLocation));
        try
        {
            List<String> logs = logMonitor.findMatches("QUE-1001");
            assertEquals("Unexpected number of Queue creation operational logs before Queue creation", 0, logs.size());
            Map<String, Object> queueAttributes = Collections.<String, Object>singletonMap(Queue.NAME, getTestQueueName());
            getRestTestHelper().submitRequest("queue/" + TEST1_VIRTUALHOST + "/" + TEST1_VIRTUALHOST, "PUT", queueAttributes, HttpServletResponse.SC_CREATED);
            logs = logMonitor.waitAndFindMatches("QUE-1001", 1000);
            assertEquals("Unexpected number of Queue creation operational logs after Queue creation", 1, logs.size());
        }
        finally
        {
            logMonitor.close();
        }
    }

    public void testDeleteVirtualHostLoggerAndRule() throws Exception
    {
        String loggerName = "testLogger1";
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHostLogger.NAME, loggerName);
        attributes.put(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);
        String loggerRestUrlBase = "virtualhostlogger/" + TEST1_VIRTUALHOST + "/" + TEST1_VIRTUALHOST;
        String loggerRestUrl = loggerRestUrlBase + "/" + loggerName;

        getRestTestHelper().submitRequest(loggerRestUrlBase, "PUT", attributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest(loggerRestUrl, "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().submitRequest(loggerRestUrl, "DELETE", null, HttpServletResponse.SC_OK);
        getRestTestHelper().submitRequest(loggerRestUrl, "GET", HttpServletResponse.SC_NOT_FOUND);
    }

}
