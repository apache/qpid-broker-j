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
package org.apache.qpid.tests.http.endtoend.statistics;

import static java.util.Collections.singletonMap;
import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.Session;

import com.google.common.io.CharStreams;
import org.junit.Test;

import org.apache.qpid.server.logging.logback.BrokerFileLogger;
import org.apache.qpid.server.logging.logback.BrokerNameAndLevelLogInclusionRule;
import org.apache.qpid.server.logging.logback.VirtualHostFileLogger;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRule;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

@HttpRequestConfig()
public class StatisticsReportingTest extends HttpTestBase
{
    private static final long STATISTICS_REPORTING_PERIOD_IN_SEC = 1L;
    private static final long LOG_TIMEOUT_IN_MS = STATISTICS_REPORTING_PERIOD_IN_SEC * 1000 * 10;
    private static final String QUEUE1_NAME = "queue1";
    private static final String QUEUE2_NAME = "queue2";

    @Test
    public void virtualHostStatisticsReporting() throws Exception
    {
        String hostLogDownloadUrl = configureLogger(true);

        Connection conn = getConnection();
        try
        {
            getBrokerAdmin().createQueue(QUEUE1_NAME);
            Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

            // Enable Virtual Host Statistics Reporting
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put(QueueManagingVirtualHost.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);
            Map<String, String> context = singletonMap("qpid.queue.statisticsReportPattern",
                                                       "${ancestor:virtualhost:name}/${ancestor:queue:name}: "
                                                       + "queueDepthMessages=${queueDepthMessages}, "
                                                       + "queueDepthBytes=${queueDepthBytes:byteunit}");
            arguments.put(ConfiguredObject.CONTEXT, context);

            getHelper().submitRequest("virtualhost", "POST", arguments, SC_OK);

            assertThat("Pre-enqueue queue1 statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl,
                                           String.format("%s/%s: queueDepthMessages=0, queueDepthBytes=0 B",
                                                         getVirtualHost(),
                                                         QUEUE1_NAME), LOG_TIMEOUT_IN_MS), is(greaterThan(0)));

            // Enqueue a single message to queue 1
            Utils.sendMessages(session, session.createQueue(QUEUE1_NAME), 1);

            assertThat("Post-enqueue queue1 statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl, String.format("%s/%s: queueDepthMessages=1",
                                                                              getVirtualHost(),
                                                                              QUEUE1_NAME), LOG_TIMEOUT_IN_MS), is(greaterThan(0)));

            getBrokerAdmin().createQueue(QUEUE2_NAME);

            assertThat("Initial queue2 statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl,
                                           String.format("%s/%s: queueDepthMessages=0, queueDepthBytes=0 B",
                                                         getVirtualHost(),
                                                         QUEUE2_NAME), LOG_TIMEOUT_IN_MS), is(greaterThan(0)));


            // Override the statistic report for queue2 only
            Map<String, String> queue2Context = singletonMap("qpid.queue.statisticsReportPattern",
                                                             "${ancestor:virtualhost:name}/${ancestor:queue:name}: "
                                                             + "oldestMessageAge=${oldestMessageAge:duration}");

            getHelper().submitRequest(String.format("queue/%s", QUEUE2_NAME), "POST", singletonMap(ConfiguredObject.CONTEXT, queue2Context), SC_OK);

            assertThat("Post-enqueue queue2 statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl, String.format("%s/%s: oldestMessageAge=PT",
                                                                              getVirtualHost(),
                                                                              QUEUE2_NAME), LOG_TIMEOUT_IN_MS), is(greaterThan(0)));
        }
        finally
        {
            conn.close();
        }
    }

    @Test
    public void virtualHostConnectionStatistics() throws Exception
    {
        String hostLogDownloadUrl = configureLogger(true);

        HttpTestHelper brokerHelper = new HttpTestHelper(getBrokerAdmin());
        Connection conn = getConnection();

        try
        {
            final Map<String, String> args = new HashMap<>();
            args.put("name", "qpid.connection.statisticsReportPattern");
            args.put("value", "${ancestor:connection:principal}: messagesIn=${messagesIn}, lastIoTime=${lastIoTime:datetime}");
            brokerHelper.submitRequest("broker/setContextVariable", "POST", args, SC_OK);

            getBrokerAdmin().createQueue(QUEUE1_NAME);
            Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

            // Enable Virtual Host Statistics Reporting
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put(QueueManagingVirtualHost.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);
            Map<String, String> context = singletonMap("qpid.connection.statisticsReportPattern",
                                                                   "${ancestor:connection:principal}: "
                                                                   + "messagesIn=${messagesIn}, "
                                                                   + "lastIoTime=${lastIoTime:datetime}");
            arguments.put(ConfiguredObject.CONTEXT, context);

            getHelper().submitRequest("virtualhost", "POST", arguments, SC_OK);

            assertThat("Pre-enqueue connection statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl,
                                           String.format("%s: messagesIn=0", getBrokerAdmin().getValidUsername()),
                                           LOG_TIMEOUT_IN_MS),
                       is(greaterThan(0)));

            // Enqueue a single message to queue 1
            Utils.sendMessages(session, session.createQueue(QUEUE1_NAME), 1);

            assertThat("Post-enqueue connection statistics report not found",
                       countLogFileMatches(hostLogDownloadUrl,
                                           String.format("%s: messagesIn=1", getBrokerAdmin().getValidUsername()),
                                           LOG_TIMEOUT_IN_MS),
                                                         is(greaterThan(0)));
        }
        finally
        {
            brokerHelper.submitRequest("broker/removeContextVariable", "POST",
                                       singletonMap("name", "qpid.connection.statisticsReportPattern"), SC_OK);
            conn.close();
        }
    }

    @Test
    @HttpRequestConfig(useVirtualHostAsHost = false)
    public void brokerStatistics() throws Exception
    {
        String logDownloadUrl = configureLogger(false);

        Connection conn = getConnection();

        try
        {
            final Map<String, Object> args1 = new HashMap<>();
            args1.put("name", "qpid.broker.statisticsReportPattern");
            args1.put("value", "messagesIn=${messagesIn}");
            getHelper().submitRequest("broker/setContextVariable", "POST", args1, SC_OK);

            final Map<String, Object> attrs = Collections.singletonMap(Broker.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);
            getHelper().submitRequest("broker", "POST", attrs, SC_OK);

            getBrokerAdmin().createQueue(QUEUE1_NAME);
            Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

            assertThat("Pre-enqueue statistics report not found",
                       countLogFileMatches(logDownloadUrl, "messagesIn=0", LOG_TIMEOUT_IN_MS),
                       is(greaterThan(0)));

            // Enqueue a single message to queue 1
            Utils.sendMessages(session, session.createQueue(QUEUE1_NAME), 1);

            assertThat("Post-enqueue statistics report not found",
                       countLogFileMatches(logDownloadUrl, "messagesIn=1", LOG_TIMEOUT_IN_MS),
                       is(greaterThan(0)));
        }
        finally
        {
            getHelper().submitRequest("broker/removeContextVariable", "POST",
                                       singletonMap("name", "qpid.broker.statisticsReportPattern"), SC_OK);
            getHelper().submitRequest("broker/removeContextVariable", "POST",
                                       singletonMap("name", Broker.STATISTICS_REPORTING_PERIOD), SC_OK);
            getHelper().submitRequest("brokerlogger/statslogger", "DELETE", SC_OK);
            conn.close();
        }
    }

    private int countLogFileMatches(final String logDownloadUrl, final String searchTerm, final long timeout)
            throws Exception
    {
        final long endTime = System.currentTimeMillis() + timeout;
        int matches;
        do
        {
            matches = countLogFileMatches(logDownloadUrl, searchTerm);

            if (matches == 0)
            {
                Thread.sleep(100);
            }
        }
        while (matches == 0 && endTime > System.currentTimeMillis());
        return matches;
    }

    private String configureLogger(final boolean virtualHost) throws Exception
    {
        final String loggerUrl;
        final String loggerRuleUrl;
        final String loggerType;
        final String loggerFileNameAttr;
        final String loggerInclusionRuleType;
        final String loggerNameAttr;

        if (virtualHost)
        {
            loggerUrl = "virtualhostlogger/statslogger";
            loggerRuleUrl = "virtualhostloginclusionrule/statslogger/rule";
            loggerType = VirtualHostFileLogger.TYPE;
            loggerInclusionRuleType = VirtualHostNameAndLevelLogInclusionRule.TYPE;
            loggerNameAttr = VirtualHostNameAndLevelLogInclusionRule.LOGGER_NAME;
            loggerFileNameAttr = VirtualHostFileLogger.FILE_NAME;

        }
        else
        {
            loggerUrl = "brokerlogger/statslogger";
            loggerRuleUrl = "brokerloginclusionrule/statslogger/rule";
            loggerType = BrokerFileLogger.TYPE;
            loggerFileNameAttr = BrokerFileLogger.FILE_NAME;
            loggerInclusionRuleType = BrokerNameAndLevelLogInclusionRule.TYPE;
            loggerNameAttr = BrokerNameAndLevelLogInclusionRule.LOGGER_NAME;
        }

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(ConfiguredObject.TYPE, loggerType);

        getHelper().submitRequest(loggerUrl, "PUT", loggerAttributes, SC_CREATED);

        Map<String, Object> ruleAttributes = new HashMap<>();
        ruleAttributes.put(ConfiguredObject.TYPE, loggerInclusionRuleType);
        ruleAttributes.put(loggerNameAttr, "qpid.statistics.*");

        getHelper().submitRequest(loggerRuleUrl, "PUT", ruleAttributes, SC_CREATED);

        Map<String, Object> loggerData = getHelper().getJsonAsMap(loggerUrl);
        String logFileLocation = String.valueOf(loggerData.get(loggerFileNameAttr));
        assertThat(logFileLocation, is(notNullValue()));
        final File logFile = new File(logFileLocation);

        return String.format("%s/getFile?fileName=%s", loggerUrl, logFile.getName());
    }

    private int countLogFileMatches(final String url, final String searchTerm) throws Exception
    {
        HttpURLConnection httpCon = getHelper().openManagementConnection(url, "GET");
        httpCon.connect();

        try (InputStreamReader r = new InputStreamReader(httpCon.getInputStream()))
        {
            final List<String> strings = CharStreams.readLines(r);
            return strings.stream()
                          .map(line -> line.contains(searchTerm))
                          .filter(found -> found)
                          .collect(Collectors.toList()).size();
        }
    }
}
