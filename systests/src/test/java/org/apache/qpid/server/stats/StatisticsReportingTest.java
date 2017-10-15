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
package org.apache.qpid.server.stats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Session;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.util.LogMonitor;

public class StatisticsReportingTest extends QpidBrokerTestCase
{
    private static final long STATISTICS_REPORTING_PERIOD_IN_SEC = 1L;
    private static final long LOG_TIMEOUT_IN_MS = STATISTICS_REPORTING_PERIOD_IN_SEC * 1000 * 10;
    private final ObjectMapper _objectMapper = new ObjectMapper();

    private LogMonitor _monitor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _monitor = new LogMonitor(getOutputFile());
    }

    public void testBrokerStatistics() throws Exception
    {
        // Enable Broker Statistics Reporting for the Broker
        Map<String, String> context = Collections.singletonMap("qpid.broker.statisticsReportPattern", "messagesIn=${messagesIn}");

        final Map<String, Object> brokerArguments = new HashMap<>();
        brokerArguments.put(Broker.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);
        brokerArguments.put(ConfiguredObject.CONTEXT, _objectMapper.writeValueAsString(context));

        updateBroker(brokerArguments);

        // The broker will count the management message.
        boolean found = _monitor.waitForMessage("messagesIn=1", LOG_TIMEOUT_IN_MS);
        assertTrue(String.format("Statistics message not found in log file within timeout %d", LOG_TIMEOUT_IN_MS), found);
    }

    public void testVirtualHostStatistics() throws Exception
    {
        Connection conn = getConnection();
        conn.start();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

        // Enable Virtual Host Statistics Reporting
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(Broker.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);
        Map<String, String> context = Collections.singletonMap("qpid.queue.statisticsReportPattern",
                                                               "${ancestor:virtualhost:name}/${ancestor:queue:name}: queueDepthMessages=${queueDepthMessages}, queueDepthBytes=${queueDepthBytes:byteunit}");
        arguments.put(ConfiguredObject.CONTEXT, _objectMapper.writeValueAsString(context));

        updatenEntityUsingAmqpManagement("test", session, "org.apache.qpid.VirtualHost", arguments);

        Queue queue1 = createTestQueue(session, "queue1");

        assertTrue("Initial queue1 statistics report not found",
                   _monitor.waitForMessage("test/queue1: queueDepthMessages=0, queueDepthBytes=0 B",
                                           LOG_TIMEOUT_IN_MS));

        // Enqueue a single message to queue 1
        sendMessage(session, queue1, 1);
        assertTrue("Post-enqeue queue1 statistics report not found",
                   _monitor.waitForMessage("test/queue1: queueDepthMessages=1", LOG_TIMEOUT_IN_MS));

        Queue queue2 = createTestQueue(session, "queue2");

        assertTrue("Initial queue2 statistics report not found",
                   _monitor.waitForMessage("test/queue2: queueDepthMessages=0, queueDepthBytes=0 B",
                                           LOG_TIMEOUT_IN_MS));

        // Override the statistic report for queue2 only
        Map<String, String> queueContext = Collections.singletonMap("qpid.queue.statisticsReportPattern",
                                                                    "${ancestor:virtualhost:name}/${ancestor:queue:name}: oldestMessageAge=${oldestMessageAge:duration}");
        updatenEntityUsingAmqpManagement("queue2",
                                         session,
                                         "org.apache.qpid.Queue",
                                         Collections.singletonMap(ConfiguredObject.CONTEXT,
                                                                  _objectMapper.writeValueAsString(queueContext)));

        sendMessage(session, queue1, 1);

        assertTrue("Post-enqueue queue2 statistics report not found",
                   _monitor.waitForMessage("test/queue2: oldestMessageAge=PT", LOG_TIMEOUT_IN_MS));
    }

    public void testVirtualHostConnectionStatistics() throws Exception
    {
        // Configure statistic reporting pattern at the Broker.
        final Map<String, Object> brokerArguments = new HashMap<>();
        brokerArguments.put(ConfiguredObject.CONTEXT, _objectMapper.writeValueAsString(Collections.singletonMap("qpid.connection.statisticsReportPattern",
                                                                                                                "${ancestor:connection:principal}: messagesIn=${messagesIn}, lastIoTime=${lastIoTime:datetime}")));

        updateBroker(brokerArguments);

        final Connection conn = getConnection();
        conn.start();
        final Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

        // Enable Virtual Host Statistics Reporting
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(Broker.STATISTICS_REPORTING_PERIOD, STATISTICS_REPORTING_PERIOD_IN_SEC);

        updatenEntityUsingAmqpManagement("test",
                                         session,
                                         "org.apache.qpid.VirtualHost",
                                         arguments);

        assertTrue("Post-enqueue queue1 statistics report not found",
                   _monitor.waitForMessage("guest: messagesIn=1", LOG_TIMEOUT_IN_MS));
    }

    private void updateBroker(final Map<String, Object> arguments) throws Exception
    {
        Connection conn = getConnectionForVHost("$management");
        conn.start();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        updatenEntityUsingAmqpManagement("Broker", session, "org.apache.qpid.Broker", arguments);
        conn.close();
    }
}
