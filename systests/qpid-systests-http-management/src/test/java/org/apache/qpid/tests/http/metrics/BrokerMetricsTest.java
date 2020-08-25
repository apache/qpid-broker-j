package org.apache.qpid.tests.http.metrics;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.QUEUE_NAME;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.assertMetricsInclusion;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.assertVirtualHostHierarchyMetrics;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.createQueueMetricPattern;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig(useVirtualHostAsHost = false)
public class BrokerMetricsTest extends HttpTestBase
{
    private static final String[] EXPECTED_BROKER_METRIC_NAMES =
            new String[]{"qpid_broker_inbound_bytes_count", "qpid_broker_outbound_bytes_count"};

    @Test
    public void testBrokerMetrics() throws Exception
    {
        final String[] unexpectedMetricNames =
                {"qpid_broker_live_threads_total", "qpid_broker_direct_memory_capacity_bytes_total"};

        final byte[] metricsBytes = getHelper().getBytes("/metrics");
        final String metricsString = new String(metricsBytes, UTF_8);
        assertMetricsInclusion(metricsString, EXPECTED_BROKER_METRIC_NAMES, true);
        assertMetricsInclusion(metricsString, unexpectedMetricNames, false);

        final byte[] metricsBytesIncludingDisabled = getHelper().getBytes("/metrics?includeDisabled=true");
        final String metricsStringIncludingDisabled = new String(metricsBytesIncludingDisabled, UTF_8);
        assertMetricsInclusion(metricsStringIncludingDisabled, unexpectedMetricNames, true);
        assertMetricsInclusion(metricsStringIncludingDisabled, EXPECTED_BROKER_METRIC_NAMES, true);
    }

    @Test
    public void testQueueMetrics() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes = getHelper().getBytes("/metrics");
        final String metricsString = new String(metricsBytes, UTF_8);

        final Pattern[] expectedMetricPattens = {createQueueMetricPattern("qpid_queue_consumers_total"),
                createQueueMetricPattern("qpid_queue_depth_messages_total")};

        assertMetricsInclusion(metricsString, expectedMetricPattens, true);
    }

    @Test
    public void testQueueMetricsIncludeOnlyMessageDepth() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes = getHelper().getBytes("/metrics?name[]=qpid_queue_depth_messages_total&name[]=qpid_queue_depth_bytes_total");
        Collection<String> metricLines = TestMetricsHelper.getMetricLines(metricsBytes);
        assertThat(metricLines.size(), is(equalTo(2)));

        final String metricsString = new String(metricsBytes, UTF_8);
        final Pattern[] expectedMetricPattens = {createQueueMetricPattern("qpid_queue_depth_bytes_total"),
                createQueueMetricPattern("qpid_queue_depth_messages_total")};

        assertMetricsInclusion(metricsString, expectedMetricPattens, true);
    }

    @Test
    public void testMappingForVirtualHost() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes =
                getHelper().getBytes(String.format("/metrics/%s/%s", getVirtualHostNode(), getVirtualHost()));

        assertVirtualHostHierarchyMetrics(metricsBytes);
    }
}
