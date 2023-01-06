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
package org.apache.qpid.server.logging.messages;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;

/**
 * Test BRK log Messages
 */
public class BrokerMessagesTest extends AbstractTestMessages
{
    @Test
    public void testBrokerStartup()
    {
        final String version = "Qpid 0.6";
        final String build = "796936M";
        _logMessage = BrokerMessages.STARTUP(version, build);
        final List<Object> log = performLog();
        final String[] expected = {"Startup :", "Version:", version, "Build:", build};

        validateLogMessage(log, "BRK-1001", expected);
    }

    @Test
    public void testBrokerListening()
    {
        final String transport = "TCP";
        final Integer port = 2765;

        _logMessage = BrokerMessages.LISTENING(transport, port);

        final List<Object> log = performLog();

        final String[] expected = {"Starting", "Listening on ", transport, "port ", String.valueOf(port)};

        validateLogMessage(log, "BRK-1002", expected);
    }

    @Test
    public void testBrokerShuttingDown()
    {
        final String transport = "TCP";
        final Integer port = 2765;

        _logMessage = BrokerMessages.SHUTTING_DOWN(transport, port);

        final List<Object> log = performLog();

        final String[] expected = { "Shutting down", transport, "port ", String.valueOf(port) };

        validateLogMessage(log, "BRK-1003", expected);
    }

    @Test
    public void testBrokerReady()
    {
        _logMessage = BrokerMessages.READY();
        final List<Object> log = performLog();

        final String[] expected = {"Ready"};

        validateLogMessage(log, "BRK-1004", expected);
    }

    @Test
    public void testBrokerStopped()
    {
        _logMessage = BrokerMessages.STOPPED();
        final List<Object> log = performLog();

        final String[] expected = {"Stopped"};

        validateLogMessage(log, "BRK-1005", expected);
    }

    @Test
    public void testBrokerConfig()
    {
        final String path = "/file/path/to/configuration.xml";

        _logMessage = BrokerMessages.CONFIG(path);
        final List<Object> log = performLog();

        final String[] expected = {"Using configuration :", path};

        validateLogMessage(log, "BRK-1006", expected);
    }

    @Test
    public void testBrokerPlatform()
    {
        final String javaVendor = "jvendor";
        final String javaVersion = "j1.0";
        final String osName = "os";
        final String osVersion = "o1.0";
        final String osArch = "oarch";
        final String cores = "2";

        _logMessage = BrokerMessages.PLATFORM(javaVendor, javaVersion, osName, osVersion, osArch, cores);
        final List<Object> log = performLog();

        final String[] expected = {"Platform :", "JVM :", javaVendor, " version: ", " OS : ", osName, " version: ", osVersion, " arch: ", osArch, " cores: ", cores};

        validateLogMessage(log, "BRK-1010", expected);
    }

    @Test
    public void testBrokerMemory()
    {
        final long oneGiga = 1024*1024*1024;
        final long twoGiga = oneGiga * 2;

        _logMessage = BrokerMessages.MAX_MEMORY(oneGiga, twoGiga);
        final List<Object> log = performLog();

        // Log messages always use US Locale format for numbers
        final NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

        final String[] expected = {"Maximum Memory :", "Heap", numberFormat.format(oneGiga), "bytes",
                "Direct", numberFormat.format(twoGiga), "bytes" };
        validateLogMessage(log, "BRK-1011", expected);
    }
}
