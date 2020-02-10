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

import org.junit.Test;

/**
 * Test BRK log Messages
 */
public class BrokerMessagesTest extends AbstractTestMessages
{
    @Test
    public void testBrokerStartup()
    {
        String version = "Qpid 0.6";
        String build = "796936M";

        _logMessage = BrokerMessages.STARTUP(version, build);
        List<Object> log = performLog();

        String[] expected = {"Startup :", "Version:", version, "Build:", build};

        validateLogMessage(log, "BRK-1001", expected);
    }

    @Test
    public void testBrokerListening()
    {
        String transport = "TCP";
        Integer port = 2765;

        _logMessage = BrokerMessages.LISTENING(transport, port);

        List<Object> log = performLog();

        String[] expected = {"Starting", "Listening on ",
                             transport, "port ", String.valueOf(port)};

        validateLogMessage(log, "BRK-1002", expected);
    }

    @Test
    public void testBrokerShuttingDown()
    {
        String transport = "TCP";
        Integer port = 2765;

        _logMessage = BrokerMessages.SHUTTING_DOWN(transport, port);

        List<Object> log = performLog();

        String[] expected = {"Shutting down", transport, "port ", String.valueOf(port)};

        validateLogMessage(log, "BRK-1003", expected);
    }

    @Test
    public void testBrokerReady()
    {
        _logMessage = BrokerMessages.READY();
        List<Object> log = performLog();

        String[] expected = {"Ready"};

        validateLogMessage(log, "BRK-1004", expected);
    }

    @Test
    public void testBrokerStopped()
    {
        _logMessage = BrokerMessages.STOPPED();
        List<Object> log = performLog();

        String[] expected = {"Stopped"};

        validateLogMessage(log, "BRK-1005", expected);
    }

    @Test
    public void testBrokerConfig()
    {
        String path = "/file/path/to/configuration.xml";

        _logMessage = BrokerMessages.CONFIG(path);
        List<Object> log = performLog();

        String[] expected = {"Using configuration :", path};

        validateLogMessage(log, "BRK-1006", expected);
    }

    @Test
    public void testBrokerPlatform()
    {
        String javaVendor = "jvendor";
        String javaVersion = "j1.0";

        String osName = "os";
        String osVersion = "o1.0";
        String osArch = "oarch";

        String cores = "2";

        _logMessage = BrokerMessages.PLATFORM(javaVendor, javaVersion, osName, osVersion, osArch, cores);
        List<Object> log = performLog();

        String[] expected = {"Platform :", "JVM :", javaVendor, " version: ", " OS : ", osName, " version: ", osVersion, " arch: ", osArch, " cores: ", cores};

        validateLogMessage(log, "BRK-1010", expected);
    }

    @Test
    public void testBrokerMemory()
    {
        long oneGiga = 1024*1024*1024;
        long twoGiga = oneGiga * 2;

        _logMessage = BrokerMessages.MAX_MEMORY(oneGiga, twoGiga);
        List<Object> log = performLog();

        // Log messages always use US Locale format for numbers
        NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

        String[] expected = {"Maximum Memory :",
                             "Heap", numberFormat.format(oneGiga), "bytes",
                             "Direct", numberFormat.format(twoGiga), "bytes"
        };

        validateLogMessage(log, "BRK-1011", expected);
    }

}
