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

package org.apache.qpid.server.logging.logback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class MdcLoggingEventTest extends UnitTestBase
{
    @Test
    @SuppressWarnings("deprecation")
    void mdcIsAnUnmodifiableSnapshot()
    {
        final Map<String, String> originalMdc = new HashMap<>(Map.of("trace", "request", "common", "original"));
        final LoggingEvent original = new LoggingEvent();
        original.setMDCPropertyMap(originalMdc);
        final Map<String, String> additions = new HashMap<>(Map.of("common", "socket", "broker", "node"));
        final MdcLoggingEvent event = new MdcLoggingEvent(original, additions);
        final Map<String, String> expected = Map.of("trace", "request", "common", "socket", "broker", "node");

        assertEquals(Map.of("trace", "request", "common", "original"), originalMdc);
        originalMdc.clear();
        additions.clear();

        assertEquals(expected, event.getMDCPropertyMap());
        assertSame(event.getMDCPropertyMap(), event.getMdc());
        assertThrows(UnsupportedOperationException.class, () -> event.getMDCPropertyMap().put("common", "changed"));
        assertThrows(UnsupportedOperationException.class, () ->
                event.getMDCPropertyMap().entrySet().iterator().next().setValue("changed"));
    }

    @Test
    void checkingForCallerDataDoesNotCaptureIt()
    {
        final LoggingEvent original = new LoggingEvent();
        original.setMDCPropertyMap(Map.of());
        final MdcLoggingEvent event = new MdcLoggingEvent(original, Map.of("broker", "node"));

        assertFalse(event.hasCallerData());
        assertFalse(original.hasCallerData());
    }
}
