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

package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.testkit.engine.EngineTestKit;

class TlsResourceExtensionFailureTest
{
    @Test
    void closesResourceWhenBeforeEachFails()
    {
        BeforeEachFailureCase.reset();

        EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(BeforeEachFailureCase.class))
                .execute();

        assertTrue(BeforeEachFailureCase.BEFORE_EACH_CALLED.get(), "Expected @BeforeEach to be called");
        assertTrue(BeforeEachFailureCase.CLOSED_IN_AFTER_EACH.get(),
                "Expected resource to be closed when @BeforeEach fails");
    }

    @Test
    void closesResourceWhenBeforeAllFails()
    {
        BeforeAllFailureCase.reset();

        EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(BeforeAllFailureCase.class))
                .execute();

        assertTrue(BeforeAllFailureCase.BEFORE_ALL_CALLED.get(), "Expected @BeforeAll to be called");
        assertTrue(BeforeAllFailureCase.CLOSED_IN_AFTER_ALL.get(),
                "Expected resource to be closed when @BeforeAll fails");
    }

    @ExtendWith({ BeforeEachFailureCase.CleanupVerifier.class, TlsResourceExtension.class })
    static class BeforeEachFailureCase
    {
        private static final AtomicReference<TlsResource> RESOURCE = new AtomicReference<>();
        private static final AtomicBoolean BEFORE_EACH_CALLED = new AtomicBoolean();
        private static final AtomicBoolean CLOSED_IN_AFTER_EACH = new AtomicBoolean();

        @BeforeEach
        void setUp(final TlsResource tls)
        {
            RESOURCE.set(tls);
            BEFORE_EACH_CALLED.set(true);
            throw new RuntimeException("boom");
        }

        @Test
        void testNeverRuns()
        {
        }

        static void reset()
        {
            RESOURCE.set(null);
            BEFORE_EACH_CALLED.set(false);
            CLOSED_IN_AFTER_EACH.set(false);
        }

        static class CleanupVerifier implements AfterEachCallback
        {
            @Override
            public void afterEach(final ExtensionContext context)
            {
                final TlsResource tls = RESOURCE.get();
                assertNotNull(tls, "Expected TlsResource instance to be created during the test");
                assertThrows(IllegalStateException.class, tls::getSecretAsCharacters);
                CLOSED_IN_AFTER_EACH.set(true);
            }
        }
    }

    @ExtendWith({ BeforeAllFailureCase.CleanupVerifier.class, TlsResourceExtension.class })
    static class BeforeAllFailureCase
    {
        private static final AtomicReference<TlsResource> RESOURCE = new AtomicReference<>();
        private static final AtomicBoolean BEFORE_ALL_CALLED = new AtomicBoolean();
        private static final AtomicBoolean CLOSED_IN_AFTER_ALL = new AtomicBoolean();

        @BeforeAll
        static void setUp(final TlsResource tls)
        {
            RESOURCE.set(tls);
            BEFORE_ALL_CALLED.set(true);
            throw new RuntimeException("boom");
        }

        @Test
        void testNeverRuns()
        {
        }

        static void reset()
        {
            RESOURCE.set(null);
            BEFORE_ALL_CALLED.set(false);
            CLOSED_IN_AFTER_ALL.set(false);
        }

        static class CleanupVerifier implements AfterAllCallback
        {
            @Override
            public void afterAll(final ExtensionContext context)
            {
                final TlsResource tls = RESOURCE.get();
                assertNotNull(tls, "Expected TlsResource instance to be created during the test");
                assertThrows(IllegalStateException.class, tls::getSecretAsCharacters);
                CLOSED_IN_AFTER_ALL.set(true);
            }
        }
    }
}
