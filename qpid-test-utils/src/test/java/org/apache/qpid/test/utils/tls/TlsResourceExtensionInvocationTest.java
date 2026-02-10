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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith({ TlsResourceExtensionInvocationTest.InvocationCleanupVerifier.class, TlsResourceExtension.class })
class TlsResourceExtensionInvocationTest
{
    private static final List<Integer> IDS = new ArrayList<>();
    private static final ThreadLocal<Path> CURRENT_DIR = new ThreadLocal<>();
    private static final ThreadLocal<TlsResource> CURRENT_RESOURCE = new ThreadLocal<>();

    private TlsResource _beforeEachResource;

    @BeforeEach
    void setUp(final TlsResource tls) throws Exception
    {
        _beforeEachResource = tls;
        CURRENT_RESOURCE.set(tls);
        final Path file = tls.createFile(".tmp");
        CURRENT_DIR.set(file.getParent());
    }

    @Test
    void usesSameInstanceBetweenBeforeEachAndTest(final TlsResource tls)
    {
        assertSame(_beforeEachResource, tls);
        IDS.add(System.identityHashCode(tls));
    }

    @Test
    void createsNewInstancePerTest(final TlsResource tls)
    {
        IDS.add(System.identityHashCode(tls));
    }

    @AfterAll
    static void verifyDifferentInstances()
    {
        assertEquals(2, IDS.size());
        assertEquals(2, IDS.stream().distinct().count());
    }

    static class InvocationCleanupVerifier implements AfterEachCallback
    {
        @Override
        public void afterEach(final ExtensionContext context) throws Exception
        {
            final Path directory = CURRENT_DIR.get();
            assertNotNull(directory, "Expected CURRENT_DIR to be created during the test");
            assertFalse(Files.exists(directory), "Expected TLS resource directory to be deleted: " + directory);
            CURRENT_DIR.remove();

            final TlsResource tls = CURRENT_RESOURCE.get();
            assertNotNull(tls, "Expected TlsResource instance to be created during the test");
            assertThrows(IllegalStateException.class, tls::getSecretAsCharacters);
            CURRENT_RESOURCE.remove();
        }
    }
}
