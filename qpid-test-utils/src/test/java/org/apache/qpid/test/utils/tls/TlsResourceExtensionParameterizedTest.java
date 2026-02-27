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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith({ TlsResourceExtensionParameterizedTest.InvocationCleanupVerifier.class, TlsResourceExtension.class })
class TlsResourceExtensionParameterizedTest
{
    private static final List<Integer> PARAMETERIZED_IDS = new ArrayList<>();
    private static final List<Integer> REPEATED_IDS = new ArrayList<>();

    private TlsResource _beforeEachResource;
    private static final ThreadLocal<TlsResource> CURRENT_RESOURCE = new ThreadLocal<>();

    @BeforeEach
    void setUp(final TlsResource tls)
    {
        _beforeEachResource = tls;
        CURRENT_RESOURCE.set(tls);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3 })
    void parameterizedTestUsesSameInstanceWithinInvocation(final int value, final TlsResource tls)
    {
        assertNotNull(value);
        assertSame(_beforeEachResource, tls);
        PARAMETERIZED_IDS.add(System.identityHashCode(tls));
    }

    @RepeatedTest(3)
    void repeatedTestUsesSameInstanceWithinInvocation(final TlsResource tls)
    {
        assertSame(_beforeEachResource, tls);
        REPEATED_IDS.add(System.identityHashCode(tls));
    }

    @AfterAll
    static void verifyInstancesAcrossInvocations()
    {
        assertEquals(3, PARAMETERIZED_IDS.size());
        assertEquals(3, REPEATED_IDS.size());
        assertEquals(3, PARAMETERIZED_IDS.stream().distinct().count());
        assertEquals(3, REPEATED_IDS.stream().distinct().count());
    }

    static class InvocationCleanupVerifier implements AfterEachCallback
    {
        @Override
        public void afterEach(final ExtensionContext context)
        {
            final TlsResource tls = CURRENT_RESOURCE.get();
            assertNotNull(tls, "Expected TlsResource instance to be created during the test");
            assertThrows(IllegalStateException.class, tls::getSecretAsCharacters);
            CURRENT_RESOURCE.remove();
        }
    }
}
