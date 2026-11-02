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

package org.apache.qpid.server.security.certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ServiceConfigurationError;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

class CertificateGeneratorsTest extends UnitTestBase
{
    private static final CertificateGenerator GENERATOR = request -> null;

    @BeforeEach
    void setUp()
    {
        CertificateGeneratorsTestHelper.reset();
    }

    @AfterEach
    void tearDown()
    {
        CertificateGeneratorsTestHelper.reset();
    }

    @Test
    void noFactories()
    {
        final CertificateGenerators.Selection selection = CertificateGenerators.select(List.of(), null);

        assertNull(selection.generator());
        assertEquals(CertificateGenerators.NOT_INSTALLED, selection.unavailableReason());
    }

    @Test
    void singleFactorySelectedWithoutProperty()
    {
        final CertificateGenerators.Selection selection =
                CertificateGenerators.select(List.of(new StubFactory("A", GENERATOR)), null);

        assertEquals("A", selection.type());
        assertSame(GENERATOR, selection.generator());
    }

    @Test
    void blankPropertyTreatedAsUnset()
    {
        final CertificateGenerators.Selection selection =
                CertificateGenerators.select(List.of(new StubFactory("A", GENERATOR)), " ");

        assertSame(GENERATOR, selection.generator());
    }

    @Test
    void severalFactoriesWithoutProperty()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", GENERATOR), new StubFactory("B", GENERATOR));

        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> CertificateGenerators.select(factories, null));

        assertTrue(thrown.getMessage().contains("[A, B]"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains(CertificateGenerators.PROPERTY), thrown.getMessage());
    }

    @Test
    void severalFactoriesWithProperty()
    {
        final CertificateGenerator other = request -> null;
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", GENERATOR), new StubFactory("B", other));

        final CertificateGenerators.Selection selection = CertificateGenerators.select(factories, "B");

        assertEquals("B", selection.type());
        assertSame(other, selection.generator());
    }

    @Test
    void onlyRequestedFactoryCreatesGenerator()
    {
        final StubFactory a = new StubFactory("A", GENERATOR);
        final StubFactory b = new StubFactory("B", GENERATOR);

        CertificateGenerators.select(List.of(a, b), "B");

        assertEquals(0, a._createCount);
        assertEquals(1, b._createCount);
    }

    @Test
    void unknownRequestedType()
    {
        final List<CertificateGeneratorFactory> factories = List.of(new StubFactory("A", GENERATOR));

        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> CertificateGenerators.select(factories, "NOPE"));

        assertTrue(thrown.getMessage().contains("'NOPE'"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[A]"), thrown.getMessage());
    }

    @Test
    void unknownRequestedTypeWithoutFactories()
    {
        assertThrows(IllegalArgumentException.class, () -> CertificateGenerators.select(List.of(), "NOPE"));
    }

    @Test
    void duplicateType()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", GENERATOR), new OtherStubFactory("A"));

        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> CertificateGenerators.select(factories, null));

        assertTrue(thrown.getMessage().contains(StubFactory.class.getName()), thrown.getMessage());
        assertTrue(thrown.getMessage().contains(OtherStubFactory.class.getName()), thrown.getMessage());
    }

    @Test
    void requestedFactoryFailsToCreate()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new GeneralSecurityException("broken")));

        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, "A"));

        assertInstanceOf(GeneralSecurityException.class, thrown.getCause());
    }

    @Test
    void singleFactoryFailsToCreate()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new GeneralSecurityException("broken")));

        assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));
    }

    @Test
    void factoryReturnsNoGenerator()
    {
        final List<CertificateGeneratorFactory> factories = List.of(new StubFactory("A", (CertificateGenerator) null));

        assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));
    }

    @Test
    void singleFactoryMissingLibrary()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new NoClassDefFoundError("org/example/Missing")));

        final CertificateGenerators.Selection selection = CertificateGenerators.select(factories, null);

        assertNull(selection.generator());
        assertTrue(selection.unavailableReason().contains("'A'"), selection.unavailableReason());
        assertTrue(selection.unavailableReason().contains("org/example/Missing"), selection.unavailableReason());
    }

    @Test
    void singleFactoryIncompatibleLibrary()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new NoSuchMethodError("org.example.Library.method()")));

        final CertificateGenerators.Selection selection = CertificateGenerators.select(factories, null);

        assertNull(selection.generator());
        assertTrue(selection.unavailableReason().contains("NoSuchMethodError"), selection.unavailableReason());
    }

    @Test
    void singleFactoryFailedInitialisation()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new ExceptionInInitializerError("broken")));

        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));

        assertTrue(thrown.getMessage().contains("could not be initialised"), thrown.getMessage());
        assertInstanceOf(ExceptionInInitializerError.class, thrown.getCause());
    }

    @Test
    void singleFactoryFailedInitialisationEarlier()
    {
        final List<CertificateGeneratorFactory> factories = List.of(new FailingInitialisationFactory());

        assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));

        // the JVM reports a later use of the class as NoClassDefFoundError
        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));
        assertTrue(thrown.getMessage().contains("could not be initialised"), thrown.getMessage());
        assertInstanceOf(NoClassDefFoundError.class, thrown.getCause());
    }

    @Test
    void singleFactoryFailedInitialisationEarlierWithoutCause()
    {
        // as reported by Java 17
        final List<CertificateGeneratorFactory> factories = List.of(
                new StubFactory("A", new NoClassDefFoundError("Could not initialize class org.example.Generator")));

        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, null));

        assertTrue(thrown.getMessage().contains("could not be initialised"), thrown.getMessage());
    }

    @Test
    void requestedFactoryIncompatibleLibrary()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new ExceptionInInitializerError("broken")));

        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, "A"));

        assertInstanceOf(ExceptionInInitializerError.class, thrown.getCause());
    }

    @Test
    void requestedFactoryMissingLibrary()
    {
        final List<CertificateGeneratorFactory> factories =
                List.of(new StubFactory("A", new NoClassDefFoundError("org/example/Missing")));

        final ServerScopedRuntimeException thrown =
                assertThrows(ServerScopedRuntimeException.class, () -> CertificateGenerators.select(factories, "A"));

        assertInstanceOf(NoClassDefFoundError.class, thrown.getCause());
    }

    @Test
    void noGeneratorInstalledOnCoreClasspath()
    {
        CertificateGenerators.initialise();

        assertFalse(CertificateGenerators.isAvailable());
        assertEquals(CertificateGenerators.NOT_INSTALLED, CertificateGenerators.getUnavailableReason());
        final NoSuchAlgorithmException thrown =
                assertThrows(NoSuchAlgorithmException.class, CertificateGenerators::get);
        assertTrue(thrown.getMessage().contains(CertificateGenerators.NOT_INSTALLED), thrown.getMessage());
    }

    @Test
    void initialiseReadsPropertyAndDoesNotKeepFailedSelection()
    {
        setTestSystemProperty(CertificateGenerators.PROPERTY, "NOPE");

        final IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, CertificateGenerators::initialise);
        assertTrue(thrown.getMessage().contains("'NOPE'"), thrown.getMessage());
        assertThrows(IllegalArgumentException.class, CertificateGenerators::isAvailable);

        // not setTestSystemProperty(..., null): that would record "NOPE" as the value to restore after the test
        System.clearProperty(CertificateGenerators.PROPERTY);

        CertificateGenerators.initialise();
        assertFalse(CertificateGenerators.isAvailable());
    }

    @Test
    void initialiseReplacesEarlierSelection()
    {
        CertificateGeneratorsTestHelper.useGenerator("A", GENERATOR);
        assertTrue(CertificateGenerators.isAvailable());

        CertificateGenerators.initialise();

        // no generator is installed on the broker-core classpath
        assertFalse(CertificateGenerators.isAvailable());
    }

    @Test
    void factoriesCannotBeLoaded()
    {
        final ServiceConfigurationError failure = new ServiceConfigurationError("broken provider configuration");
        try (final MockedConstruction<QpidServiceLoader> ignored = mockConstruction(QpidServiceLoader.class,
                (loader, context) -> when(loader.instancesOf(CertificateGeneratorFactory.class)).thenThrow(failure)))
        {
            final ServerScopedRuntimeException thrown =
                    assertThrows(ServerScopedRuntimeException.class, CertificateGenerators::initialise);

            assertSame(failure, thrown.getCause());
        }
    }

    @Test
    void suppliedGeneratorIsUsed() throws Exception
    {
        try (final CertificateGeneratorsTestHelper.Reset ignored =
                     CertificateGeneratorsTestHelper.useGenerator("A", GENERATOR))
        {
            assertTrue(CertificateGenerators.isAvailable());
            assertNull(CertificateGenerators.getUnavailableReason());
            assertSame(GENERATOR, CertificateGenerators.get());
        }

        assertFalse(CertificateGenerators.isAvailable());
    }

    @Test
    void nullSuppliedGeneratorRejected()
    {
        assertThrows(NullPointerException.class, () -> CertificateGeneratorsTestHelper.useGenerator("A", null));
    }

    private static final class OtherStubFactory implements CertificateGeneratorFactory
    {
        private final String _type;

        OtherStubFactory(final String type)
        {
            _type = type;
        }

        @Override
        public String getType()
        {
            return _type;
        }

        @Override
        public CertificateGenerator create()
        {
            return GENERATOR;
        }
    }

    private static final class FailingInitialisationFactory implements CertificateGeneratorFactory
    {
        @Override
        public String getType()
        {
            return "A";
        }

        @Override
        public CertificateGenerator create()
        {
            return FailingInitialisation.GENERATOR;
        }
    }

    private static final class FailingInitialisation
    {
        static final CertificateGenerator GENERATOR = fail();

        private static CertificateGenerator fail()
        {
            throw new IllegalStateException("broken");
        }
    }

    private static final class StubFactory implements CertificateGeneratorFactory
    {
        private final String _type;
        private final CertificateGenerator _generator;
        private final Throwable _failure;
        private int _createCount;

        StubFactory(final String type, final CertificateGenerator generator)
        {
            _type = type;
            _generator = generator;
            _failure = null;
        }

        StubFactory(final String type, final Throwable failure)
        {
            _type = type;
            _generator = null;
            _failure = failure;
        }

        @Override
        public String getType()
        {
            return _type;
        }

        @Override
        public CertificateGenerator create() throws GeneralSecurityException
        {
            _createCount++;
            if (_failure instanceof GeneralSecurityException)
            {
                throw (GeneralSecurityException) _failure;
            }
            if (_failure instanceof Error)
            {
                throw (Error) _failure;
            }
            return _generator;
        }
    }
}
