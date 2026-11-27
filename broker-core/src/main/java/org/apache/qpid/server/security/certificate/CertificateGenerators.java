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

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * Selects the certificate generator used by the JVM.
 * <p>
 * The certificate generator is selected when the Broker starts, and otherwise on first use. The selection rules are:
 * <ul>
 *     <li>when {@value #PROPERTY} is set, the factory of that type is used; startup fails if it is not installed
 *     or cannot create its generator for any reason</li>
 *     <li>when the property is not set and several factories are installed, startup fails</li>
 *     <li>when the property is not set and no factory is installed, certificate generation is unavailable</li>
 *     <li>when the property is not set and exactly one factory is installed, it is used; if it cannot create its
 *     generator because a library it requires is missing or incompatible, a warning is logged and certificate
 *     generation is unavailable, while any other failure, including a failed static initialisation, makes startup
 *     fail</li>
 * </ul>
 * A selection that fails is not kept, so the next use selects again.
 */
public final class CertificateGenerators
{
    public static final String PROPERTY = "qpid.security.certificateGenerator";

    static final String NOT_INSTALLED = "no certificate generator is installed and enabled. Add the "
            + "qpid-broker-plugins-certificate-generator-bouncycastle plugin and the Bouncy Castle libraries "
            + "(bcprov, bcpkix and bcutil) to the classpath, or use a keystore with a supplied certificate";

    private static final Logger LOGGER = LoggerFactory.getLogger(CertificateGenerators.class);

    private static final Object LOCK = new Object();

    // guarded by LOCK
    private static Selection _selection;

    private CertificateGenerators()
    {
    }

    /**
     * Selects the certificate generator again, so that the current value of {@value #PROPERTY} is used even if a
     * generator was selected before, for example by an earlier Broker in the same JVM.
     *
     * @throws IllegalArgumentException if the configuration does not identify a single generator
     * @throws ServerScopedRuntimeException if the factories cannot be loaded or the generator cannot be created
     */
    public static void initialise()
    {
        synchronized (LOCK)
        {
            _selection = null;
            selection();
        }
    }

    /**
     * Selects the certificate generator, if not already selected.
     *
     * @return true if a certificate generator is available
     * @throws IllegalArgumentException if the configuration does not identify a single generator
     * @throws ServerScopedRuntimeException if the factories cannot be loaded or the generator cannot be created
     */
    public static boolean isAvailable()
    {
        return selection().generator() != null;
    }

    /**
     * Selects the certificate generator, if not already selected. Use the result both to check availability and
     * to report the reason, as the selection may change between two calls.
     *
     * @return why certificate generation is unavailable, or null if it is available
     * @throws IllegalArgumentException if the configuration does not identify a single generator
     * @throws ServerScopedRuntimeException if the factories cannot be loaded or the generator cannot be created
     */
    public static String getUnavailableReason()
    {
        return selection().unavailableReason();
    }

    /**
     * @return the selected certificate generator
     * @throws NoSuchAlgorithmException if no certificate generator is available
     */
    public static CertificateGenerator get() throws NoSuchAlgorithmException
    {
        final Selection selection = selection();
        if (selection.generator() == null)
        {
            throw new NoSuchAlgorithmException("Certificate generation is unavailable: "
                                               + selection.unavailableReason());
        }
        return selection.generator();
    }

    private static Selection selection()
    {
        // not used on a hot path, so no lock-free fast path is needed
        synchronized (LOCK)
        {
            if (_selection == null)
            {
                _selection = select(discover(), System.getProperty(PROPERTY));
            }
            return _selection;
        }
    }

    private static Iterable<CertificateGeneratorFactory> discover()
    {
        try
        {
            return new QpidServiceLoader().instancesOf(CertificateGeneratorFactory.class);
        }
        catch (ServiceConfigurationError | LinkageError e)
        {
            throw new ServerScopedRuntimeException("Unable to load certificate generator factories", e);
        }
    }

    static Selection select(final Iterable<CertificateGeneratorFactory> discovered, final String requestedType)
    {
        final Map<String, CertificateGeneratorFactory> factories = new TreeMap<>();
        for (final CertificateGeneratorFactory factory : discovered)
        {
            final CertificateGeneratorFactory existing = factories.putIfAbsent(factory.getType(), factory);
            if (existing != null)
            {
                throw new IllegalArgumentException(String.format(
                        "Certificate generator type '%s' is provided by both %s and %s",
                        factory.getType(), existing.getClass().getName(), factory.getClass().getName()));
            }
        }

        if (requestedType != null && !requestedType.isBlank())
        {
            final CertificateGeneratorFactory factory = factories.get(requestedType);
            if (factory == null)
            {
                throw new IllegalArgumentException(String.format(
                        "Unknown certificate generator '%s' set in '%s', only the following types are supported: %s",
                        requestedType, PROPERTY, factories.keySet()));
            }
            try
            {
                return create(factory);
            }
            catch (GeneralSecurityException | LinkageError e)
            {
                throw new ServerScopedRuntimeException(String.format(
                        "Certificate generator '%s' set in '%s' could not be created", requestedType, PROPERTY), e);
            }
        }

        if (factories.size() > 1)
        {
            throw new IllegalArgumentException(String.format(
                    "Several certificate generators are installed %s, set '%s' to select one",
                    factories.keySet(), PROPERTY));
        }

        if (factories.isEmpty())
        {
            LOGGER.info("No certificate generator is installed and enabled, "
                        + "generation of self-signed certificates is unavailable");
            return Selection.unavailable(NOT_INSTALLED);
        }

        final CertificateGeneratorFactory factory = factories.values().iterator().next();
        try
        {
            return create(factory);
        }
        catch (ExceptionInInitializerError e)
        {
            // a class was found but its static initialisation failed, so the library is present but unusable
            throw initialisationFailed(factory, e);
        }
        catch (LinkageError e)
        {
            if (isEarlierInitialisationFailure(e))
            {
                throw initialisationFailed(factory, e);
            }
            final String reason = String.format(
                    "certificate generator '%s' is installed but a library it requires is missing or incompatible (%s)",
                    factory.getType(), e);
            LOGGER.warn("Generation of self-signed certificates is unavailable: {}", reason, e);
            return Selection.unavailable(reason);
        }
        catch (GeneralSecurityException e)
        {
            throw new ServerScopedRuntimeException(String.format(
                    "Certificate generator '%s' could not be created", factory.getType()), e);
        }
    }

    /**
     * The JVM reports a later use of a class whose static initialisation failed as NoClassDefFoundError. Since
     * Java 18 its cause is the original ExceptionInInitializerError; Java 17 only gives the message.
     */
    private static boolean isEarlierInitialisationFailure(final LinkageError e)
    {
        return e instanceof NoClassDefFoundError
               && (e.getCause() instanceof ExceptionInInitializerError
                   || String.valueOf(e.getMessage()).startsWith("Could not initialize class"));
    }

    private static ServerScopedRuntimeException initialisationFailed(final CertificateGeneratorFactory factory,
                                                                     final LinkageError e)
    {
        return new ServerScopedRuntimeException(String.format(
                "Certificate generator '%s' could not be initialised", factory.getType()), e);
    }

    private static Selection create(final CertificateGeneratorFactory factory) throws GeneralSecurityException
    {
        final CertificateGenerator generator = factory.create();
        if (generator == null)
        {
            throw new GeneralSecurityException(
                    String.format("Certificate generator factory '%s' returned no generator", factory.getType()));
        }
        LOGGER.info("Using certificate generator '{}'", factory.getType());
        return new Selection(factory.getType(), generator, null);
    }

    /** Replaces the selection, so that tests can supply a generator or clear the selection with null. */
    // visible for testing
    static void setSelection(final Selection selection)
    {
        synchronized (LOCK)
        {
            _selection = selection;
        }
    }

    record Selection(String type, CertificateGenerator generator, String unavailableReason)
    {
        static Selection unavailable(final String reason)
        {
            return new Selection(null, null, reason);
        }
    }
}
