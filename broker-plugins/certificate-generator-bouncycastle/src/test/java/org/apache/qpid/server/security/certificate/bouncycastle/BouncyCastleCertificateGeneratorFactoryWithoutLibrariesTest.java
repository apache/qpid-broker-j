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
package org.apache.qpid.server.security.certificate.bouncycastle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.security.certificate.CertificateGeneratorFactory;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Loads the plugin classes with a class loader that hides all or part of Bouncy Castle, as when the plugin is deployed
 * without some of the Bouncy Castle libraries.
 */
class BouncyCastleCertificateGeneratorFactoryWithoutLibrariesTest extends UnitTestBase
{
    private static final String PLUGIN_PACKAGE = BouncyCastleCertificateGeneratorFactory.class.getPackageName() + ".";

    /**
     * @param hiddenPackage package prefix hidden from the plugin: all of Bouncy Castle, only classes of bcpkix
     *                      (certificate and operator API), or only classes of bcprov (ASN.1)
     */
    @ParameterizedTest
    @ValueSource(strings = { "org.bouncycastle.", "org.bouncycastle.cert.", "org.bouncycastle.operator.",
                             "org.bouncycastle.asn1." })
    void factoryLoadsAndCreateReportsMissingLibrary(final String hiddenPackage) throws Exception
    {
        final URL pluginClasses =
                BouncyCastleCertificateGeneratorFactory.class.getProtectionDomain().getCodeSource().getLocation();
        final ClassLoader parent = new HidingClassLoader(getClass().getClassLoader(), hiddenPackage);
        try (final URLClassLoader loader = new URLClassLoader(new URL[] { pluginClasses }, parent))
        {
            final Class<?> factoryClass = loader.loadClass(BouncyCastleCertificateGeneratorFactory.class.getName());
            assertNotSame(BouncyCastleCertificateGeneratorFactory.class, factoryClass);

            final CertificateGeneratorFactory factory =
                    (CertificateGeneratorFactory) factoryClass.getDeclaredConstructor().newInstance();
            assertEquals(BouncyCastleCertificateGeneratorFactory.TYPE, factory.getType());

            final NoClassDefFoundError thrown = assertThrows(NoClassDefFoundError.class, factory::create);
            assertTrue(thrown.getMessage().startsWith(hiddenPackage.replace('.', '/')), thrown.getMessage());
        }
    }

    /** Hides a package, and the plugin classes so that they are loaded by the child class loader. */
    private static final class HidingClassLoader extends ClassLoader
    {
        private final String _hiddenPackage;

        HidingClassLoader(final ClassLoader parent, final String hiddenPackage)
        {
            super(parent);
            _hiddenPackage = hiddenPackage;
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException
        {
            if (name.startsWith(_hiddenPackage) || name.startsWith(PLUGIN_PACKAGE))
            {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
