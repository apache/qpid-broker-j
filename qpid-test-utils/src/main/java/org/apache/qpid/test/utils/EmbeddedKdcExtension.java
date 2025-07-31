/*
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
package org.apache.qpid.test.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit's extension for managing embedded kerberos server.
 *
 * Creates embedded kerberos server, starts it before executing all tests and stops it after executing all tests.
 *
 * Currently, Apache DS does not provide out of box extension for JUnit 5 for handling embedded kerberos server
 * lifecycle. After it will fully migrate to JUnit 5 and expose functionality needed this class can be replaced
 * with the Apache DS one.
 */
public class EmbeddedKdcExtension implements BeforeAllCallback, AutoCloseable
{
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKdcExtension.class);

    /**
     * Kerberos server counter
     */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /**
     * Kerberos server port counter
     */
    private static final AtomicInteger PORT = new AtomicInteger();

    /**
     * Clean up flag
     */
    private static final boolean CLEAN_UP = Boolean.parseBoolean(System.getProperty("qpid.test.cleanUpKdcArtifacts", "true"));

    /**
     * Kerberos server
     */
    private final SimpleKdcServer _simpleKdcServer;

    /**
     * Realm name
     */
    private final String _realm;

    /**
     * Files created
     */
    private final List<File> _createdFiles = new ArrayList<>();

    /**
     * Kerberos server directory
     */
    private final Path _kdcDirectory;

    /**
     * Kerberos server port
     */
    private final int _port;

    /**
     * Whether kerberos server is started or not
     */
    private boolean _started = false;

    /**
     * Creates kerberos server
     *
     * @param host Hostname
     * @param port Port
     * @param serviceName Service name
     * @param realm Realm
     */
    public EmbeddedKdcExtension(final String host, final int port, final String serviceName, final String realm)
    {
        _port = port;
        _realm = realm;
        _kdcDirectory = Paths.get("target", "simple-kdc-" + COUNTER.incrementAndGet());
        try
        {
            createWorkDirectory(_kdcDirectory);
            _simpleKdcServer = new SimpleKdcServer();
        }
        catch (KrbException | IOException e)
        {
            throw new AssertionError(String.format("Unable to create SimpleKdcServer': %s", e.getMessage()), e);
        }

        _simpleKdcServer.setKdcHost(host);

        // re-use port from previous start-up if any
        // IBM JDK caches port somehow causing test failures
        int p = port == 0 ? PORT.get() : port;
        if (p > 0)
        {
            _simpleKdcServer.setKdcTcpPort(p);
        }
        _simpleKdcServer.setAllowUdp(false);
        _simpleKdcServer.setKdcRealm(realm);
        _simpleKdcServer.getKdcConfig().setString(KdcConfigKey.KDC_SERVICE_NAME, serviceName);
        _simpleKdcServer.setWorkDir(_kdcDirectory.toFile());
    }

    /**
     * Starts embedded kerberos server
     *
     * @param ctx ExtensionContext
     *
     * @throws Exception Thrown exception
     */
    public void beforeAll(ExtensionContext ctx) throws Exception
    {
        if (_started)
        {
            return;
        }
        _started = true;
        _simpleKdcServer.init();
        if (_port == 0)
        {
            PORT.compareAndSet(0, _simpleKdcServer.getKdcSetting().checkGetKdcTcpPort());
        }
        _simpleKdcServer.start();
        LOGGER.debug("SimpleKdcServer started on port {}, realm '{}' with work dir '{}'", getPort(), getRealm(), _kdcDirectory);

        final String config =
                String.join("", Files.readAllLines(Paths.get(System.getProperty("java.security.krb5.conf"))));
        LOGGER.debug("java.security.krb5.conf='{}'", System.getProperty("java.security.krb5.conf"));
        final Path krb5Conf = Paths.get(_kdcDirectory.toString(), "krb5.conf");
        LOGGER.debug("JAAS config:" + config);
        if (!CLEAN_UP)
        {
            Files.copy(krb5Conf, Paths.get(_kdcDirectory.toString(), "krb5.conf.copy"), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Stops embedded kerberos server
     */
    public void close()
    {
        try
        {
            _simpleKdcServer.stop();
        }
        catch (KrbException e)
        {
            LOGGER.warn("Failure to stop KDC server", e);
        }
        finally
        {
            if (CLEAN_UP)
            {
                cleanUp();
            }
        }
    }

    /**
     * Deletes files in the directory
     *
     * @param path Directory path
     *
     * @throws IOException Thrown exception
     */
    private void delete(Path path) throws IOException
    {
        try (final Stream<Path> stream = Files.walk(path))
        {
            stream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(file ->
                    {
                        if (!file.delete())
                        {
                            LOGGER.warn("Could not delete file at {}", file.getAbsolutePath());
                        }
                    });
        }
    }

    /**
     * Creates principals
     *
     * @param keyTabFileName Keytab file name
     * @param principals Arrays of principals
     *
     * @return Keytab file created
     *
     * @throws Exception Thrown exception
     */
    public File createPrincipal(final String keyTabFileName, final String... principals) throws Exception
    {
        final Path keyTabPath = Paths.get("target", keyTabFileName).toAbsolutePath().normalize();
        final File keyTabFile = keyTabPath.toFile();
        _createdFiles.add(keyTabFile);
        createPrincipal(keyTabFile, principals);
        return keyTabFile;
    }

    /**
     * Creates principals
     *
     * @param keyTabFile Keytab file
     * @param principals Array of principals
     *
     * @throws Exception Thrown exception
     */
    private void createPrincipal(final File keyTabFile, final String... principals) throws Exception
    {
        _simpleKdcServer.createPrincipals(principals);
        if (keyTabFile.exists() && !keyTabFile.delete())
        {
            LOGGER.error("Failed to delete keytab file: " + keyTabFile);
        }
        for (final String principal : principals)
        {
            _simpleKdcServer.getKadmin().exportKeytab(keyTabFile, principal);
        }
    }

    /**
     * Creates directory for kerberos server
     *
     * @param kdcDir Directory path
     *
     * @throws IOException Thrown exception
     */
    private void createWorkDirectory(final Path kdcDir) throws IOException
    {
        try
        {
            Files.createDirectory(kdcDir);
        }
        catch (FileAlreadyExistsException e)
        {
            delete(kdcDir);
            Files.createDirectory(kdcDir);
        }
    }

    /**
     * Deletes files created for kerberos server
     */
    private void cleanUp()
    {
        try
        {
            delete(_kdcDirectory);
        }
        catch (IOException e)
        {
            LOGGER.warn("Failure to delete KDC directory", e);
        }
        for (final File file: _createdFiles)
        {
            if (!file.delete())
            {
                LOGGER.warn("Failure to delete file {}", file.getAbsolutePath());
            }
        }
    }

    public int getPort()
    {
        return _simpleKdcServer.getKdcSetting().getKdcTcpPort();
    }

    public String getRealm()
    {
        return _realm;
    }
}
