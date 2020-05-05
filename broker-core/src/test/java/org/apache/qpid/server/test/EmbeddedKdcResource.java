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
 */

package org.apache.qpid.server.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedKdcResource extends ExternalResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKdcResource.class);
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final boolean CLEAN_UP = Boolean.parseBoolean(System.getProperty("qpid.test.cleanUpKdcArtifacts", "true"));
    private final SimpleKdcServer _simpleKdcServer;
    private final String _realm;
    private final List<File> _createdFiles = new ArrayList<>();
    private volatile Path _kdcDirectory;

    public EmbeddedKdcResource(final String host, final int port, final String serviceName, final String realm)
    {
        _realm = realm;
        try
        {
            _simpleKdcServer = new SimpleKdcServer();
            _simpleKdcServer.setKdcHost(host);
            if (port > 0)
            {
                _simpleKdcServer.setKdcTcpPort(port);
            }
            _simpleKdcServer.setAllowUdp(false);
            _simpleKdcServer.setKdcRealm(realm);
            _simpleKdcServer.getKdcConfig().setString(KdcConfigKey.KDC_SERVICE_NAME, serviceName);
        }
        catch (KrbException e)
        {
            throw new AssertionError(String.format("Unable to create SimpleKdcServer': %s", e.getMessage()), e);
        }
    }

    @Override
    public void before() throws Exception
    {
        final Path kdcDir = Paths.get("target", "simple-kdc-" + COUNTER.incrementAndGet());
        createWorkDirectory(kdcDir);
        _kdcDirectory = kdcDir;
        _simpleKdcServer.setWorkDir(_kdcDirectory.toFile());
        _simpleKdcServer.init();
        _simpleKdcServer.start();
        LOGGER.info("SimpleKdcServer started on port {}, realm '{}' with work dir '{}'", getPort(), getRealm(), _kdcDirectory);
        if (!CLEAN_UP)
        {
            Files.copy(Paths.get(kdcDir.toString(), "krb5.conf"), Paths.get(kdcDir.toString(), "krb5.conf.copy"));
        }
    }

    @Override
    public void after()
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

    public String getRealm()
    {
        return _realm;
    }

    private void delete(Path f) throws IOException
    {
        Files.walkFileTree(f,
                           new SimpleFileVisitor<Path>()
                           {
                               @Override
                               public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                                       throws IOException
                               {
                                   Files.delete(file);
                                   return FileVisitResult.CONTINUE;
                               }

                               @Override
                               public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
                                       throws IOException
                               {
                                   Files.delete(dir);
                                   return FileVisitResult.CONTINUE;
                               }
                           });
    }

    public int getPort()
    {
        return _simpleKdcServer.getKdcTcpPort();
    }

    public File createPrincipal(String keyTabFileName, String... principals)
            throws Exception
    {
        final Path ketTabPath = Paths.get("target", keyTabFileName);
        final File ketTabFile = ketTabPath.toFile();
        _createdFiles.add(ketTabFile);
        createPrincipal(ketTabFile, principals);
        return ketTabFile;
    }

    public void createPasswordPrincipal(String name, String password)
            throws Exception
    {
        _simpleKdcServer.createPrincipal(name, password);
    }

    private void createPrincipal(File keyTabFile, String... principals)
            throws Exception
    {
        _simpleKdcServer.createPrincipals(principals);
        if (keyTabFile.exists() && !keyTabFile.delete())
        {
            LOGGER.error("Failed to delete keytab file: " + keyTabFile);
        }
        for (String principal : principals)
        {
            _simpleKdcServer.getKadmin().exportKeytab(keyTabFile, principal);
        }
    }

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
        for (File f: _createdFiles)
        {
            if (!f.delete())
            {
                LOGGER.warn("Failure to delete file {}", f.getAbsolutePath());
            }
        }
    }
}
