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
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedKdcResource extends ExternalResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKdcResource.class);
    private final SimpleKdcServer _simpleKdcServer;
    private final String _realm;
    private final List<File> _createdFiles = new ArrayList<>();
    private volatile File _kdcDirectory;

    public EmbeddedKdcResource(final String realm)
    {
        this(InetAddress.getLoopbackAddress().getCanonicalHostName(), 0, "QpidTestKerberosServer", realm);
    }

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
        final Path targetDir = FileSystems.getDefault().getPath("target");
        _kdcDirectory = Files.createTempDirectory(targetDir, "simple-kdc-").toFile();
        _simpleKdcServer.setWorkDir(_kdcDirectory);
        _simpleKdcServer.init();
        _simpleKdcServer.start();
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

    public String getRealm()
    {
        return _realm;
    }

    private void delete(File f) throws IOException
    {
        Files.walkFileTree(f.toPath(),
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
        final File ketTabFile = createFile(keyTabFileName);
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

    private static File createFile(final String keyTabFile) throws IOException
    {
        final File target = FileSystems.getDefault().getPath("target").toFile();
        final File file = new File(target, keyTabFile);
        if (file.exists())
        {
            if (!file.delete())
            {
                throw new IOException(String.format("Cannot delete existing file '%s'", keyTabFile));
            }
        }
        if (!file.createNewFile())
        {
            throw new IOException(String.format("Cannot create file '%s'", keyTabFile));
        }
        return file;
    }

}
