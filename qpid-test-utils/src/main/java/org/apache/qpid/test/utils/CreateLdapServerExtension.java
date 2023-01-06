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

import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.ldap.LdapServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit's extension for managing ApacheDS test framework annotations.
 *
 * Creates and starts embedded LDAP server and directory service before executing tests and shutdowns them
 * after executing all tests.
 *
 * Currently, Apache DS does not provide out of box extension for JUnit 5 for handling embedded LDAP server lifecycle.
 * After it will fully migrate to JUnit 5 and expose functionality needed this class can be replaced with the
 * Apache DS one.
 */
public class CreateLdapServerExtension implements BeforeAllCallback, AfterAllCallback
{
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateLdapServerExtension.class);

    /**
     * LDAP server
     */
    private LdapServer _ldapServer;

    /**
     * Directory service
     */
    private DirectoryService _directoryService;

    /**
     * Creates embedded directory service and LDAP server based on test class annotations, applies ldif files and starts
     * directory service and LDAP server.
     *
     * @param ctx ExtensionContext
     */
    @Override
    public void beforeAll(ExtensionContext ctx)
    {
        if (_ldapServer != null)
        {
            return;
        }
        final Class<?> testClass = TestUtils.getTestClass(ctx);
        final CreateLdapServer createLdapServer = testClass.getAnnotation(CreateLdapServer.class);
        final CreateDS createDS = testClass.getAnnotation(CreateDS.class);

        LOGGER.trace("Creating directory service");
        _directoryService = createDirectoryService(createDS);

        final ApplyLdifFiles applyLdifFiles = testClass.getAnnotation(ApplyLdifFiles.class);

        if (applyLdifFiles != null)
        {
            try
            {
                DSAnnotationProcessor.injectLdifFiles(applyLdifFiles.clazz(), _directoryService,
                        applyLdifFiles.value());
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to inject LDIF files", e);
            }
        }

        LOGGER.trace("Creating ldap server");
        _ldapServer = createLdapServer(createLdapServer, _directoryService);

        try
        {
            _directoryService.startup();
        }
        catch (LdapException e)
        {
            throw new RuntimeException("Failed to start directory service", e);
        }

        try
        {
            _ldapServer.start();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to start LDAP server", e);
        }
    }

    /**
     * Shutdowns directory service and LDAP server
     *
     * @param ctx ExtensionContext
     */
    public void afterAll(ExtensionContext ctx)
    {
        if (_ldapServer != null)
        {
            LOGGER.trace("Stopping ldap server");
            _ldapServer.stop();
        }

        if (_directoryService != null)
        {
            try
            {
                LOGGER.trace("Stopping directory service");
                _directoryService.shutdown();
            }
            catch (LdapException e)
            {
                throw new RuntimeException("Failed to stop directory service", e);
            }
        }
    }

    /**
     * Creates LDAP server
     *
     * @param createLdapServer CreateLdapServer annotation
     * @param directoryService DirectoryService instance
     *
     * @return Initialized LdapServer
     */
    public LdapServer createLdapServer(final CreateLdapServer createLdapServer, final DirectoryService directoryService)
    {
        return ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
    }

    /**
     * Creates DirectoryService
     *
     * @param createDS CreateDS annotation
     *
     * @return Initialized DirectoryService
     */
    public DirectoryService createDirectoryService(final CreateDS createDS)
    {
        try
        {
            return DSAnnotationProcessor.createDS(createDS);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create embedded Directory Service", e);
        }
    }

    public LdapServer getLdapServer()
    {
        return _ldapServer;
    }

    public DirectoryService getDirectoryService()
    {
        return _directoryService;
    }
}
