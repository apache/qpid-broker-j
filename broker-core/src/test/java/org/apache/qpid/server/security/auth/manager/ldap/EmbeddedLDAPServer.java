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
package org.apache.qpid.server.security.auth.manager.ldap;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.model.schema.registries.SchemaLoader;
import org.apache.directory.api.ldap.schema.extractor.SchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.api.util.exception.Exceptions;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.DnFactory;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.xdbm.Index;

public class EmbeddedLDAPServer
{
    /** The directory service */
    private DirectoryService _directoryService;

    /** The LDAP server */
    private LdapServer _ldapServer;

    private final String _keyStore;
    private final String _keyStorePass;
    private final int _port;

    /**
     * Creates a new instance of EmbeddedADS. It initializes the directory service.
     *
     * @throws Exception If something went wrong
     */
    public EmbeddedLDAPServer(final File workDir,
                              final String keyStore,
                              final String keyStorePass,
                              final int port) throws Exception
    {
        initDirectoryService(workDir);
        _keyStore = keyStore;
        _keyStorePass = keyStorePass;
        _port = port;
    }

    /**
     * Add a new partition to the server
     *
     * @param dnFactory the DN factory
     * @return The newly added partition
     * @throws Exception If the partition can't be added
     */
    private Partition addPartition(final DnFactory dnFactory) throws Exception
    {
        final JdbmPartition partition = new JdbmPartition(_directoryService.getSchemaManager(), dnFactory);
        partition.setId("qpid");
        partition.setPartitionPath(
                new File(_directoryService.getInstanceLayout().getPartitionsDirectory(), "qpid").toURI()
                                  );
        partition.setSuffixDn(new Dn("dc=qpid,dc=org"));
        _directoryService.addPartition(partition);
        return partition;
    }

    /**
     * Add a new set of index on the given attributes
     *
     * @param partition The partition on which we want to add index
     * @param attrs The list of attributes to index
     */
    private void addIndex(Partition partition, String... attrs)
    {
        final Set<Index<?, String>> indexedAttributes = new HashSet<>();
        for (String attribute : attrs)
        {
            indexedAttributes.add(new JdbmIndex<>(attribute, false));
        }
        ((JdbmPartition) partition).setIndexedAttributes(indexedAttributes);
    }

    /**
     * Initialize the schema manager and add the schema partition to directory service
     *
     * @throws Exception if the schema LDIF files are not found on the classpath
     */
    private void initSchemaPartition() throws Exception
    {
        final InstanceLayout instanceLayout = _directoryService.getInstanceLayout();
        final File schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory(), "schema");
        if (schemaPartitionDirectory.exists())
        {
            System.out.println("schema partition already exists, skipping schema extraction");
        }
        else
        {
            final SchemaLdifExtractor extractor =
                    new DefaultSchemaLdifExtractor(instanceLayout.getPartitionsDirectory());
            extractor.extractOrCopy();
        }

        final SchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
        final SchemaManager schemaManager = new DefaultSchemaManager(loader);
        schemaManager.loadAllEnabled();

        final List<Throwable> errors = schemaManager.getErrors();

        if (errors.size() != 0)
        {
            throw new Exception(Exceptions.printErrors(errors));
        }

        _directoryService.setSchemaManager(schemaManager);

        // Init the LdifPartition with schema
        final LdifPartition schemaLdifPartition = new LdifPartition(schemaManager, _directoryService.getDnFactory());
        schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());

        // The schema partition
        final SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(schemaLdifPartition);
        _directoryService.setSchemaPartition(schemaPartition);
    }

    /**
     * Initialize the server. It creates the partition, adds the index, and
     * injects the context entries for the created partitions.
     *
     * @param workDir the directory to be used for storing the data
     * @throws Exception if there were some problems while initializing the system
     */
    private void initDirectoryService(File workDir) throws Exception
    {
        _directoryService = new DefaultDirectoryService();
        _directoryService.setInstanceLayout(new InstanceLayout(workDir));
        _directoryService.setAllowAnonymousAccess(true);

        initSchemaPartition();

        final JdbmPartition systemPartition =
                new JdbmPartition(_directoryService.getSchemaManager(), _directoryService.getDnFactory());
        systemPartition.setId("system");
        systemPartition.setPartitionPath(new File(_directoryService.getInstanceLayout().getPartitionsDirectory(),
                                                  systemPartition.getId()).toURI());
        systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
        systemPartition.setSchemaManager(_directoryService.getSchemaManager());

        _directoryService.setSystemPartition(systemPartition);

        _directoryService.getChangeLog().setEnabled(false);
        _directoryService.setDenormalizeOpAttrsEnabled(true);

        final Partition qpidPartition = addPartition(_directoryService.getDnFactory());

        addIndex(qpidPartition, "objectClass", "ou", "uid");

        _directoryService.startup();

        final Dn domain = new Dn("dc=qpid,dc=org");
        final Entry entryDomain = _directoryService.newEntry(domain);
        entryDomain.add("objectClass", "domain");
        entryDomain.add("objectClass", "top");
        entryDomain.add("dc", "tests");
        _directoryService.getAdminSession().add(entryDomain);

        final Dn users = new Dn("ou=users,dc=qpid,dc=org");
        final Entry entryUsers = _directoryService.newEntry(users);
        entryUsers.add("objectClass", "organizationalUnit");
        entryUsers.add("objectClass", "top");
        entryUsers.add("ou", "Users");
        _directoryService.getAdminSession().add(entryUsers);

        final Dn groups = new Dn("ou=groups,dc=qpid,dc=org");
        final Entry entryGroups = _directoryService.newEntry(groups);
        entryGroups.add("objectClass", "organizationalUnit");
        entryGroups.add("objectClass", "top");
        entryGroups.add("ou", "Groups");
        _directoryService.getAdminSession().add(entryGroups);

        final Dn testDN = new Dn("cn=integration-test1,ou=users,dc=qpid,dc=org");
        final Entry entryTest = _directoryService.newEntry(testDN);
        entryTest.add("objectClass", "inetOrgPerson");
        entryTest.add("objectClass", "organizationalPerson");
        entryTest.add("objectClass", "person");
        entryTest.add("objectClass", "top");
        entryTest.add("cn", "integration-test1");
        entryTest.add("sn", "ldap-integration-test1");
        entryTest.add("uid", "test1");
        entryTest.add("userPassword", "password1");
        _directoryService.getAdminSession().add(entryTest);
    }

    /**
     * Starts the LdapServer
     *
     * @throws Exception error thrown
     */
    public void startServer() throws Exception
    {
        final TcpTransport tcp = new TcpTransport(_port);
        tcp.setEnableSSL(true);
        tcp.setWantClientAuth(true);

        _ldapServer = new LdapServer();
        _ldapServer.setTransports(tcp);
        _ldapServer.setKeystoreFile(_keyStore);
        _ldapServer.setCertificatePassword(_keyStorePass);
        _ldapServer.addSaslMechanismHandler(SupportedSaslMechanisms.PLAIN, new PlainMechanismHandler());
        _ldapServer.addSaslMechanismHandler(SupportedSaslMechanisms.GSSAPI, new GssapiMechanismHandler());
        _ldapServer.setSaslHost("localhost");
        _ldapServer.setSaslPrincipal("ldap/localhost@QPID.ORG");
        _ldapServer.setDirectoryService(_directoryService);
        _ldapServer.start();
    }

    public void stopServer() throws LdapException
    {
        _ldapServer.stop();
        _directoryService.shutdown();
    }
}
