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
package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.server.security.encryption.AESGCMKeyFileEncrypterFactory;
import org.apache.qpid.server.security.encryption.ConfigurationSecretEncrypter;
import org.apache.qpid.test.utils.UnitTestBase;

public class BrokerRecovererTest extends UnitTestBase
{
    private final ConfiguredObjectRecord _brokerEntry = mock(ConfiguredObjectRecord.class);
    private final UUID _brokerId = randomUUID();
    private final UUID _authenticationProvider1Id = randomUUID();

    private SystemConfig<?> _systemConfig;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _systemConfig = new JsonSystemConfigImpl(_taskExecutor, mock(EventLogger.class),null, Map.of())
        {
            {
                updateModel(BrokerModel.getInstance());
            }
        };

        when(_brokerEntry.getId()).thenReturn(_brokerId);
        when(_brokerEntry.getType()).thenReturn(Broker.class.getSimpleName());

        final Map<String, Object> attributesMap = Map.of(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.NAME, getTestName());

        when(_brokerEntry.getAttributes()).thenReturn(attributesMap);
        when(_brokerEntry.getParents()).thenReturn(Map.of(SystemConfig.class.getSimpleName(), _systemConfig
                .getId()));

        // Add a base AuthenticationProvider for all tests
        final AuthenticationProvider<?> authenticationProvider1 = mock(AuthenticationProvider.class);
        when(authenticationProvider1.getName()).thenReturn("authenticationProvider1");
        when(authenticationProvider1.getId()).thenReturn(_authenticationProvider1Id);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stop();
        final Path path = Path.of(_systemConfig.getContextValue(String.class, SystemConfig.QPID_WORK_DIR));
        if (path.toFile().exists())
        {
            try (Stream<Path> stream = Files.walk(path))
            {
                stream.sorted(Comparator.reverseOrder()).map(Path::toFile).filter(File::exists).forEach(file ->
                {
                    makeFileDeletable(file);
                    file.delete();
                });
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerAttributes()
    {
        final Map<String, Object> attributes = Map.of(Broker.NAME, getTestName(),
                Broker.STATISTICS_REPORTING_PERIOD, 4000,
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        final Map<String, Object> entryAttributes = attributes.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> convertToString(entry.getValue())));

        when(_brokerEntry.getAttributes()).thenReturn(entryAttributes);

        resolveObjects(_brokerEntry);
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        assertNotNull(broker);

        broker.open();

        assertEquals(_brokerId, broker.getId());

        for (final Map.Entry<String, Object> attribute : attributes.entrySet())
        {
            final Object attributeValue = broker.getAttribute(attribute.getKey());
            assertEquals(attribute.getValue(), attributeValue, "Unexpected value of attribute '" +
                    attribute.getKey() + "'");
        }
    }

    public ConfiguredObjectRecord createAuthProviderRecord(final UUID id, final String name)
    {
        final Map<String, Object> authProviderAttrs = Map.of(AuthenticationProvider.NAME, name,
                AuthenticationProvider.TYPE, "Anonymous");
        return new ConfiguredObjectRecordImpl(id, AuthenticationProvider.class.getSimpleName(), authProviderAttrs, Map.of(Broker.class.getSimpleName(), _brokerEntry.getId()));
    }

    public ConfiguredObjectRecord createSimpleLDAPAuthProviderRecord(final UUID id, final String name, final String password)
    {
        final Map<String, Object> authProviderAttrs = Map.of(AuthenticationProvider.NAME, name,
                SimpleLDAPAuthenticationManager.TYPE, SimpleLDAPAuthenticationManager.PROVIDER_TYPE,
                SimpleLDAPAuthenticationManager.PROVIDER_URL, "ldap://localhost:%d",
                SimpleLDAPAuthenticationManager.SEARCH_CONTEXT, "ou=users,dc=qpid,dc=org",
                SimpleLDAPAuthenticationManager.SEARCH_FILTER, "(uid={0})",
                SimpleLDAPAuthenticationManager.SEARCH_PASSWORD, password);
        return new ConfiguredObjectRecordImpl(id, AuthenticationProvider.class.getSimpleName(), authProviderAttrs, Map.of(Broker.class.getSimpleName(), _brokerEntry.getId()));
    }

    public ConfiguredObjectRecord createGroupProviderRecord(final UUID id, final String name)
    {
        final Map<String, Object> groupProviderAttrs = Map.of(GroupProvider.NAME, name,
                GroupProvider.TYPE, "GroupFile",
                "path", "/no-such-path");
        return new ConfiguredObjectRecordImpl(id, GroupProvider.class.getSimpleName(), groupProviderAttrs, Map.of(Broker.class.getSimpleName(), _brokerEntry.getId()));
    }

    public ConfiguredObjectRecord createPortRecord(final UUID id, final int port, final Object authProviderRef)
    {
        final Map<String, Object> portAttrs = Map.of(Port.NAME, "port-" + port,
                Port.TYPE, "HTTP",
                Port.PORT, port,
                Port.AUTHENTICATION_PROVIDER, authProviderRef);
        return new ConfiguredObjectRecordImpl(id, Port.class.getSimpleName(), portAttrs, Map.of(Broker.class.getSimpleName(), _brokerEntry.getId()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerWithPorts()
    {
        final UUID authProviderId = randomUUID();
        final UUID portId = randomUUID();

        resolveObjects(_brokerEntry, createAuthProviderRecord(authProviderId, "authProvider"), createPortRecord(
                portId, 5672, "authProvider"));
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        assertNotNull(broker);
        broker.open();
        assertEquals(_brokerId, broker.getId());
        assertEquals(1, (long) broker.getPorts().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerWithOneAuthenticationProvider()
    {
        final UUID authProviderId = randomUUID();

        resolveObjects(_brokerEntry, createAuthProviderRecord(authProviderId, "authProvider"));
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        assertNotNull(broker);
        broker.open();
        assertEquals(_brokerId, broker.getId());
        assertEquals(1, (long) broker.getAuthenticationProviders().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerWithSimpleLDAPAuthenticationProvider() throws Exception
    {
        final UUID authProviderId = randomUUID();

        final String password = "password";
        final ConfigurationSecretEncrypter configurationSecretEncrypter =
                new AESGCMKeyFileEncrypterFactory().createEncrypter(_systemConfig);
        final String encryptedPassword = configurationSecretEncrypter.encrypt(password);

        final Method setter = _systemConfig.getClass().getSuperclass().getSuperclass().getSuperclass()
                .getDeclaredMethod("setEncrypter", ConfigurationSecretEncrypter.class);
        setter.setAccessible(true);
        setter.invoke(_systemConfig, configurationSecretEncrypter);

        // SimpleLDAPAuthenticationManager with the encrypted password is created
        resolveObjects(_brokerEntry, createSimpleLDAPAuthProviderRecord(authProviderId, "ldap", encryptedPassword));
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        broker.open();

        // check if SimpleLDAPAuthenticationManager returns decrypted password
        final SimpleLDAPAuthenticationManager<?> simpleLDAPAuthenticationManager =
                (SimpleLDAPAuthenticationManager<?>) broker.getAuthenticationProviders().iterator().next();
        assertEquals(password, simpleLDAPAuthenticationManager.getSearchPassword());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerWithMultipleAuthenticationProvidersAndPorts()
    {
        final UUID authProviderId = randomUUID();
        final UUID portId = randomUUID();
        final UUID authProvider2Id = randomUUID();
        final UUID port2Id = randomUUID();

        resolveObjects(_brokerEntry,
                       createAuthProviderRecord(authProviderId, "authProvider"),
                       createPortRecord(portId, 5672, "authProvider"),
                       createAuthProviderRecord(authProvider2Id, "authProvider2"),
                       createPortRecord(port2Id, 5673, "authProvider2"));
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        assertNotNull(broker);
        broker.open();
        assertEquals(_brokerId, broker.getId());
        assertEquals(2, (long) broker.getPorts().size());

        assertEquals(2, (long) broker.getAuthenticationProviders().size(),
                     "Unexpected number of authentication providers");

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateBrokerWithGroupProvider()
    {
        final UUID authProviderId = randomUUID();

        resolveObjects(_brokerEntry, createGroupProviderRecord(authProviderId, "groupProvider"));
        final Broker<?> broker = _systemConfig.getContainer(Broker.class);

        assertNotNull(broker);
        broker.open();
        assertEquals(_brokerId, broker.getId());
        assertEquals(1, (long) broker.getGroupProviders().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModelVersionValidationForIncompatibleMajorVersion() throws Exception
    {
        final Map<String, Object> brokerAttributes = new HashMap<>();
        final String[] incompatibleVersions = { Integer.MAX_VALUE + "." + 0, "0.0" };
        for (final String incompatibleVersion : incompatibleVersions)
        {
            // need to reset all the shared objects for every iteration of the test
            setUp();
            brokerAttributes.put(Broker.MODEL_VERSION, incompatibleVersion);
            brokerAttributes.put(Broker.NAME, getTestName());
            when(_brokerEntry.getAttributes()).thenReturn(brokerAttributes);

            resolveObjects(_brokerEntry);
            final Broker<?> broker = _systemConfig.getContainer(Broker.class);
            broker.open();
            assertEquals(State.ERRORED, broker.getState(), "Unexpected broker state");
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testModelVersionValidationForIncompatibleMinorVersion()
    {
        final Map<String, Object> brokerAttributes = new HashMap<>();
        final String incompatibleVersion = BrokerModel.MODEL_MAJOR_VERSION + "." + Integer.MAX_VALUE;
        brokerAttributes.put(Broker.MODEL_VERSION, incompatibleVersion);
        brokerAttributes.put(Broker.NAME, getTestName());

        when(_brokerEntry.getAttributes()).thenReturn(brokerAttributes);

        final UnresolvedConfiguredObject<? extends ConfiguredObject> recover =
                _systemConfig.getObjectFactory().recover(_brokerEntry, _systemConfig);

        final Broker<?> broker = (Broker<?>) (ConfiguredObject<?>) recover.resolve();
        broker.open();
        assertEquals(State.ERRORED, broker.getState(), "Unexpected broker state");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testIncorrectModelVersion()
    {
        final Map<String, Object> brokerAttributes = new HashMap<>();
        brokerAttributes.put(Broker.NAME, getTestName());

        final String[] versions = { Integer.MAX_VALUE + "_" + 0, "", null };
        for (final String modelVersion : versions)
        {
            brokerAttributes.put(Broker.MODEL_VERSION, modelVersion);
            when(_brokerEntry.getAttributes()).thenReturn(brokerAttributes);

            UnresolvedConfiguredObject<? extends ConfiguredObject> recover =
                    _systemConfig.getObjectFactory().recover(_brokerEntry, _systemConfig);
            final Broker<?> broker = (Broker<?>) (ConfiguredObject<?>) recover.resolve();
            broker.open();
            assertEquals(State.ERRORED, broker.getState(), "Unexpected broker state");
        }
    }

    private String convertToString(final Object attributeValue)
    {
        return String.valueOf(attributeValue);
    }

    private void resolveObjects(final ConfiguredObjectRecord... records)
    {
        final GenericRecoverer recoverer = new GenericRecoverer(_systemConfig);
        recoverer.recover(Arrays.asList(records), false);
    }

    private void makeFileDeletable(File file)
    {
        try
        {
            if (Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class) != null)
            {
                Files.setPosixFilePermissions(file.toPath(), EnumSet.of(PosixFilePermission.OTHERS_WRITE));
            }
            else if (Files.getFileAttributeView(file.toPath(), AclFileAttributeView.class) != null)
            {
                file.setWritable(true);
                final AclFileAttributeView attributeView =
                        Files.getFileAttributeView(file.toPath(), AclFileAttributeView.class);
                final ArrayList<AclEntry> acls = new ArrayList<>(attributeView.getAcl());

                final AclEntry.Builder builder = AclEntry.newBuilder();
                final UserPrincipal everyone = FileSystems.getDefault().getUserPrincipalLookupService()
                    .lookupPrincipalByName("Everyone");
                builder.setPrincipal(everyone);
                builder.setType(AclEntryType.ALLOW);
                builder.setPermissions(Stream.of(AclEntryPermission.values()).collect(Collectors.toSet()));
                acls.add(builder.build());
                attributeView.setAcl(acls);
            }
            else
            {
                throw new IllegalConfigurationException("Failed to change file permissions");
            }
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Failed to change file permissions", e);
        }
    }
}
