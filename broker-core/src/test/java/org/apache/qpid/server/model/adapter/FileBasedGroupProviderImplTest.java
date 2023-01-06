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
package org.apache.qpid.server.model.adapter;

import static org.apache.qpid.server.model.adapter.FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.Group;
import org.apache.qpid.server.model.GroupMember;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class FileBasedGroupProviderImplTest extends UnitTestBase
{
    private Broker<?> _broker;
    private File _groupFile;
    private ConfiguredObjectFactory _objectFactory;

    @BeforeEach
    public void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createBrokerMock();
        _objectFactory = _broker.getObjectFactory();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_groupFile.exists())
        {
            _groupFile.delete();
        }
    }

    @Test
    public void testValidationOnCreateWithInvalidPath()
    {
        _groupFile = TestFileUtils.createTempFile(this, "groups");
        final String groupsFile = _groupFile.getAbsolutePath() + File.separator + "groups";
        assertFalse(new File(groupsFile).exists(), "File should not exist");
        final Map<String, Object> attributes = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName());
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(GroupProvider.class, attributes, _broker),
                "Exception is expected on validation of groups provider with invalid path");
        assertEquals(String.format("Cannot create groups file at '%s'", groupsFile), thrown.getMessage(),
                "Unexpected exception message:" + thrown.getMessage());
    }

    @Test
    public void testValidationOnCreateWithInvalidGroups()
    {
        _groupFile = TestFileUtils.createTempFile(this, "groups", "=blah");
        final String groupsFile = _groupFile.getAbsolutePath();
        final Map<String, Object> attributes = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName());
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(GroupProvider.class, attributes, _broker),
                "Exception is expected on validation of groups provider with invalid group file");
        assertEquals(String.format("Cannot load groups from '%s'", groupsFile),thrown.getMessage(),
                "Unexpected exception message:" + thrown.getMessage());
    }

    @Test
    public void testExistingGroupFile() throws Exception
    {
        final Map<String, Set<String>> input = Map.of("super", Set.of("root"));
        final GroupProvider<?> provider = createGroupProvider(input);
        final Set<Principal> adminGroups = provider.getGroupPrincipalsForUser(() -> "root");
        assertThat("root has unexpected group membership",
                adminGroups.stream().map(Principal::getName).collect(Collectors.toSet()), containsInAnyOrder("super"));

        final Collection<Group> groups = provider.getChildren(Group.class);
        assertThat(groups.size(), is(equalTo(1)));
        final Group<?> superGroup = groups.iterator().next();
        assertThat(superGroup.getName(), is(equalTo("super")));

        final Collection<GroupMember> members = superGroup.getChildren(GroupMember.class);
        assertThat(members.size(), is(equalTo(1)));
        final GroupMember<?> rootMember = members.iterator().next();
        assertThat(rootMember.getName(), is(equalTo("root")));
    }

    @Test
    public void testGetGroupPrincipalsForUserCaseAware() throws Exception
    {
        final Map<String, Set<String>> input = Map.of("super", Set.of("root"));
        final GroupProvider<?> provider = createGroupProvider(input);
        assertThat(provider, is(instanceOf(FileBasedGroupProvider.class)));
        assertThat(((FileBasedGroupProvider<?>) provider).isCaseSensitive(), is(true));

        final Set<Principal> adminGroups = provider.getGroupPrincipalsForUser(() -> "Root");
        assertThat("No group should be found when caseSensitive=true",
                adminGroups.stream().map(Principal::getName).collect(Collectors.toSet()), is(empty()));

        provider.setAttributes(Map.of("caseSensitive", false));
        assertThat(((FileBasedGroupProvider<?>) provider).isCaseSensitive(), is(false));
        Set<Principal> adminGroups2 = provider.getGroupPrincipalsForUser(() -> "Root");
        assertThat("root has unexpected group membership",
                adminGroups2.stream().map(Principal::getName).collect(Collectors.toSet()), containsInAnyOrder("super"));
    }

    @Test
    public void testAddGroupAndMember() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final String groupName = "supers";
        final Group<?> superGroup = provider.createChild(Group.class, Map.of(Group.NAME, groupName));
        assertThat(superGroup.getName(), is(equalTo(groupName)));

        final Map<String, Object> memberAttrs = Map.of(GroupMember.NAME, "root");
        final GroupMember<?> rootMember = (GroupMember<?>) superGroup.createChild(GroupMember.class, memberAttrs);
        assertThat(rootMember.getName(), is(equalTo("root")));
    }

    @Test
    public void testRemoveGroupAndMember() throws Exception
    {
        final Map<String, Set<String>> input = Map.of("supers", Set.of("root"),
                "operators", Set.of("operator", "root"));
        final GroupProvider<?> provider = createGroupProvider(input);

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(2)));

        final Group<?> operators = provider.getChildByName(Group.class, "operators");
        final GroupMember<?> rootMember = (GroupMember<?>) operators.getChildByName(GroupMember.class, "root");
        rootMember.delete();

        assertThat(operators.getChildren(GroupMember.class).size(), is(equalTo(1)));
        final Group<?> supers = provider.getChildByName(Group.class, "supers");
        assertThat(supers.getChildren(GroupMember.class).size(), is(equalTo(1)));

        operators.delete();
        assertThat(provider.getChildren(Group.class).size(), is(equalTo(1)));
    }

    @Test
    public void testGroupAndMemberDurability() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Map.of());
        final String groupsFile = _groupFile.getAbsolutePath();
        final Map<String, Object> providerAttrs = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName());

        {
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

            final Map<String, Object> groupAttrs = Map.of(Group.NAME, "group");
            final Group<?> group = provider.createChild(Group.class, groupAttrs);

            final Map<String, Object> memberAttrs = Map.of(GroupMember.NAME, "user");
            group.createChild(GroupMember.class, memberAttrs);

            provider.close();
        }

        {
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(1)));

            final Group<?> group = provider.getChildByName(Group.class, "group");
            assertThat(group.getChildren(GroupMember.class).size(), is(equalTo(1)));
            final GroupMember<?> member = (GroupMember<?>) group.getChildByName(GroupMember.class, "user");

            member.delete();
            provider.close();
        }

        {
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            final Group<?> group = provider.getChildByName(Group.class, "group");
            assertThat(group.getChildren(GroupMember.class).size(), is(equalTo(0)));

            group.delete();
            provider.close();
        }

        {
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));
            provider.close();
        }
    }

    @Test
    public void testProvideDelete() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        provider.delete();
        assertThat(_groupFile.exists(), is(equalTo(false)));
    }

    @Test
    public void testSharingUnderlyingFileDisallowed() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Map.of());
        final String groupsFile = _groupFile.getAbsolutePath();
        final Map<String, Object> providerAttrs1 = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName() + "1");
        final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs1, _broker);

        when(_broker.getChildren(GroupProvider.class)).thenReturn(Collections.singletonList(provider));

        final Map<String, Object> providerAttrs2 = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName() + "2");

        assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(GroupProvider.class, providerAttrs2, _broker),
                "Exception not thrown");
    }

    @Test
    public void testCreateDuplicateGroup() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final String groupName = "supers";
        final Group<?> superGroup = provider.createChild(Group.class, Map.of(Group.NAME, groupName));
        assertThat(superGroup.getName(), is(equalTo(groupName)));

        assertThrows(IllegalConfigurationException.class,
                     () -> provider.createChild(Group.class, Map.of(Group.NAME, groupName)),
                     "Group member with name root1 already exists");
    }

    @Test
    public void testCreateDuplicateMember() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final String groupName = "supers";
        final Group<?> superGroup = provider.createChild(Group.class, Map.of(Group.NAME, groupName));
        assertThat(superGroup.getName(), is(equalTo(groupName)));

        final String memberName = "root1";
        final Map<String, Object> memberAttributes = Map.of(GroupMember.NAME, memberName);
        final GroupMember<?> rootMember = (GroupMember<?>) superGroup.createChild(GroupMember.class, memberAttributes);
        assertThat(rootMember.getName(), is(equalTo(memberName)));

        assertThrows(IllegalConfigurationException.class,
                     () -> superGroup.createChild(GroupMember.class, memberAttributes),
                     "Group member with name root1 already exists");
        assertThat(superGroup.getChildren(GroupMember.class).size(), is(equalTo(1)));
    }

    @Test
    public void testCreateGroups() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final String groupName = "supers";
        final Group<?> superGroup = provider.createChild(Group.class, Map.of(Group.NAME, groupName));
        assertThat(superGroup.getName(), is(equalTo(groupName)));

        final String groupName2 = "Supers";
        final Group<?> superGroup2 = provider.createChild(Group.class, Map.of(Group.NAME, groupName2));
        assertThat(superGroup2.getName(), is(equalTo(groupName2)));

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(2)));
    }

    @Test
    public void testCreateMembers() throws Exception
    {
        final GroupProvider<?> provider = createGroupProvider(Map.of());

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final String groupName = "supers";
        final Group<?> superGroup = provider.createChild(Group.class, Map.of(Group.NAME, groupName));
        assertThat(superGroup.getName(), is(equalTo(groupName)));

        final String memberName = "root1";
        final Map<String, Object> memberAttrs1 = Map.of(GroupMember.NAME, memberName);
        final GroupMember<?> rootMember = superGroup.createChild(GroupMember.class, memberAttrs1);
        assertThat(rootMember.getName(), is(equalTo(memberName)));

        assertThrows(IllegalConfigurationException.class,
                     () -> superGroup.createChild(GroupMember.class, memberAttrs1),
                     "Group member with name root1 already exists");
        assertThat(superGroup.getChildren(GroupMember.class).size(), is(equalTo(1)));
    }

    private File createTemporaryGroupFile(final Map<String, Set<String>> groups) throws Exception
    {
        final File groupFile = File.createTempFile("group", "grp");
        groupFile.deleteOnExit();

        final Properties props = new Properties();

        final Map<String, String> m = groups.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey() + ".users", e -> String.join(",", e.getValue())));
        props.putAll(m);
        try (final FileOutputStream out = new FileOutputStream(groupFile))
        {
            props.store(out, "test group file");
        }
        return groupFile;
    }

    private GroupProvider<?> createGroupProvider(final Map<String, Set<String>> objectObjectMap) throws Exception
    {
        _groupFile = createTemporaryGroupFile(objectObjectMap);
        final String groupsFile = _groupFile.getAbsolutePath();
        final Map<String, Object> providerAttrs = Map.of(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE,
                FileBasedGroupProvider.PATH, groupsFile,
                FileBasedGroupProvider.NAME, getTestName());
        return _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
    }
}
