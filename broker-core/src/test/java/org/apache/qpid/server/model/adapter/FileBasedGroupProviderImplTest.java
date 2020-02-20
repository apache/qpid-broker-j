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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.Group;
import org.apache.qpid.server.model.GroupMember;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileBasedGroupProviderImplTest extends UnitTestBase
{
    private Broker<?> _broker;
    private File _groupFile;
    private ConfiguredObjectFactory _objectFactory;

    @Before
    public void setUp() throws Exception
    {

        _broker = BrokerTestHelper.createBrokerMock();
        _objectFactory = _broker.getObjectFactory();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_groupFile.exists())
            {
                _groupFile.delete();
            }
        }
        finally
        {
        }
    }

    @Test
    public void testValidationOnCreateWithInvalidPath()
    {
        Map<String,Object> attributes = new HashMap<>();
        _groupFile = TestFileUtils.createTempFile(this, "groups");

        String groupsFile = _groupFile.getAbsolutePath() + File.separator + "groups";
        assertFalse("File should not exist", new File(groupsFile).exists());
        attributes.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, groupsFile);
        attributes.put(FileBasedGroupProvider.NAME, getTestName());

        try
        {
            _objectFactory.create(GroupProvider.class, attributes, _broker);
            fail("Exception is expected on validation of groups provider with invalid path");
        } catch (IllegalConfigurationException e)
        {
            assertEquals("Unexpected exception message:" + e.getMessage(),
                                String.format("Cannot create groups file at '%s'", groupsFile),
                                e.getMessage());

        }
    }

    @Test
    public void testValidationOnCreateWithInvalidGroups()
    {
        _groupFile = TestFileUtils.createTempFile(this, "groups", "=blah");
        Map<String, Object> attributes = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        attributes.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, groupsFile);
        attributes.put(FileBasedGroupProvider.NAME, getTestName());

        try
        {
            _objectFactory.create(GroupProvider.class, attributes, _broker);
            fail("Exception is expected on validation of groups provider with invalid group file");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Unexpected exception message:" + e.getMessage(),
                                String.format("Cannot load groups from '%s'", groupsFile),
                                e.getMessage());
        }
    }

    @Test
    public void testExistingGroupFile() throws Exception
    {
        Map<String, Set<String>> input = new HashMap<>();
        input.put("super", Sets.newHashSet("root"));

        _groupFile = createTemporaryGroupFile(input);

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        @SuppressWarnings("unchecked")
        GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);

        Set<Principal> adminGroups = provider.getGroupPrincipalsForUser(() -> "root");
        assertThat("root has unexpected group membership",
                   adminGroups.stream().map(Principal::getName).collect(Collectors.toSet()),
                   containsInAnyOrder("super"));

        Collection<Group> groups = provider.getChildren(Group.class);
        assertThat(groups.size(), is(equalTo(1)));
        Group<?> superGroup = groups.iterator().next();
        assertThat(superGroup.getName(), is(equalTo("super")));

        Collection<GroupMember> members = superGroup.getChildren(GroupMember.class);
        assertThat(members.size(), is(equalTo(1)));
        GroupMember rootMember = members.iterator().next();
        assertThat(rootMember.getName(), is(equalTo("root")));
    }


    @Test
    public void testGetGroupPrincipalsForUserCaseAware() throws Exception
    {
        Map<String, Set<String>> input = new HashMap<>();
        input.put("super", Sets.newHashSet("root"));

        _groupFile = createTemporaryGroupFile(input);

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        @SuppressWarnings("unchecked")
        GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
        assertThat(provider, is(instanceOf(FileBasedGroupProvider.class)));
        assertThat(((FileBasedGroupProvider)provider).isCaseSensitive(), is(true));

        Set<Principal> adminGroups = provider.getGroupPrincipalsForUser(() -> "Root");
        assertThat("No group should be found when caseSensitive=true",
                   adminGroups.stream().map(Principal::getName).collect(Collectors.toSet()),
                  is(empty()));

        provider.setAttributes(Collections.singletonMap("caseSensitive", false));
        assertThat(((FileBasedGroupProvider)provider).isCaseSensitive(), is(false));
        Set<Principal> adminGroups2 = provider.getGroupPrincipalsForUser(() -> "Root");
        assertThat("root has unexpected group membership",
                   adminGroups2.stream().map(Principal::getName).collect(Collectors.toSet()),
                   containsInAnyOrder("super"));
    }

    @Test
    public void testAddGroupAndMember() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Collections.emptyMap());

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        @SuppressWarnings("unchecked")
        GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

        final Map<String, Object> groupAttrs = Collections.singletonMap(Group.NAME, "supers");
        Group superGroup = provider.createChild(Group.class, groupAttrs);
        assertThat(superGroup.getName(), is(equalTo("supers")));

        final Map<String, Object> memberAttrs = Collections.singletonMap(GroupMember.NAME, "root");
        GroupMember rootMember = (GroupMember) superGroup.createChild(GroupMember.class, memberAttrs);
        assertThat(rootMember.getName(), is(equalTo("root")));
    }

    @Test
    public void testRemoveGroupAndMember() throws Exception
    {
        Map<String, Set<String>> input = new HashMap<>();
        input.put("supers", Sets.newHashSet( "root"));
        input.put("operators", Sets.newHashSet("operator", "root"));

        _groupFile = createTemporaryGroupFile(input);

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        @SuppressWarnings("unchecked")
        GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);

        assertThat(provider.getChildren(Group.class).size(), is(equalTo(2)));

        Group operators = provider.getChildByName(Group.class, "operators");
        GroupMember rootMember = (GroupMember) operators.getChildByName(GroupMember.class, "root");
        rootMember.delete();

        assertThat(operators.getChildren(GroupMember.class).size(), is(equalTo(1)));
        Group supers = provider.getChildByName(Group.class, "supers");
        assertThat(supers.getChildren(GroupMember.class).size(), is(equalTo(1)));

        operators.delete();
        assertThat(provider.getChildren(Group.class).size(), is(equalTo(1)));
    }

    @Test
    public void testGroupAndMemberDurability() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Collections.emptyMap());

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        {
            @SuppressWarnings("unchecked")
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));

            final Map<String, Object> groupAttrs = Collections.singletonMap(Group.NAME, "group");
            final Group group = provider.createChild(Group.class, groupAttrs);

            final Map<String, Object> memberAttrs = Collections.singletonMap(GroupMember.NAME, "user");
            group.createChild(GroupMember.class, memberAttrs);

            provider.close();
        }

        {
            @SuppressWarnings("unchecked")
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(1)));

            final Group group = provider.getChildByName(Group.class, "group");
            assertThat(group.getChildren(GroupMember.class).size(), is(equalTo(1)));
            final GroupMember member = (GroupMember) group.getChildByName(GroupMember.class, "user");

            member.delete();
            provider.close();
        }

        {
            @SuppressWarnings("unchecked")
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            final Group group = provider.getChildByName(Group.class, "group");
            assertThat(group.getChildren(GroupMember.class).size(), is(equalTo(0)));

            group.delete();
            provider.close();
        }

        {
            @SuppressWarnings("unchecked")
            final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(Group.class).size(), is(equalTo(0)));
            provider.close();
        }
    }

    @Test
    public void testProvideDelete() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Collections.emptyMap());

        Map<String, Object> providerAttrs = new HashMap<>();
        String groupsFile = _groupFile.getAbsolutePath();
        providerAttrs.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs.put(FileBasedGroupProvider.NAME, getTestName());

        @SuppressWarnings("unchecked")
        final GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs, _broker);

        provider.delete();
        assertThat(_groupFile.exists(), is(equalTo(false)));
    }

    @Test
    public void testSharingUnderlyingFileDisallowed() throws Exception
    {
        _groupFile = createTemporaryGroupFile(Collections.emptyMap());
        String groupsFile = _groupFile.getAbsolutePath();

        Map<String, Object> providerAttrs1 = new HashMap<>();
        providerAttrs1.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
        providerAttrs1.put(FileBasedGroupProvider.PATH, groupsFile);
        providerAttrs1.put(FileBasedGroupProvider.NAME, getTestName() + "1");

        @SuppressWarnings("unchecked")
        GroupProvider<?> provider = _objectFactory.create(GroupProvider.class, providerAttrs1, _broker);

        when(_broker.getChildren(GroupProvider.class)).thenReturn(Collections.singletonList(provider));
        try
        {

            Map<String, Object> providerAttrs2 = new HashMap<>();
            providerAttrs2.put(FileBasedGroupProvider.TYPE, GROUP_FILE_PROVIDER_TYPE);
            providerAttrs2.put(FileBasedGroupProvider.PATH, groupsFile);
            providerAttrs2.put(FileBasedGroupProvider.NAME, getTestName() + "2");

            _objectFactory.create(GroupProvider.class, providerAttrs2, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // PASS
        }
    }

    private File createTemporaryGroupFile(Map<String, Set<String>> groups) throws Exception
    {
        File groupFile = File.createTempFile("group", "grp");
        groupFile.deleteOnExit();

        Properties props = new Properties();

        Map<String, String> m = groups.entrySet()
                                      .stream()
                                      .collect(Collectors.toMap(e -> e.getKey() + ".users",
                                                                e -> e.getValue()
                                                                      .stream()
                                                                      .collect(Collectors.joining(","))));
        props.putAll(m);
        try(final FileOutputStream out = new FileOutputStream(groupFile))
        {
            props.store(out, "test group file");
        }
        return groupFile;
    }
}
