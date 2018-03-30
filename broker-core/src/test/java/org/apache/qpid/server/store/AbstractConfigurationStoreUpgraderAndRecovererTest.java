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

package org.apache.qpid.server.store;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractConfigurationStoreUpgraderAndRecovererTest extends UnitTestBase
{

    private TestConfigurationStoreUpgraderAndRecoverer _recoverer;

    @Before
    public void setUp() throws Exception
    {
        _recoverer = new TestConfigurationStoreUpgraderAndRecoverer();
    }

    @Test
    public void testRegister()
    {
        _recoverer.register(new TestStoreUpgraderPhase("0.0", "1.0"));
        _recoverer.register(new TestStoreUpgraderPhase("1.0", "1.1"));
        _recoverer.register(new TestStoreUpgraderPhase("1.1", "2.0"));
    }

    @Test
    public void testRegisterFailsOnUnknownFromVersion()
    {
        _recoverer.register(new TestStoreUpgraderPhase("0.0", "1.0"));
        try
        {
            _recoverer.register(new TestStoreUpgraderPhase("1.1", "2.0"));
            fail("should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testRegisterFailsOnNoVersionNumberChange()
    {
        _recoverer.register(new TestStoreUpgraderPhase("0.0", "1.0"));
        try
        {
            _recoverer.register(new TestStoreUpgraderPhase("1.0", "1.0"));
            fail("should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testRegisterFailsOnDuplicateFromVersion()
    {
        _recoverer.register(new TestStoreUpgraderPhase("0.0", "1.0"));
        try
        {
            _recoverer.register(new TestStoreUpgraderPhase("0.0", "2.0"));
            fail("should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    @Test
    public void testRegisterFailsOnUnexpectedFromVersionInFirstUpgrader()
    {
        try
        {
            _recoverer.register(new TestStoreUpgraderPhase("0.1", "1.0"));
            fail("should fail");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    private static class TestConfigurationStoreUpgraderAndRecoverer extends AbstractConfigurationStoreUpgraderAndRecoverer
    {
        TestConfigurationStoreUpgraderAndRecoverer()
        {
            super("0.0");
        }
    }

    private static class TestStoreUpgraderPhase extends StoreUpgraderPhase
    {
        TestStoreUpgraderPhase(String fromVersion, String toVersion)
        {
            super("", fromVersion, toVersion);
        }

        @Override
        public void configuredObject(final ConfiguredObjectRecord record)
        {
        }

        @Override
        public void complete()
        {
        }
    }
}
