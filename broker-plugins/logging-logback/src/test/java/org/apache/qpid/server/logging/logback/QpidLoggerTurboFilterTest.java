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
package org.apache.qpid.server.logging.logback;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class QpidLoggerTurboFilterTest extends UnitTestBase
{
    private LoggerContext _loggerContext;
    private QpidLoggerTurboFilter _turboFilter;

    @Before
    public void setUp() throws Exception
    {
        _loggerContext = new LoggerContext();
        _turboFilter = QpidLoggerTurboFilter.installIfNecessary(_loggerContext);
    }

    @After
    public void tearDown() throws Exception
    {
        QpidLoggerTurboFilter.uninstall(_loggerContext);
    }

    @Test
    public void testDebugOffByDefault()
    {
        Logger fooLogger = _loggerContext.getLogger("foo");
        if(fooLogger.isDebugEnabled())
        {
            fail("debug should not be enabled by default");
        }
    }


    @Test
    public void testInstallFilterWorksCorrectly()
    {
        Logger fooBarLogger = _loggerContext.getLogger("foo.bar");
        if(fooBarLogger.isDebugEnabled())
        {
            fail("debug should not be enabled by default");
        }
        if(fooBarLogger.isInfoEnabled())
        {
            fail("info should not be enabled by default");
        }

        final LoggerNameAndLevelFilter allFooInfo = new LoggerNameAndLevelFilter("foo.*", Level.INFO);
        _turboFilter.filterAdded(allFooInfo);

        if(!fooBarLogger.isInfoEnabled())
        {
            fail("info should be enabled after filter added");
        }
        if(fooBarLogger.isDebugEnabled())
        {
            fail("debug should not be enabled after info enabled");
        }

        final LoggerNameAndLevelFilter fooBarDebugFilter = new LoggerNameAndLevelFilter("foo.bar", Level.DEBUG);
        _turboFilter.filterAdded(fooBarDebugFilter);
        if(!fooBarLogger.isDebugEnabled())
        {
            fail("debug should now be enabled");
        }
        final Logger fooBazLogger = _loggerContext.getLogger("foo.baz");
        if(fooBazLogger.isDebugEnabled())
        {
            fail("debug should not be enabled after for foo.baz");
        }

        _turboFilter.filterRemoved(allFooInfo);
        if(!fooBarLogger.isInfoEnabled())
        {
            fail("info should be still be enabled fo foo.bar");
        }

        if(fooBazLogger.isInfoEnabled())
        {
            fail("info should not still be enabled fo foo.baz");
        }
    }

    @Test
    public void testUninstall()
    {
        Logger fooBarLogger = _loggerContext.getLogger("foo.bar");
        assertFalse(
                "Debug should not be enabled when QpidLoggerTurboFilter is installed but no regular filter is set",
                fooBarLogger.isDebugEnabled());

        QpidLoggerTurboFilter.uninstall(_loggerContext);
        assertTrue("Debug should be enabled as per test logback configuration", fooBarLogger.isDebugEnabled());
    }
}
