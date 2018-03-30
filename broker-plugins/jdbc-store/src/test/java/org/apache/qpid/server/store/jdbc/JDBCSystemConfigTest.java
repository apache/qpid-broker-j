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

package org.apache.qpid.server.store.jdbc;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.test.utils.UnitTestBase;

public class JDBCSystemConfigTest extends UnitTestBase
{
    @Test
    public void testInvalidTableNamePrefix() throws Exception
    {
        final TaskExecutor taskExecutor = new CurrentThreadTaskExecutor();
        final EventLogger eventLogger = mock(EventLogger.class);
        final Principal systemPrincipal = mock(Principal.class);
        JDBCSystemConfig<?> jdbcSystemConfig = new JDBCSystemConfigImpl(taskExecutor, eventLogger, systemPrincipal,
                                                                        Collections.<String, Object>emptyMap());

        // This list is not exhaustive
        List<String> knownInvalidPrefixes = Arrays.asList("with\"dblquote",
                                                          "with'quote",
                                                          "with-dash",
                                                          "with;semicolon",
                                                          "with space",
                                                          "with%percent",
                                                          "with|pipe",
                                                          "with(paren",
                                                          "with)paren",
                                                          "with[bracket",
                                                          "with]bracket",
                                                          "with{brace",
                                                          "with}brace");
        for (String invalidPrefix : knownInvalidPrefixes)
        {
            try
            {
                jdbcSystemConfig.setAttributes(Collections.<String, Object>singletonMap("tableNamePrefix",
                                                                                        invalidPrefix));
                fail(String.format("Should not be able to set prefix to '%s'", invalidPrefix));
            }
            catch (IllegalConfigurationException e)
            {
                // pass
            }
        }
    }
}
