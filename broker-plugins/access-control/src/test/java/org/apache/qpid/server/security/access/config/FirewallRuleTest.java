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

package org.apache.qpid.server.security.access.config;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import javax.security.auth.Subject;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class FirewallRuleTest extends UnitTestBase
{

    @Test
    public void subjectMatchesAllRules()
    {
        final Subject subject = new Subject();
        final FirewallRule rule1 = s -> s.equals(subject);
        final FirewallRule rule2 = s -> s.equals(subject);

        final FirewallRule combined = rule1.and(rule2);

        assertThat(combined.matches(subject), is(true));
    }

    @Test
    public void subjectDoesNotMatchAllRules()
    {
        final Subject subject = new Subject();
        final FirewallRule rule1 = s -> s.equals(subject);
        final FirewallRule rule2 = s -> !s.equals(subject);

        final FirewallRule combined = rule1.and(rule2);

        assertThat(combined.matches(subject), is(false));
    }
}
