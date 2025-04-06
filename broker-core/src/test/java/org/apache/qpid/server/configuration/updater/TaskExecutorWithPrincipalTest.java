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

package org.apache.qpid.server.configuration.updater;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.qpid.server.BrokerPrincipal;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.virtualhost.VirtualHostPrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

class TaskExecutorWithPrincipalTest extends UnitTestBase
{
    final VirtualHost _virtualHost = mock(VirtualHost.class);
    final VirtualHostPrincipal _virtualHostPrincipal = new VirtualHostPrincipal(_virtualHost);

    private TaskExecutorImpl _executor;

    @BeforeEach
    void setUp()
    {
        _executor = new TaskExecutorImpl("Broker-Config", () -> _virtualHostPrincipal);
        _executor.start();
    }

    @AfterEach
    void tearDown()
    {
        _executor.stopImmediately();
    }

    @Test
    void emptySubject()
    {
        final Subject subject = new Subject();
        final AtomicReference<Subject> taskSubject = new AtomicReference<>();

        runTask(subject, taskSubject, _executor);

        assertEquals(Set.of(_virtualHostPrincipal), taskSubject.get().getPrincipals(), "Unexpected security manager principal");
    }

    @Test
    void subjectWithBrokerPrincipal()
    {
        final Broker<?> broker = mock(Broker.class);
        final BrokerPrincipal brokerPrincipal = new BrokerPrincipal(broker);
        final Subject subject = new Subject(true, Set.of(brokerPrincipal), Set.of(), Set.of());
        final AtomicReference<Subject> taskSubject = new AtomicReference<>();

        runTask(subject, taskSubject, _executor);

        assertEquals(2, taskSubject.get().getPrincipals().size(), "Unexpected principals count");
        assertTrue(taskSubject.get().getPrincipals().contains(brokerPrincipal), "Expected to have broker principal");
        assertTrue(taskSubject.get().getPrincipals().contains(_virtualHostPrincipal), "Expected to have virtualhost principal");
    }

    @Test
    void cachedSubjects()
    {
        final TaskExecutorImpl spy1 = spy(_executor);
        final TaskExecutorImpl spy2 = spy(_executor);
        final AuthenticationProvider authProvider = mock(AuthenticationProvider.class);
        when(authProvider.getName()).thenReturn("authProvider");
        when(authProvider.getType()).thenReturn("mock");
        final UsernamePrincipal usernamePrincipal1 = new UsernamePrincipal("user1", authProvider);
        final UsernamePrincipal usernamePrincipal2 = new UsernamePrincipal("user2", authProvider);
        final Subject subject1 = new Subject(true, Set.of(usernamePrincipal1), Set.of(), Set.of());
        final Subject subject2 = new Subject(true, Set.of(usernamePrincipal2), Set.of(), Set.of());
        final AtomicReference<Subject> taskSubject = new AtomicReference<>();

        // subject1 should be created
        runTask(subject1, taskSubject, spy1);
        verify(spy1, times(1)).createSubjectWithPrincipals(any(Set.class), any(Subject.class));

        // repeated call should retrieve subject1 from cache
        runTask(subject1, taskSubject, spy2);
        verify(spy2, never()).createSubjectWithPrincipals(any(Set.class), any(Subject.class));

        // subject2 should be created
        runTask(subject2, taskSubject, spy1);
        verify(spy1, times(2)).createSubjectWithPrincipals(any(Set.class), any(Subject.class));

        // repeated call should retrieve subject2 from cache
        runTask(subject2, taskSubject, spy2);
        verify(spy2, never()).createSubjectWithPrincipals(any(Set.class), any(Subject.class));
    }

    private void runTask(final Subject subject, final AtomicReference<Subject> taskSubject, final TaskExecutorImpl executor)
    {
        Subject.doAs(subject, (PrivilegedAction<Object>) () ->
        {
            executor.run(new Task<Void, RuntimeException>()
            {
                @Override
                public Void execute()
                {
                    taskSubject.set(Subject.getSubject(AccessController.getContext()));
                    return null;
                }

                @Override
                public String getObject()
                {
                    return getTestName();
                }

                @Override
                public String getAction()
                {
                    return "test";
                }

                @Override
                public String getArguments()
                {
                    return null;
                }
            });
            return null;
        });
    }
}
