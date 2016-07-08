/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TaskPrincipal;

public class SecurityManager
{

    private static final SystemPrincipal SYSTEM_PRINCIPAL = new SystemPrincipal();
    private static final Subject SYSTEM = new Subject(true,
                                                     Collections.singleton(SYSTEM_PRINCIPAL),
                                                     Collections.emptySet(),
                                                     Collections.emptySet());

    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityManager.class);


    public SecurityManager()
    {

    }

    public static Subject getSubjectWithAddedSystemRights()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if(subject == null)
        {
            subject = new Subject();
        }
        else
        {
            subject = new Subject(false, subject.getPrincipals(), subject.getPublicCredentials(), subject.getPrivateCredentials());
        }
        subject.getPrincipals().addAll(SYSTEM.getPrincipals());
        subject.setReadOnly();
        return subject;
    }


    public static Subject getSystemTaskSubject(String taskName)
    {
        return getSystemSubject(new TaskPrincipal(taskName));
    }

    public static AccessControlContext getSystemTaskControllerContext(String taskName, Principal principal)
    {
        final Subject subject = getSystemTaskSubject(taskName, principal);
        final AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged
                (new PrivilegedAction<AccessControlContext>()
                {
                    public AccessControlContext run()
                    {
                        if (subject == null)
                            return new AccessControlContext(acc, null);
                        else
                            return new AccessControlContext
                                    (acc,
                                     new SubjectDomainCombiner(subject));
                    }
                });
    }

    public static Subject getSystemTaskSubject(String taskName, Principal principal)
    {
        return getSystemSubject(new TaskPrincipal(taskName), principal);
    }

    private static Subject getSystemSubject(Principal... principals)
    {
        Subject subject = new Subject(false, SYSTEM.getPrincipals(), SYSTEM.getPublicCredentials(), SYSTEM.getPrivateCredentials());
        if (principals !=null)
        {
            for (Principal principal:principals)
            {
                subject.getPrincipals().add(principal);
            }
        }
        subject.setReadOnly();
        return subject;
    }

    public static boolean isSystemProcess()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        return isSystemSubject(subject);
    }

    private static boolean isSystemSubject(final Subject subject)
    {
        return subject != null  && subject.getPrincipals().contains(SYSTEM_PRINCIPAL);
    }

    public static AuthenticatedPrincipal getCurrentUser()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        final AuthenticatedPrincipal user;
        if(subject != null)
        {
            Set<AuthenticatedPrincipal> principals = subject.getPrincipals(AuthenticatedPrincipal.class);
            if(!principals.isEmpty())
            {
                user = principals.iterator().next();
            }
            else
            {
                user = null;
            }
        }
        else
        {
            user = null;
        }
        return user;
    }

    public static AccessControlContext getAccessControlContextFromSubject(final Subject subject)
    {
        final AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged
                (new PrivilegedAction<AccessControlContext>()
                {
                    public AccessControlContext run()
                    {
                        if (subject == null)
                            return new AccessControlContext(acc, null);
                        else
                            return new AccessControlContext
                                    (acc,
                                     new SubjectDomainCombiner(subject));
                    }
                });
    }


    private static final class SystemPrincipal implements Principal
    {
        private SystemPrincipal()
        {
        }

        @Override
        public String getName()
        {
            return "SYSTEM";
        }
    }


}
