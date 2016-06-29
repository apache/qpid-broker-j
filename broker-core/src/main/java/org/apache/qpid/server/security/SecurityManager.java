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
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TaskPrincipal;

public class SecurityManager
{

    private static final SystemPrincipal SYSTEM_PRINCIPAL = new SystemPrincipal();
    private static final Subject SYSTEM = new Subject(true,
                                                     Collections.singleton(SYSTEM_PRINCIPAL),
                                                     Collections.emptySet(),
                                                     Collections.emptySet());

    private final boolean _managementMode;
    private final ConfiguredObject<?> _aclProvidersParent;

    public SecurityManager(ConfiguredObject<?> aclProvidersParent, boolean managementMode)
    {
        _managementMode = managementMode;
        _aclProvidersParent = aclProvidersParent;
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

    public static boolean isSystemSubject(final Subject subject)
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
            if(principals != null && !principals.isEmpty())
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

    public SecurityToken newToken(final Subject subject)
    {
        Collection<AccessControlProvider> accessControlProviders = _aclProvidersParent.getChildren(AccessControlProvider.class);
        if(accessControlProviders != null && !accessControlProviders.isEmpty())
        {
            AccessControlProvider<?> accessControlProvider = accessControlProviders.iterator().next();
            if (accessControlProvider != null
                && accessControlProvider.getState() == State.ACTIVE
                && accessControlProvider.getAccessControl() != null)
            {
                return accessControlProvider.getAccessControl().newToken(subject);
            }
        }
        return null;
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

    private abstract class AccessCheck
    {
        abstract Result allowed(AccessControl plugin);
    }

    private boolean checkAllPlugins(AccessCheck checker)
    {
        return checkAllPlugins(checker, Subject.getSubject(AccessController.getContext()));

    }

    private boolean checkAllPlugins(AccessCheck checker, Subject subject)
    {
        // If we are running as SYSTEM then no ACL checking
        if(isSystemSubject(subject) || _managementMode)
        {
            return true;
        }


        Collection<AccessControlProvider> accessControlProviders = _aclProvidersParent.getChildren(AccessControlProvider.class);
        if(accessControlProviders != null && !accessControlProviders.isEmpty())
        {
            AccessControlProvider<?> accessControlProvider = accessControlProviders.iterator().next();
            if (accessControlProvider != null
                && accessControlProvider.getState() == State.ACTIVE
                && accessControlProvider.getAccessControl() != null)
            {
                Result remaining = checker.allowed(accessControlProvider.getAccessControl());
                if (remaining == Result.DEFER)
                {
                    remaining = accessControlProvider.getAccessControl().getDefault();
                }
                if (remaining == Result.DENIED)
                {
                    return false;
                }
            }
            else
            {
                // Default to DENY when the accessControlProvider is in unhealthy state.
                return false;
            }
        }
        // getting here means either allowed or abstained from all plugins
        return true;
    }

    public void authoriseCreate(ConfiguredObject<?> object)
    {
        authorise(Operation.CREATE, object);
    }

    public void authoriseUpdate(ConfiguredObject<?> configuredObject)
    {
        authorise(Operation.UPDATE, configuredObject);
    }

    public void authoriseDelete(ConfiguredObject<?> configuredObject)
    {
        authorise(Operation.DELETE, configuredObject);
    }

    public void authorise(Operation operation, ConfiguredObject<?> configuredObject)
    {
        // If we are running as SYSTEM then no ACL checking
        if(isSystemProcess() || _managementMode)
        {
            return;
        }



        if(!checkAllPlugins(operation, configuredObject))
        {
            Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
            String objectName = (String)configuredObject.getAttribute(ConfiguredObject.NAME);
            StringBuilder exceptionMessage = new StringBuilder(String.format("Permission %s is denied for : %s '%s'",
                    operation.name(), categoryClass.getSimpleName(), objectName ));
            Model model = getModel();

            Collection<Class<? extends ConfiguredObject>> parentClasses = model.getParentTypes(categoryClass);
            if (parentClasses != null)
            {
                exceptionMessage.append(" on");
                for (Class<? extends ConfiguredObject> parentClass: parentClasses)
                {
                    String objectCategory = parentClass.getSimpleName();
                    ConfiguredObject<?> parent = configuredObject.getParent(parentClass);
                    exceptionMessage.append(" ").append(objectCategory);
                    if (parent != null)
                    {
                        exceptionMessage.append(" '").append(parent.getAttribute(ConfiguredObject.NAME)).append("'");
                    }
                }
            }
            throw new AccessControlException(exceptionMessage.toString());
        }
    }

    public void authoriseExecute(final ConfiguredObject<?> object, final String methodName, final Map<String,Object> arguments)
    {
        if(!checkAllPlugins(new AccessCheck()
        {
            Result allowed(AccessControl plugin)
            {
                return plugin.authoriseMethod(object, methodName, arguments);
            }
        }))
        {
            throw new AccessControlException("Permission denied on "
                                             + object.getCategoryClass().getSimpleName()
                                             + " '" + object.getName() + "' to perform '"
                                             + methodName + "' operation");
        }
    }


    public void authoriseExecute(final SecurityToken token, final ConfiguredObject<?> object, final String methodName, final Map<String,Object> arguments)
    {
        if(!checkAllPlugins(new AccessCheck()
        {
            Result allowed(AccessControl plugin)
            {
                return plugin.authoriseMethod(token, object, methodName, arguments);
            }
        }))
        {
            throw new AccessControlException("Permission denied on "
                                             + object.getCategoryClass().getSimpleName()
                                             + " '" + object.getName() + "' to perform '"
                                             + methodName + "' operation");
        }
    }

    private Model getModel()
    {
        return _aclProvidersParent.getModel();
    }


    private boolean checkAllPlugins(final Operation operation, final ConfiguredObject<?> configuredObject)
    {
        return checkAllPlugins(new AccessCheck()
        {
            Result allowed(AccessControl plugin)
            {
                return plugin.authorise(operation, configuredObject);
            }
        });
    }


}
