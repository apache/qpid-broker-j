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
package org.apache.qpid.server.security;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.util.Collections;

import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.test.utils.QpidTestCase;

public class SecurityManagerTest extends QpidTestCase
{
    private static final String TEST_VIRTUAL_HOST = "testVirtualHost";

    private AccessControl _accessControl;
    private SecurityManager _securityManager;
    private VirtualHost<?> _virtualHost;
    private Broker _broker;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _accessControl = mock(AccessControl.class);
        _virtualHost = mock(VirtualHost.class);

        AccessControlProvider<?> aclProvider = mock(AccessControlProvider.class);
        when(aclProvider.getAccessControl()).thenReturn(_accessControl);
        when(aclProvider.getState()).thenReturn(State.ACTIVE);

        when(_virtualHost.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(VirtualHost.class).when(_virtualHost).getCategoryClass();

        _broker = mock(Broker.class);
        when(_broker.getAccessControlProviders()).thenReturn(Collections.singleton(aclProvider));
        when(_broker.getChildren(AccessControlProvider.class)).thenReturn(Collections.singleton(aclProvider));
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getName()).thenReturn("My Broker");
        when(_broker.getAttribute(Broker.NAME)).thenReturn("My Broker");
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());

        _securityManager = new SecurityManager(_broker, false);
    }






    public void testDenyWhenAccessControlProviderIsErrored()
    {
        AccessControlProvider erroredACLProvider = mock(AccessControlProvider.class);
        when(erroredACLProvider.getState()).thenReturn(State.ERRORED);
        when(erroredACLProvider.getAccessControl()).thenReturn(_accessControl);
        when(_broker.getAccessControlProviders()).thenReturn(Collections.singleton(erroredACLProvider));
        when(_broker.getChildren(AccessControlProvider.class)).thenReturn(Collections.singleton(erroredACLProvider));

        assertAuthorisationDenied();
    }

    public void testDenyWhenAccessControlProviderHasNoAccessControl()
    {
        AccessControlProvider erroredACLProvider = mock(AccessControlProvider.class);
        when(erroredACLProvider.getState()).thenReturn(State.ACTIVE);
        when(erroredACLProvider.getAccessControl()).thenReturn(null);
        when(_broker.getAccessControlProviders()).thenReturn(Collections.singleton(erroredACLProvider));
        when(_broker.getChildren(AccessControlProvider.class)).thenReturn(Collections.singleton(erroredACLProvider));

        assertAuthorisationDenied();
    }

    private void assertAuthorisationDenied()
    {
        ConfiguredObject mockConfiguredObject = mock(BrokerLogger.class);
        when(mockConfiguredObject.getCategoryClass()).thenReturn(BrokerLogger.class);

        try
        {
            _securityManager.authorise(Operation.UPDATE, mockConfiguredObject);
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            _securityManager.authoriseCreate(mockConfiguredObject);
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            _securityManager.authoriseDelete(mockConfiguredObject);
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            _securityManager.authoriseExecute(mockConfiguredObject, "getAllFiles", Collections.<String,Object>emptyMap());
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            _securityManager.authoriseExecute(mockConfiguredObject, "getPreferences", Collections.<String,Object>singletonMap("userId", "guest"));
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            Queue mockQueue = mock(Queue.class);
            when(mockQueue.getParent(VirtualHost.class)).thenReturn(_virtualHost);
            when(mockQueue.getCategoryClass()).thenReturn(Queue.class);
            _securityManager.authoriseExecute(mockQueue, "clearQueue", Collections.<String,Object>emptyMap());
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            _securityManager.authoriseExecute(_broker, "manage", Collections.<String,Object>emptyMap());
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

        try
        {
            Queue mockQueue = mock(Queue.class);
            when(mockQueue.getParent(VirtualHost.class)).thenReturn(_virtualHost);
            when(mockQueue.getCategoryClass()).thenReturn(Queue.class);
            _securityManager.authoriseExecute(mockQueue, "deleteMessages", Collections.<String,Object>emptyMap());
            fail("AccessControlException is expected");
        }
        catch(AccessControlException e)
        {
            // pass
        }

    }


}
