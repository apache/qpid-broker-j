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
package org.apache.qpid.server.security.auth.manager;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.UnitTestBase;

public class SimpleLDAPAuthenticationManagerExceptionHandlingTest extends UnitTestBase
{
    private SimpleLDAPAuthenticationManagerImpl _authenticationProvider;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(SimpleLDAPAuthenticationManager.NAME, getTestName());
        attributes.put(SimpleLDAPAuthenticationManager.PROVIDER_URL, "ldap://localhost:389");
        attributes.put(SimpleLDAPAuthenticationManager.SEARCH_CONTEXT, "dc=qpid,dc=org");
        attributes.put(SimpleLDAPAuthenticationManager.SEARCH_FILTER, "(uid={0})");
        attributes.put(SimpleLDAPAuthenticationManager.LDAP_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

        _authenticationProvider = new SimpleLDAPAuthenticationManagerImpl(attributes, BrokerTestHelper.createBrokerMock());
        setField(_authenticationProvider, "_authenticationMethod", LdapAuthenticationMethod.SIMPLE);
        setField(_authenticationProvider, "_bindWithoutSearch", false);
    }

    @Test
    public void doLDAPNameAuthenticationRethrowsRuntimeException() throws Exception
    {
        // Force an unchecked failure in name lookup path.
        setField(_authenticationProvider, "_providerUrl", null);

        assertThrows(NullPointerException.class,
                () -> invokeDoLdapNameAuthentication("user", "password"),
                "Unchecked exception must not be converted into AuthenticationResult(ERROR)");
    }

    @Test
    public void validateInitialDirContextRethrowsRuntimeException() throws Exception
    {
        final SimpleLDAPAuthenticationManager<?> changed = mock(SimpleLDAPAuthenticationManager.class);
        when(changed.getProviderUrl()).thenReturn(null);
        when(changed.getSearchUsername()).thenReturn(null);
        when(changed.getSearchPassword()).thenReturn(null);
        when(changed.getAuthenticationMethod()).thenReturn(LdapAuthenticationMethod.SIMPLE);

        assertThrows(NullPointerException.class,
                () -> invokeValidateInitialDirContext(changed),
                "Unchecked exception must not be wrapped as IllegalConfigurationException");
    }

    private AuthenticationResult invokeDoLdapNameAuthentication(final String username, final String password) throws Exception
    {
        final Method method = SimpleLDAPAuthenticationManagerImpl.class.getDeclaredMethod(
                "doLDAPNameAuthentication", String.class, String.class);
        method.setAccessible(true);
        return (AuthenticationResult) invokeAndUnwrap(method, username, password);
    }

    private void invokeValidateInitialDirContext(final SimpleLDAPAuthenticationManager<?> provider) throws Exception
    {
        final Method method = SimpleLDAPAuthenticationManagerImpl.class.getDeclaredMethod(
                "validateInitialDirContext", SimpleLDAPAuthenticationManager.class);
        method.setAccessible(true);
        invokeAndUnwrap(method, provider);
    }

    private Object invokeAndUnwrap(final Method method, final Object... args) throws Exception
    {
        try
        {
            return method.invoke(_authenticationProvider, args);
        }
        catch (InvocationTargetException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime)
            {
                throw runtime;
            }
            if (cause instanceof Error error)
            {
                throw error;
            }
            if (cause instanceof Exception exception)
            {
                throw exception;
            }
            throw e;
        }
    }

    private static void setField(final Object target, final String fieldName, final Object value) throws Exception
    {
        final Field field = SimpleLDAPAuthenticationManagerImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
