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
package org.apache.qpid.server.security.auth.manager;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.login.AccountNotFoundException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;

public abstract class ConfigModelPasswordManagingAuthenticationProvider<X extends ConfigModelPasswordManagingAuthenticationProvider<X>>
        extends AbstractAuthenticationManager<X>
        implements PasswordCredentialManagingAuthenticationProvider<X>
{
    static final Charset ASCII = Charset.forName("ASCII");
    protected Map<String, ManagedUser> _users = new ConcurrentHashMap<>();

    protected ConfigModelPasswordManagingAuthenticationProvider(final Map<String, Object> attributes,
                                                                final Container<?> container)
    {
        super(attributes, container);
    }

    public ManagedUser getUser(final String username)
    {
        return _users.get(username);
    }

    protected PasswordSource getPasswordSource()
    {
        return new PasswordSource()
        {
            @Override
            public char[] getPassword(final String username)
            {
                ManagedUser user = getUser(username);
                if (user == null)
                {
                    return null;
                }
                return user.getPassword().toCharArray();
            }
        };
    }


    @Override
    public boolean createUser(final String username, final String password, final Map<String, String> attributes)
    {
        return runTask(new Task<Boolean, RuntimeException>()
        {
            @Override
            public Boolean execute()
            {

                Map<String, Object> userAttrs = new HashMap<>();
                userAttrs.put(User.ID, UUID.randomUUID());
                userAttrs.put(User.NAME, username);
                userAttrs.put(User.PASSWORD, password);
                userAttrs.put(User.TYPE, ManagedUser.MANAGED_USER_TYPE);
                User user = createChild(User.class, userAttrs);
                return user != null;

            }

            @Override
            public String getObject()
            {
                return ConfigModelPasswordManagingAuthenticationProvider.this.toString();
            }

            @Override
            public String getAction()
            {
                return "create user";
            }

            @Override
            public String getArguments()
            {
                return username;
            }
        });
    }

    @Override
    public void deleteUser(final String user) throws AccountNotFoundException
    {
        final ManagedUser authUser = getUser(user);
        if(authUser != null)
        {
            authUser.delete();
        }
        else
        {
            throw new AccountNotFoundException("No such user: '" + user + "'");
        }
    }

    @Override
    public Map<String, Map<String, String>> getUsers()
    {
        return runTask(new Task<Map<String, Map<String, String>>, RuntimeException>()
        {
            @Override
            public Map<String, Map<String, String>> execute()
            {

                Map<String, Map<String, String>> users = new HashMap<>();
                for (String user : _users.keySet())
                {
                    users.put(user, Collections.<String, String>emptyMap());
                }
                return users;
            }

            @Override
            public String getObject()
            {
                return ConfigModelPasswordManagingAuthenticationProvider.this.toString();
            }

            @Override
            public String getAction()
            {
                return "get users";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        });
    }

    @Override
    public void reload() throws IOException
    {

    }

    @Override
    public void setPassword(final String username, final String password) throws AccountNotFoundException
    {
        runTask(new Task<Object, AccountNotFoundException>()
        {
            @Override
            public Void execute() throws AccountNotFoundException
            {

                final ManagedUser authUser = getUser(username);
                if (authUser != null)
                {
                    authUser.setPassword(password);
                    return null;
                }
                else
                {
                    throw new AccountNotFoundException("No such user: '" + username + "'");
                }
            }

            @Override
            public String getObject()
            {
                return ConfigModelPasswordManagingAuthenticationProvider.this.toString();
            }

            @Override
            public String getAction()
            {
                return "set password";
            }

            @Override
            public String getArguments()
            {
                return username;
            }
        });

    }

    protected abstract String createStoredPassword(String password);

    Map<String, ManagedUser> getUserMap()
    {
        return _users;
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(final Class<C> childClass,
                                                                          final Map<String, Object> attributes)
    {
        if(childClass == User.class)
        {
            String username = (String) attributes.get(User.NAME);
            if (_users.containsKey(username))
            {
                throw new IllegalArgumentException("User '" + username + "' already exists");
            }
            attributes.put(User.PASSWORD, createStoredPassword((String) attributes.get(User.PASSWORD)));
            ManagedUser user = new ManagedUser(attributes, ConfigModelPasswordManagingAuthenticationProvider.this);
            user.create();
            return Futures.immediateFuture((C)getUser(username));
        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }

    abstract void validateUser(final ManagedUser managedUser);

    @SuppressWarnings("unused")
    public static Map<String, Collection<String>> getSupportedUserTypes()
    {
        return Collections.<String, Collection<String>>singletonMap(User.class.getSimpleName(), Collections.singleton(ManagedUser.MANAGED_USER_TYPE));
    }
}
