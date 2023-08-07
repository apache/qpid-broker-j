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
 *
 *
 */
package org.apache.qpid.server.security.auth.manager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.login.AccountNotFoundException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ExternalFileBasedAuthenticationManager;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.database.PrincipalDatabase;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.util.FileHelper;

public abstract class PrincipalDatabaseAuthenticationManager<T extends PrincipalDatabaseAuthenticationManager<T>>
        extends AbstractAuthenticationManager<T>
        implements ExternalFileBasedAuthenticationManager<T>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(PrincipalDatabaseAuthenticationManager.class);


    private final Map<Principal, PrincipalAdapter> _userMap = new ConcurrentHashMap<>();

    private PrincipalDatabase _principalDatabase;
    @ManagedAttributeField
    private String _path;

    protected PrincipalDatabaseAuthenticationManager(final Map<String, Object> attributes, final Container<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        File passwordFile = new File(_path);
        if (passwordFile.exists() && !passwordFile.canRead())
        {
            throw new IllegalConfigurationException(String.format("Cannot read password file '%s'. Please check permissions.", _path));
        }
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
        File passwordFile = new File(_path);
        if (!passwordFile.exists())
        {
            try
            {
                Path path = new FileHelper().createNewFile(passwordFile, getContextValue(String.class, SystemConfig.POSIX_FILE_PERMISSIONS));
                if (!Files.exists(path))
                {
                    throw new IllegalConfigurationException(String.format("Cannot create password file at '%s'", _path));
                }
            }
            catch (IOException e)
            {
                throw new IllegalConfigurationException(String.format("Cannot create password file at '%s'", _path), e);
            }
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        initialise();
    }

    @Override
    protected void postResolve()
    {
        super.postResolve();
        _principalDatabase = createDatabase();
    }

    protected abstract PrincipalDatabase createDatabase();


    @Override
    public String getPath()
    {
        return _path;
    }

    public void initialise()
    {
        try
        {
            _principalDatabase.open(new File(_path));
        }
        catch (FileNotFoundException e)
        {
            throw new IllegalConfigurationException("Exception opening password database: " + e.getMessage(), e);
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Cannot use password database at :" + _path, e);
        }
    }

    @Override
    public List<String> getMechanisms()
    {
        return _principalDatabase.getMechanisms();
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        return _principalDatabase.createSaslNegotiator(mechanism, saslSettings);
    }

    /**
     * @see org.apache.qpid.server.security.auth.manager.UsernamePasswordAuthenticationProvider#authenticate(String, String)
     */
    @Override
    public AuthenticationResult authenticate(final String username, final String password)
    {
        try
        {
            if (_principalDatabase.verifyPassword(username, password.toCharArray()))
            {
                return new AuthenticationResult(new UsernamePrincipal(username, this));
            }
            else
            {
                return new AuthenticationResult(AuthenticationStatus.ERROR);
            }
        }
        catch (AccountNotFoundException e)
        {
            return new AuthenticationResult(AuthenticationStatus.ERROR);
        }
    }

    public PrincipalDatabase getPrincipalDatabase()
    {
        return _principalDatabase;
    }

    @Override
    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    public ListenableFuture<Void> activate()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        final List<Principal> users = _principalDatabase == null ? List.of() : _principalDatabase.getUsers();
        _userMap.clear();
        if(!users.isEmpty())
        {
            for (final Principal user : users)
            {
                final PrincipalAdapter principalAdapter = new PrincipalAdapter(user);
                principalAdapter.registerWithParents();
                principalAdapter.openAsync().addListener(() ->
                {
                    _userMap.put(user, principalAdapter);
                    if (_userMap.size() == users.size())
                    {
                        setState(State.ACTIVE);
                        returnVal.set(null);
                    }
                }, getTaskExecutor());

            }

            return returnVal;
        }
        else
        {
            setState(State.ACTIVE);
            return Futures.immediateFuture(null);
        }
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        // We manage the storage children so we close (so they may free any resources) them rather than deleting them
        return doAfterAlways(closeChildren(),
                             () -> {
                                 File file = new File(_path);
                                 if (file.exists() && file.isFile())
                                 {
                                     file.delete();
                                 }
                             });
    }

    @Override
    public boolean createUser(String username, String password, Map<String, String> attributes)
    {
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(User.NAME, username);
        userAttrs.put(User.PASSWORD, password);

        User user = createChild(User.class, userAttrs);
        return user != null;

    }


    private void deleteUserFromDatabase(String username) throws AccountNotFoundException
    {
        UsernamePrincipal principal = new UsernamePrincipal(username, this);
        getPrincipalDatabase().deletePrincipal(principal);
        _userMap.remove(principal);
    }

    @Override
    public void deleteUser(String username) throws AccountNotFoundException
    {
        UsernamePrincipal principal = new UsernamePrincipal(username, this);
        PrincipalAdapter user = _userMap.get(principal);
        if(user != null)
        {
            user.delete();
        }
        else
        {
            throw new AccountNotFoundException("No such user: '" + username + "'");
        }
    }

    @Override
    public void setPassword(String username, String password) throws AccountNotFoundException
    {
        Principal principal = new UsernamePrincipal(username, this);
        User user = _userMap.get(principal);
        if (user != null)
        {
            user.setPassword(password);
        }
    }

    @Override
    public Map<String, Map<String, String>> getUsers()
    {
        Map<String, Map<String,String>> users = new HashMap<>();
        for(Principal principal : getPrincipalDatabase().getUsers())
        {
            users.put(principal.getName(), Map.of());
        }
        return users;
    }

    @Override
    public void reload() throws IOException
    {
        getPrincipalDatabase().reload();
    }

    @Override
    protected  <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                          Map<String, Object> attributes)
    {
        if(childClass == User.class)
        {
            String username = (String) attributes.get("name");
            String password = (String) attributes.get("password");
            Principal p = new UsernamePrincipal(username, this);
            PrincipalAdapter principalAdapter = new PrincipalAdapter(p);
            principalAdapter.create(); // for a duplicate user DuplicateNameException should be thrown
            try
            {
                boolean created = getPrincipalDatabase().createPrincipal(p, password.toCharArray());
                if (!created)
                {
                    throw new IllegalArgumentException("User '" + username + "' was not added into principal database");
                }
            }
            catch (RuntimeException e)
            {
                principalAdapter.deleteNoChecks();
                throw e;
            }
            _userMap.put(p, principalAdapter);
            return Futures.immediateFuture((C)principalAdapter);
        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }


    @Override
    protected void validateChange(final ConfiguredObject<?> updatedObject, final Set<String> changedAttributes)
    {
        super.validateChange(updatedObject, changedAttributes);

        ExternalFileBasedAuthenticationManager<?> updated = (ExternalFileBasedAuthenticationManager<?>) updatedObject;
        if (changedAttributes.contains(NAME) &&  !getName().equals(updated.getName()))
        {
            throw new IllegalConfigurationException("Changing the name of authentication provider is not supported");
        }
        if (changedAttributes.contains(TYPE) && !getType().equals(updated.getType()))
        {
            throw new IllegalConfigurationException("Changing the type of authentication provider is not supported");
        }
    }

    @Override
    protected void changeAttributes(Map<String, Object> attributes)
    {
        super.changeAttributes(attributes);
        if(getState() != State.DELETED && getDesiredState() != State.DELETED)
        {
            // TODO - this does not belong here!
            try
            {
                initialise();
                // if provider was previously in ERRORED state then set its state to ACTIVE
                setState(State.ACTIVE);
            }
            catch(RuntimeException e)
            {
                if(getState() != State.ERRORED)
                {
                    throw e;
                }
            }
        }


    }

    private class PrincipalAdapter extends AbstractConfiguredObject<PrincipalAdapter> implements User<PrincipalAdapter>
    {
        private final Principal _user;

        @ManagedAttributeField
        private String _password;

        public PrincipalAdapter(Principal user)
        {
            super(PrincipalDatabaseAuthenticationManager.this, createPrincipalAttributes(PrincipalDatabaseAuthenticationManager.this, user));
            _user = user;

        }

        @Override
        public void onValidate()
        {
            super.onValidate();
            if(!isDurable())
            {
                throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
            }
        }

        @Override
        public String getPassword()
        {
            return _password;
        }

        @Override
        public void setPassword(String password)
        {
            setAttributes(Map.of(PASSWORD, password));
        }

        @Override
        protected void changeAttributes(final Map<String, Object> attributes)
        {
            if(attributes.containsKey(PASSWORD))
            {
                try
                {
                    String desiredPassword = (String) attributes.get(PASSWORD);
                    boolean changed = getPrincipalDatabase().updatePassword(_user, desiredPassword.toCharArray());
                    if (!changed)
                    {
                        throw new IllegalStateException(String.format("Failed to user password for user : '%s'", getName()));
                    }
                }
                catch(AccountNotFoundException e)
                {
                    throw new IllegalStateException(e);
                }
            }
            super.changeAttributes(attributes);
        }

        @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
        private ListenableFuture<Void> activate()
        {
            setState(State.ACTIVE);
            return Futures.immediateFuture(null);
        }

        @Override
        protected ListenableFuture<Void> onDelete()
        {
            try
            {
                String userName = _user.getName();
                deleteUserFromDatabase(userName);
            }
            catch (AccountNotFoundException e)
            {
                // pass
            }
            return super.onDelete();
        }

        @Override
        protected ListenableFuture<Void> deleteNoChecks()
        {
            return super.deleteNoChecks();
        }
    }

    private static Map<String, Object> createPrincipalAttributes(PrincipalDatabaseAuthenticationManager manager, final Principal user)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ID, UUID.randomUUID());
        attributes.put(NAME, user.getName());
        return attributes;
    }

}
