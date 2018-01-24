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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.messages.AuthenticationProviderMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.User;

@ManagedObject( category = false, type = ManagedUser.MANAGED_USER_TYPE)
class ManagedUser extends AbstractConfiguredObject<ManagedUser> implements User<ManagedUser>
{
    public static final String MANAGED_USER_TYPE = "managed";

    private ConfigModelPasswordManagingAuthenticationProvider<?> _authenticationManager;
    @ManagedAttributeField
    private String _password;

    @ManagedObjectFactoryConstructor
    ManagedUser(final Map<String, Object> attributes, ConfigModelPasswordManagingAuthenticationProvider<?> parent)
    {
        super(parent, attributes);
        _authenticationManager = parent;

        setState(State.ACTIVE);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _authenticationManager.getUserMap().put(getName(), this);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        _authenticationManager.validateUser(this);
        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        _authenticationManager.getUserMap().remove(getName());
        return super.onDelete();
    }

    @Override
    protected void changeAttributes(Map<String, Object> attributes)
    {
        if(attributes.containsKey(PASSWORD))
        {
            String desiredPassword = (String) attributes.get(PASSWORD);
            String storedPassword = _authenticationManager.createStoredPassword(desiredPassword);
            if (!storedPassword.equals(getActualAttributes().get(User.PASSWORD)))
            {
                attributes = new HashMap<>(attributes);
                attributes.put(PASSWORD, storedPassword);
            }
        }
        super.changeAttributes(attributes);
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    @Override
    public void setPassword(final String password)
    {
        setAttributes(Collections.<String, Object>singletonMap(User.PASSWORD, password));
    }

    @Override
    protected void logOperation(final String operation)
    {
        ((Container) _authenticationManager.getParent()).getEventLogger().message(AuthenticationProviderMessages.OPERATION(operation));
    }

}
