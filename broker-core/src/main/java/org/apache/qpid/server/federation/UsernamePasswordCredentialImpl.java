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
package org.apache.qpid.server.federation;

import java.util.List;
import java.util.Map;

import javax.security.sasl.SaslClient;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.federation.sasl.ScramSHA1SaslClient;
import org.apache.qpid.server.federation.sasl.ScramSHA256SaslClient;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.RemoteHost;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.UsernamePasswordCredential;

class UsernamePasswordCredentialImpl extends AbstractConfiguredObject<UsernamePasswordCredentialImpl>
        implements UsernamePasswordCredential<UsernamePasswordCredentialImpl>
{
    @ManagedAttributeField
    private String _username;
    @ManagedAttributeField
    private String _password;

    @ManagedObjectFactoryConstructor
    UsernamePasswordCredentialImpl(Map<String, Object> attributes, RemoteHost<?> host)
    {
        super(parentsMap(host), attributes);
    }

        @Override
    public SaslClient getSaslClient(final List<String> mechanisms)
    {
        if(mechanisms.contains(ScramSHA256SaslClient.MECHANISM))
        {
            return new ScramSHA256SaslClient(this);
        }
        else if(mechanisms.contains(ScramSHA1SaslClient.MECHANISM))
        {
            return new ScramSHA1SaslClient(this);
        }
        return null;
    }

    @Override
    public String getUsername()
    {
        return _username;
    }

    @Override
    public String getPassword()
    {
        return _password;
    }

    @StateTransition( currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

}
