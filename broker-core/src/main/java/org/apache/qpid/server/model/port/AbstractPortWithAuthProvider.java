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
package org.apache.qpid.server.model.port;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Transport;

abstract public class AbstractPortWithAuthProvider<X extends AbstractPortWithAuthProvider<X>> extends AbstractPort<X> implements PortWithAuthProvider<X>
{
    @ManagedAttributeField
    private AuthenticationProvider _authenticationProvider;

    public AbstractPortWithAuthProvider(final Map<String, Object> attributes,
                                        final Broker<?> broker)
    {
        super(attributes, broker);
    }

    public AuthenticationProvider getAuthenticationProvider()
    {
        Broker<?> broker = getParent(Broker.class);
        if(broker.isManagementMode())
        {
            return broker.getManagementModeAuthenticationProvider();
        }
        return _authenticationProvider;
    }

    @Override
    public void onValidate()
    {
        super.onValidate();

        AuthenticationProvider<?> authenticationProvider = getAuthenticationProvider();
        final Set<Transport> transports = getTransports();
        validateAuthenticationMechanisms(authenticationProvider, transports);

    }

    private void validateAuthenticationMechanisms(final AuthenticationProvider<?> authenticationProvider,
                                                  final Set<Transport> transports)
    {
        List<String> availableMechanisms = new ArrayList<>(authenticationProvider.getMechanisms());
        if(authenticationProvider.getDisabledMechanisms() != null)
        {
            availableMechanisms.removeAll(authenticationProvider.getDisabledMechanisms());
        }
        if (availableMechanisms.isEmpty())
        {
            throw new IllegalConfigurationException("The authentication provider '"
                                                    + authenticationProvider.getName()
                                                    + "' on port '"
                                                    + getName()
                                                    + "' has all authentication mechanisms disabled.");
        }
        if (hasNonTLSTransport(transports) && authenticationProvider.getSecureOnlyMechanisms() != null)
        {
            availableMechanisms.removeAll(authenticationProvider.getSecureOnlyMechanisms());
            if(availableMechanisms.isEmpty())
            {
                throw new IllegalConfigurationException("The port '"
                                                        + getName()
                                                        + "' allows for non TLS connections, but all authentication "
                                                        + "mechanisms of the authentication provider '"
                                                        + authenticationProvider.getName()
                                                        + "' are disabled on non-secure connections.");
            }
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if(changedAttributes.contains(Port.AUTHENTICATION_PROVIDER) || changedAttributes.contains(Port.TRANSPORTS))
        {
            PortWithAuthProvider<?> port = (PortWithAuthProvider<?>) proxyForValidation;
            validateAuthenticationMechanisms(port.getAuthenticationProvider(), port.getTransports());
        }
    }
}
