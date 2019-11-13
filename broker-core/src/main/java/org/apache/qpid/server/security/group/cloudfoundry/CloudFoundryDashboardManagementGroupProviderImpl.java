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
package org.apache.qpid.server.security.group.cloudfoundry;

import static org.apache.qpid.server.configuration.CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST;
import static org.apache.qpid.server.configuration.CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST;
import static org.apache.qpid.server.configuration.CommonProperties.QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST;
import static org.apache.qpid.server.configuration.CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST;
import static org.apache.qpid.server.util.ParameterizedTypes.LIST_OF_STRINGS;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2UserPrincipal;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.util.ConnectionBuilder;
import org.apache.qpid.server.util.ExternalServiceException;
import org.apache.qpid.server.util.ExternalServiceTimeoutException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/*
 * This GroupProvider checks a CloudFoundry service dashboard to see whether a certain user (represented by an
 * accessToken) has permission to manage a set of service instances and adds corresponding GroupPrincipals.
 * See the CloudFoundry docs for more information:
 * http://docs.cloudfoundry.org/services/dashboard-sso.html#checking-user-permissions
 */
public class CloudFoundryDashboardManagementGroupProviderImpl extends AbstractConfiguredObject<CloudFoundryDashboardManagementGroupProviderImpl>
        implements CloudFoundryDashboardManagementGroupProvider<CloudFoundryDashboardManagementGroupProviderImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudFoundryDashboardManagementGroupProviderImpl.class);
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @ManagedAttributeField
    private URI _cloudFoundryEndpointURI;

    @ManagedAttributeField
    private TrustStore _trustStore;

    @ManagedAttributeField
    private Map<String, String> _serviceToManagementGroupMapping;

    private List<String> _tlsProtocolWhiteList;
    private List<String> _tlsProtocolBlackList;
    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;
    private int _connectTimeout;
    private int _readTimeout;

    @ManagedObjectFactoryConstructor
    public CloudFoundryDashboardManagementGroupProviderImpl(Map<String, Object> attributes, Container<?> container)
    {
        super(container, attributes);
    }

    @Override
    public void onOpen()
    {
        super.onOpen();
        _tlsProtocolWhiteList = getContextValue(List.class, LIST_OF_STRINGS, QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST);
        _tlsProtocolBlackList = getContextValue(List.class, LIST_OF_STRINGS, QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST);
        _tlsCipherSuiteWhiteList = getContextValue(List.class, LIST_OF_STRINGS, QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST);
        _tlsCipherSuiteBlackList = getContextValue(List.class, LIST_OF_STRINGS, QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST);
        _connectTimeout = getContextValue(Integer.class, QPID_GROUPPROVIDER_CLOUDFOUNDRY_CONNECT_TIMEOUT);
        _readTimeout = getContextValue(Integer.class, QPID_GROUPPROVIDER_CLOUDFOUNDRY_READ_TIMEOUT);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        final CloudFoundryDashboardManagementGroupProvider<?> validationProxy = (CloudFoundryDashboardManagementGroupProvider<?>) proxyForValidation;
        validateSecureEndpoint(validationProxy);
        validateMapping(validationProxy);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateSecureEndpoint(this);
        validateMapping(this);
    }

    private void validateSecureEndpoint(final CloudFoundryDashboardManagementGroupProvider<?> provider)
    {
        if (!"https".equals(provider.getCloudFoundryEndpointURI().getScheme()))
        {
            throw new IllegalConfigurationException(String.format("CloudFoundryDashboardManagementEndpoint is not secure: '%s'",
                                                                  provider.getCloudFoundryEndpointURI()));
        }
    }

    private void validateMapping(final CloudFoundryDashboardManagementGroupProvider<?> provider)
    {
        for(Map.Entry<String, String> entry : provider.getServiceToManagementGroupMapping().entrySet())
        {
            if ("".equals(entry.getKey()))
            {
                throw new IllegalConfigurationException("Service instance id may not be empty");
            }
            if ("".equals(entry.getValue()))
            {
                throw new IllegalConfigurationException("Group name for service id '"
                                                        + entry.getKey() + "' may not be empty");
            }
        }
    }

    @Override
    public Set<Principal> getGroupPrincipalsForUser(Principal userPrincipal)
    {
        if (!(userPrincipal instanceof OAuth2UserPrincipal))
        {
            return Collections.emptySet();
        }
        if (_serviceToManagementGroupMapping == null)
        {
            throw new IllegalConfigurationException("CloudFoundryDashboardManagementGroupProvider serviceToManagementGroupMapping may not be null");
        }

        OAuth2UserPrincipal oauth2UserPrincipal = (OAuth2UserPrincipal) userPrincipal;
        String accessToken = oauth2UserPrincipal.getAccessToken();
        Set<Principal> groupPrincipals = new HashSet<>();

        for (Map.Entry<String, String> entry : _serviceToManagementGroupMapping.entrySet())
        {
            String serviceInstanceId = entry.getKey();
            String managementGroupName = entry.getValue();
            if (mayManageServiceInstance(serviceInstanceId, accessToken))
            {
                LOGGER.debug("Adding group '{}' to the set of Principals", managementGroupName);
                groupPrincipals.add(new GroupPrincipal(managementGroupName, this));
            }
            else
            {
                LOGGER.debug("CloudFoundryDashboardManagementEndpoint denied management permission for service instance '{}'", serviceInstanceId);
            }
        }
        return groupPrincipals;
    }

    private boolean mayManageServiceInstance(final String serviceInstanceId, final String accessToken)
    {
        HttpURLConnection connection;
        String cloudFoundryEndpoint = String.format("%s/v2/service_instances/%s/permissions",
                                                    getCloudFoundryEndpointURI().toString(), serviceInstanceId);
        try
        {
            ConnectionBuilder connectionBuilder = new ConnectionBuilder(new URL(cloudFoundryEndpoint));
            connectionBuilder.setConnectTimeout(_connectTimeout).setReadTimeout(_readTimeout);
            if (_trustStore != null)
            {
                try
                {
                    connectionBuilder.setTrustMangers(_trustStore.getTrustManagers());
                }
                catch (GeneralSecurityException e)
                {
                    throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
                }
            }
            connectionBuilder.setTlsProtocolWhiteList(_tlsProtocolWhiteList)
                             .setTlsProtocolBlackList(_tlsProtocolBlackList)
                             .setTlsCipherSuiteWhiteList(_tlsCipherSuiteWhiteList)
                             .setTlsCipherSuiteBlackList(_tlsCipherSuiteBlackList);

            LOGGER.debug("About to call CloudFoundryDashboardManagementEndpoint '{}'", cloudFoundryEndpoint);
            connection = connectionBuilder.build();

            connection.setRequestProperty("Accept-Charset", UTF8);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + accessToken);

            connection.connect();
        }
        catch (SocketTimeoutException e)
        {
            throw new ExternalServiceTimeoutException(String.format("Timed out trying to connect to CloudFoundryDashboardManagementEndpoint '%s'.",
                                                             cloudFoundryEndpoint), e);
        }
        catch (IOException e)
        {
            throw new ExternalServiceException(String.format("Could not connect to CloudFoundryDashboardManagementEndpoint '%s'.",
                                                             cloudFoundryEndpoint), e);
        }

        try (InputStream input = connection.getInputStream())
        {
            final int responseCode = connection.getResponseCode();
            LOGGER.debug("Call to CloudFoundryDashboardManagementEndpoint '{}' complete, response code : {}", cloudFoundryEndpoint, responseCode);

            Map<String, Object> responseMap = _objectMapper.readValue(input, Map.class);
            Object mayManageObject = responseMap.get("manage");
            if (mayManageObject == null || !(mayManageObject instanceof Boolean))
            {
                throw new ExternalServiceException("CloudFoundryDashboardManagementEndpoint response did not contain \"manage\" entry.");
            }
            return (boolean) mayManageObject;
        }
        catch (JsonProcessingException e)
        {
            throw new ExternalServiceException(String.format("CloudFoundryDashboardManagementEndpoint '%s' did not return json.",
                                                                     cloudFoundryEndpoint), e);
        }
        catch (SocketTimeoutException e)
        {
            throw new ExternalServiceTimeoutException(String.format("Timed out reading from CloudFoundryDashboardManagementEndpoint '%s'.",
                                    cloudFoundryEndpoint), e);
        }
        catch (IOException e)
        {
            throw new ExternalServiceException(String.format("Connection to CloudFoundryDashboardManagementEndpoint '%s' failed.",
                                                                     cloudFoundryEndpoint), e);
        }
    }

    @StateTransition( currentState = { State.UNINITIALIZED, State.QUIESCED, State.ERRORED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    public URI getCloudFoundryEndpointURI()
    {
        return _cloudFoundryEndpointURI;
    }

    @Override
    public TrustStore getTrustStore()
    {
        return _trustStore;
    }

    @Override
    public Map<String, String> getServiceToManagementGroupMapping()
    {
        return _serviceToManagementGroupMapping;
    }

    @Override
    public List<String> getTlsProtocolWhiteList()
    {
        return _tlsProtocolWhiteList;
    }

    @Override
    public List<String> getTlsProtocolBlackList()
    {
        return _tlsProtocolBlackList;
    }

    @Override
    public List<String> getTlsCipherSuiteWhiteList()
    {
        return _tlsCipherSuiteWhiteList;
    }

    @Override
    public List<String> getTlsCipherSuiteBlackList()
    {
        return _tlsCipherSuiteBlackList;
    }

}
