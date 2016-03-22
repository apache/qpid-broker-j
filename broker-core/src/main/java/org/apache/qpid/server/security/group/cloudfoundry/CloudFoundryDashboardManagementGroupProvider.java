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

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject( category = false, type = "CloudFoundryDashboardManagement" )
public interface CloudFoundryDashboardManagementGroupProvider<X extends CloudFoundryDashboardManagementGroupProvider<X>> extends GroupProvider<X>
{
    String QPID_GROUPPROVIDER_CLOUDFOUNDRY_CONNECT_TIMEOUT = "qpid.groupprovider.cloudfoundry.connectTimeout";
    @ManagedContextDefault(name = QPID_GROUPPROVIDER_CLOUDFOUNDRY_CONNECT_TIMEOUT)
    int DEFAULT_QPID_GROUPPROVIDER_CLOUDFOUNDRY_CONNECT_TIMEOUT = 60000;

    String QPID_GROUPPROVIDER_CLOUDFOUNDRY_READ_TIMEOUT = "qpid.groupprovider.cloudfoundry.readTimeout";
    @ManagedContextDefault(name = QPID_GROUPPROVIDER_CLOUDFOUNDRY_READ_TIMEOUT)
    int DEFAULT_QPID_GROUPPROVIDER_CLOUDFOUNDRY_READ_TIMEOUT = 60000;

    @ManagedAttribute( description = "The CloudFoundry dashboard SSO base URI. The API version and service instance information will be appended by this GroupProvider.", mandatory = true )
    URI getCloudFoundryEndpointURI();

    @ManagedAttribute( description = "The TrustStore that contains the CA certificate that signed the CloudFoundry endpoint." )
    TrustStore getTrustStore();

    @ManagedAttribute( description = "A service instance id to qpid management group mapping. If the CloudFoundry endpoint grants a user permission to manage a service instance the user will be associated with the corresponding management group.", mandatory = true )
    Map<String, String> getServiceToManagementGroupMapping();

    @DerivedAttribute
    List<String> getTlsProtocolWhiteList();
    @DerivedAttribute
    List<String> getTlsProtocolBlackList();
    @DerivedAttribute
    List<String> getTlsCipherSuiteWhiteList();
    @DerivedAttribute
    List<String> getTlsCipherSuiteBlackList();
}
