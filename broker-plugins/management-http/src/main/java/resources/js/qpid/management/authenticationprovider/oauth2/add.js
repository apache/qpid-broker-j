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
define(["dojo/dom",
        "dojo/query",
        "dojo/_base/array",
        "dijit/registry",
        "qpid/common/util",
        "dojo/parser",
        "dojo/text!authenticationprovider/oauth2/add.html",
        "dojo/domReady!"],
    function (dom, query, array, registry, util, parser, template)
    {
        var addAuthenticationProvier =
        {
            show: function(data)
            {
                var that = this;
                this.metadata = data.metadata;
                this.containerNode = data.containerNode;
                data.containerNode.innerHTML = template;
                return parser.parse(this.containerNode).then(function(instances)
                {
                    var identityResolverType = registry.byId("addAuthenticationProvider.identityResolverType");
                    var validValues = that.metadata.getMetaData(data.category, data.type).attributes.identityResolverType.validValues;
                    var validValueStore = util.makeTypeStore(validValues);
                    identityResolverType.set("store", validValueStore);

                    if (data.data)
                    {
                        var authorizationEndpointURI = registry.byId("addAuthenticationProvider.authorizationEndpointURI");
                        authorizationEndpointURI.set("value", data.data.authorizationEndpointURI);
                        var tokenEndpointURI = registry.byId("addAuthenticationProvider.tokenEndpointURI");
                        tokenEndpointURI.set("value", data.data.tokenEndpointURI);
                        var tokenEndpointNeedsAuth = registry.byId("addAuthenticationProvider.tokenEndpointNeedsAuth");
                        tokenEndpointNeedsAuth.set("value", data.data.tokenEndpointNeedsAuth);
                        var identityResolverEndpointURI = registry.byId("addAuthenticationProvider.identityResolverEndpointURI");
                        identityResolverEndpointURI.set("value", data.data.identityResolverEndpointURI);
                        identityResolverType.set("value", data.data.identityResolverType);
                        var clientId = registry.byId("addAuthenticationProvider.clientId");
                        clientId.set("value", data.data.clientId);
                        var clientSecret = registry.byId("addAuthenticationProvider.clientSecret");
                        clientSecret.set("value", data.data.clientSecret);
                        var scope = registry.byId("addAuthenticationProvider.scope");
                        scope.set("value", data.data.scope);
                        var postLogoutURI = registry.byId("addAuthenticationProvider.postLogoutURI");
                        postLogoutURI.set("value", data.data.postLogoutURI);

                    }
                });
            }
        };

        return addAuthenticationProvier;
    }
);
