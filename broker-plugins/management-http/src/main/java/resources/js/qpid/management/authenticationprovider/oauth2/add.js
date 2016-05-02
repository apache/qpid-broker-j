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
        "dojo/domReady!"], function (dom, query, array, registry, util, parser, template)
{
    var addAuthenticationProvier = {
        show: function (data)
        {
            var that = this;
            util.parse(data.containerNode, template, function ()
            {
                that._postParse(data);
            });
        },
        _postParse: function (data)
        {
            var identityResolverType = registry.byId("addAuthenticationProvider.identityResolverType");
            var validValues = data.metadata.getMetaData(data.category,
                data.type).attributes.identityResolverType.validValues;
            var validValueStore = util.makeTypeStore(validValues);
            identityResolverType.set("store", validValueStore);

            util.makeInstanceStore(data.parent.management, "Broker", "TrustStore", function (trustStoresStore)
            {
                var trustStore = registry.byNode(query(".trustStore", data.containerNode)[0]);
                trustStore.set("store", trustStoresStore);
                if (data.data)
                {
                    util.initialiseFields(data.data,
                        data.containerNode,
                        data.metadata,
                        "AuthenticationProvider",
                        "OAuth2");
                }
            });
        }
    };

    return addAuthenticationProvier;
});
