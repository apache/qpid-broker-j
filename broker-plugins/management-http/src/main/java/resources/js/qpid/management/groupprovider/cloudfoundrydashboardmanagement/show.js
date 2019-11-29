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
define(["qpid/common/util", "dojo/query", "dojo/_base/lang", "dojox/html/entities", "dojo/domReady!"],
    function (util, query, lang, entities)
    {
        var fieldNames = ["cloudFoundryEndpointURI", "trustStore", "serviceToManagementGroupMapping"];

        function GroupProvider(params)
        {
            this.containerNode = params.containerNode;
            util.buildUI(params.containerNode,
                params.parent,
                "groupprovider/cloudfoundrydashboardmanagement/show.html",
                fieldNames,
                this);
        }

        GroupProvider.prototype.update = function (restData)
        {
            var data = restData || {};
            if (data.serviceToManagementGroupMapping)
            {
                var tableContent = "";
                for (var serviceInstanceId in data.serviceToManagementGroupMapping)
                {
                    tableContent += "<tr><td>" + entities.encode(serviceInstanceId) + "</td><td>: " + entities.encode(
                        data.serviceToManagementGroupMapping[serviceInstanceId]) + "</td></tr>\n";
                }
                var table = query(".serviceToManagementGroupMappingTable", this.containerNode)[0];
                if (typeof table != "undefined")
                {
                    table.innerHTML = tableContent;
                }
            }
            util.updateUI(data, fieldNames, this);
        };

        return GroupProvider;
    });
