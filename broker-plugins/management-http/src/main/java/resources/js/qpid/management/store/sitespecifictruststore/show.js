/*
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
 */

define(["dojo/_base/lang", "dojo/query", "dijit/registry", "dojo/_base/connect", "dojo/_base/event", "qpid/common/util", "dojox/grid/DataGrid", "qpid/common/UpdatableStore", "dojo/domReady!"],
    function (lang, query, registry, connect, event, util, DataGrid, UpdatableStore)
    {

        function SiteSpecificTrustStore(data)
        {
            var that = this;
            this.fields = [];
            this.management = data.parent.management;
            this.modelObj = data.parent.modelObj;
            this.dateTimeFormatter = function (value)
            {
                return value ? that.management.userPreferences.formatDateTime(value, {
                    addOffset: true,
                    appendTimeZone: true
                }) : "";
            };
            var attributes = this.management.metadata.getMetaData("TrustStore", "SiteSpecificTrustStore").attributes;
            for (var name in attributes)
            {
                this.fields.push(name);
            }
            util.buildUI(data.containerNode, data.parent, "store/sitespecifictruststore/show.html", this.fields, this,
                function ()
                {
                    var refreshCertificateButton = query(".refreshCertificateButton", data.containerNode)[0];
                    var refreshCertificateWidget = registry.byNode(refreshCertificateButton);
                    connect.connect(refreshCertificateWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.refreshCertificate();
                    });
                });
        }

        SiteSpecificTrustStore.prototype.update = function (data)
        {
            util.updateUI(data, this.fields, this, {datetime: this.dateTimeFormatter});
        };

        SiteSpecificTrustStore.prototype.refreshCertificate = function ()
        {
            var modelObj = this.modelObj;
            this.management.update({
                parent: modelObj,
                name: "refreshCertificate",
                type: modelObj.type
            }, {}).then(lang.hitch(this, this.update));
        };

        return SiteSpecificTrustStore;
    });
