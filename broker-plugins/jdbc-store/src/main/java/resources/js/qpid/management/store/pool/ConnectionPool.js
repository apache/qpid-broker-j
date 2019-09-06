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

define(["qpid/common/util",
        "dijit/registry",
        "dojo/_base/array",
        "dojo/dom-construct",
        "dojo/dom",
        "dojo/string"],
    function (util, registry, array, domConstruct, dom, string) {

        function ConnectionPool(container, management, modelObj)
        {
            this.containerNode = container;
            this.previousConnectionPoolType = null;
            this.management = management;
            this.modelObj = modelObj;
            this.poolDetails = null;
        }

        ConnectionPool.prototype.update = function (data) {
            if (data && data.hasOwnProperty("connectionPoolType")
                && (!this.poolDetails || this.previousConnectionPoolType !== data.connectionPoolType))
            {
                var that = this;
                require(["qpid/management/store/pool/" + data.connectionPoolType.toLowerCase() + "/show"],
                    function (PoolDetails) {
                        that.poolDetails = that._createPoolDetails(PoolDetails);
                        that.previousConnectionPoolType = data.connectionPoolType;
                        that.poolDetails.update(data);
                    });
            }
            else
            {
                this.poolDetails.update(data);
            }
        };

        ConnectionPool.prototype._createPoolDetails = function (PoolDetails) {
            var widgets = registry.findWidgets(this.containerNode);
            array.forEach(widgets, function (item) {
                item.destroyRecursive();
            });
            domConstruct.empty(this.containerNode);

            return new PoolDetails({
                containerNode: this.containerNode,
                management: this.management,
                modelObj: this.modelObj
            });
        };

        ConnectionPool.initPoolFieldsInDialog = function (dialogIdPrefix, data) {
            registry.byId(dialogIdPrefix + ".connectionUrl")
                .set("regExpGen", util.jdbcUrlOrContextVarRegexp);
            registry.byId(dialogIdPrefix + ".username")
                .set("regExpGen", util.nameOrContextVarRegexp);

            var passwordControl = registry.byId(dialogIdPrefix + ".password");
            passwordControl.set("required", !data.data);

            var poolTypeControl = registry.byId(dialogIdPrefix + ".connectionPoolType");

            var typeMetaData = data.metadata.getMetaData(data.category, data.type);
            var values = ["NONE"];
            if (typeMetaData.attributes.hasOwnProperty("connectionPoolType")
                && typeMetaData.attributes.connectionPoolType.hasOwnProperty("validValues"))
            {
                values = typeMetaData.attributes.connectionPoolType.validValues;
            }
            var store = util.makeTypeStore(values);
            poolTypeControl.set("store", store);
            poolTypeControl.set("value", "NONE");

            var poolTypeFieldsDiv = dom.byId(dialogIdPrefix + ".poolSpecificDiv");
            poolTypeControl.on("change", function (type) {
                if (type && string.trim(type) !== "")
                {
                    var widgets = registry.findWidgets(poolTypeFieldsDiv);
                    array.forEach(widgets, function (item) {
                        item.destroyRecursive();
                    });
                    widgets.forEach(function (item) {
                        item.destroyRecursive();
                    });
                    domConstruct.empty(poolTypeFieldsDiv);
                    require(["qpid/management/store/pool/" + type.toLowerCase() + "/add"], function (poolType) {
                        poolType.show({
                            containerNode: poolTypeFieldsDiv,
                            context: data.context
                        });
                    });
                }
            });
        };
        return ConnectionPool;
    });
