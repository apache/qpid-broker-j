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
        "dojo/request/xhr",
        "dojo/store/Memory",
        "dojo/string"],
    function (util, registry, array, domConstruct, dom, xhr, Memory, string) {

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
                const that = this;
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
            const widgets = registry.findWidgets(this.containerNode);
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

            const passwordControl = registry.byId(dialogIdPrefix + ".password");

            const poolTypeControl = registry.byId(dialogIdPrefix + ".connectionPoolType");

            const typeMetaData = data.metadata.getMetaData(data.category, data.type);
            let values = ["NONE"];
            if (typeMetaData.attributes.hasOwnProperty("connectionPoolType")
                && typeMetaData.attributes.connectionPoolType.hasOwnProperty("validValues"))
            {
                values = typeMetaData.attributes.connectionPoolType.validValues;
            }
            const store = util.makeTypeStore(values);
            poolTypeControl.set("store", store);
            poolTypeControl.set("value", "NONE");

            const poolTypeFieldsDiv = dom.byId(dialogIdPrefix + ".poolSpecificDiv");
            poolTypeControl.on("change", function (type) {
                if (type && string.trim(type) !== "")
                {
                    const widgets = registry.findWidgets(poolTypeFieldsDiv);
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

            const keystoreWidget = registry.byId(dialogIdPrefix + ".keyStore");
            if (keystoreWidget)
            {
                xhr("/api/latest/keystore", {handleAs: "json"})
                    .then((keystores) => {
                        const keystoresStore = new Memory({
                            data: keystores.map(keystore => ({
                                id: keystore.name,
                                name: keystore.name
                            }))
                        });
                        keystoreWidget.set("store", keystoresStore);
                        keystoreWidget.startup();
                    });
            }

            const truststoreWidget = registry.byId(dialogIdPrefix + ".trustStore");
            if (truststoreWidget)
            {
                xhr("/api/latest/truststore", {handleAs: "json"})
                    .then((truststores) => {
                        const truststoresStore = new Memory({
                            data: truststores.map(truststore => ({
                                id: truststore.name,
                                name: truststore.name
                            }))
                        });
                        truststoreWidget.set("store", truststoresStore);
                        truststoreWidget.startup();
                    });
            }
        };
        return ConnectionPool;
    });
