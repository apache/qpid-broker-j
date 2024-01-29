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
define(["dojo/_base/xhr",
        "dojo/_base/array",
        "dojo/parser",
        "dojo/dom",
        "dojo/dom-construct",
        "dojo/json",
        "dojo/string",
        "dojo/request/xhr",
        "dojo/store/Memory",
        "dijit/registry",
        "dojo/text!virtualhost/jdbc/add.html",
        "qpid/common/util",
        "dijit/form/ValidationTextBox",
        "dijit/form/CheckBox",
        "dojo/domReady!"],
    function (xhr, array, parser, dom, domConstruct, json, string, _xhr, Memory, registry, template, util)
    {
        return {
            show: function (data)
            {
                const that = this;
                this.containerNode = domConstruct.create("div", {innerHTML: template}, data.containerNode);
                parser.parse(this.containerNode).then((instances) => that._postParse(data));
            },
            _postParse: function (data)
            {
                const that = this;
                registry.byId("addVirtualHost.connectionUrl")
                    .set("regExpGen", util.jdbcUrlOrContextVarRegexp);
                registry.byId("addVirtualHost.username")
                    .set("regExpGen", util.nameOrContextVarRegexp);

                const typeMetaData = data.metadata.getMetaData("VirtualHost", "JDBC");
                const poolTypes = typeMetaData.attributes.connectionPoolType.validValues;
                const poolTypesData = [];
                array.forEach(poolTypes, (item) =>
                {
                    poolTypesData.push({
                        id: item,
                        name: item
                    });
                });

                const poolTypesStore = new Memory({data: poolTypesData});
                const poolTypeControl = registry.byId("addVirtualHost.connectionPoolType");
                poolTypeControl.set("store", poolTypesStore);
                poolTypeControl.set("value", "NONE");

                const poolTypeFieldsDiv = dom.byId("addVirtualHost.poolSpecificDiv");
                poolTypeControl.on("change", (type) =>
                {
                    if (type && string.trim(type) !== "")
                    {
                        const widgets = registry.findWidgets(poolTypeFieldsDiv);
                        array.forEach(widgets, (item) => item.destroyRecursive());
                        domConstruct.empty(poolTypeFieldsDiv);
                        require(["qpid/management/store/pool/" + type.toLowerCase() + "/add"], function (poolType)
                        {
                            poolType.show({
                                containerNode: poolTypeFieldsDiv,
                                context: data.parent.virtualHostContext
                            });
                        });
                    }
                });
                util.applyMetadataToWidgets(data.containerNode, "VirtualHost", data.type, data.metadata);

                const keystoreWidget = registry.byId("addVirtualHost.keyStore");
                _xhr("/api/latest/keystore", {handleAs: "json"}).then((keystores) =>
                {
                    const keystoresStore = new Memory({
                        data: keystores.map(keystore => ({ id: keystore.name, name: keystore.name }))
                    });
                    keystoreWidget.set("store", keystoresStore);
                    keystoreWidget.startup();
                });

                const truststoreWidget = registry.byId("addVirtualHost.trustStore");
                _xhr("/api/latest/truststore", {handleAs: "json"}).then((truststores) =>
                {
                    const truststoresStore = new Memory({
                        data: truststores.map(truststore => ({ id: truststore.name, name: truststore.name }))
                    });
                    truststoreWidget.set("store", truststoresStore);
                    truststoreWidget.startup();
                });
            }
        };
    });
