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
        "dojo/_base/array",
        "dojo/json",
        "dojo/string",
        "dojo/request/xhr",
        "dojo/store/Memory",
        "dojo/dom",
        "dojo/dom-construct",
        "dijit/registry",
        "dojo/domReady!"], function (util, array, json, string, xhr, Memory, dom, domConstruct, registry)
{
    return {
        show: function (data)
        {
            const that = this;
            util.parseHtmlIntoDiv(data.containerNode, "virtualhost/jdbc/edit.html", () =>
            {
                that._postParse(data)
            });
        },
        _postParse: function (data)
        {
            registry.byId("editVirtualHost.connectionUrl")
                .set("regExpGen", util.jdbcUrlOrContextVarRegexp);
            registry.byId("editVirtualHost.username")
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
            const poolTypeControl = registry.byId("editVirtualHost.connectionPoolType");
            poolTypeControl.set("store", poolTypesStore);
            poolTypeControl.set("value", data.data.connectionPoolType);

            const passwordControl = registry.byId("editVirtualHost.password");
            if (data.data.password)
            {
                passwordControl.set("placeHolder", "*******");
            }

            const poolTypeFieldsDiv = dom.byId("editVirtualHost.poolSpecificDiv");
            poolTypeControl.on("change", function (type)
            {
                if (type && string.trim(type) !== "")
                {
                    const widgets = registry.findWidgets(poolTypeFieldsDiv);
                    array.forEach(widgets, function (item)
                    {
                        item.destroyRecursive();
                    });
                    domConstruct.empty(poolTypeFieldsDiv);

                    require(["qpid/management/store/pool/" + type.toLowerCase() + "/edit"], function (poolType)
                    {
                        poolType.show({
                            containerNode: poolTypeFieldsDiv,
                            data: data.data,
                            context: data.parent.context
                        })
                    });
                }
            });

            const keystoreWidget = registry.byId("editVirtualHost.keyStore");
            xhr("/api/latest/keystore", {handleAs: "json"}).then((keystores) =>
            {
                const keystoresStore = new Memory({
                    data: keystores.map(keystore => ({ id: keystore.name, name: keystore.name }))
                });
                keystoreWidget.set("store", keystoresStore);
                keystoreWidget.setValue(data.data.keyStore);
                keystoreWidget.startup();
            });

            const keyStorePathPropertyNameWidget = registry.byId("editVirtualHost.keyStorePathPropertyName");
            keyStorePathPropertyNameWidget.setValue(data.data.keyStorePathPropertyName);

            const keyStorePasswordPropertyNameWidget = registry.byId("editVirtualHost.keyStorePasswordPropertyName");
            keyStorePasswordPropertyNameWidget.setValue(data.data.keyStorePasswordPropertyName);

            const truststoreWidget = registry.byId("editVirtualHost.trustStore");
            xhr("/api/latest/truststore", {handleAs: "json"}).then((truststores) =>
            {
                const truststoresStore = new Memory({
                    data: truststores.map(truststore => ({ id: truststore.name, name: truststore.name }))
                });
                truststoreWidget.set("store", truststoresStore);
                truststoreWidget.setValue(data.data.trustStore);
                truststoreWidget.startup();
            });

            const trustStorePathPropertyNameWidget = registry.byId("editVirtualHost.keyStorePathPropertyName");
            trustStorePathPropertyNameWidget.setValue(data.data.keyStorePathPropertyName);

            const trustStorePasswordPropertyNameWidget = registry.byId("editVirtualHost.trustStorePasswordPropertyName");
            trustStorePasswordPropertyNameWidget.setValue(data.data.trustStorePasswordPropertyName);

            util.applyToWidgets(data.containerNode, "VirtualHost", data.data.type, data.data, data.metadata);
        }
    };
});
