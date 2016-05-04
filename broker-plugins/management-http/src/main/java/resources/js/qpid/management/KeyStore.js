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
        "dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/management/addStore",
        "dojo/text!showStore.html",
        "dojo/domReady!"],
    function (dom, parser, query, connect, registry, entities, properties, updater, util, formatter, addStore, template)
    {

        function KeyStore(name, parent, controller)
        {
            this.keyStoreName = name;
            this.controller = controller;
            this.management = controller.management;
            this.modelObj = {
                type: "keystore",
                name: name,
                parent: parent
            };
        }

        KeyStore.prototype.getTitle = function ()
        {
            return "KeyStore: " + this.keyStoreName;
        };

        KeyStore.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.keyStoreUpdater = new KeyStoreUpdater(that);
                    that.keyStoreUpdater.update(function ()
                    {
                        updater.add(that.keyStoreUpdater);
                    });

                    var deleteKeyStoreButton = query(".deleteStoreButton", contentPane.containerNode)[0];
                    var node = registry.byNode(deleteKeyStoreButton);
                    connect.connect(node, "onClick", function (evt)
                    {
                        that.deleteKeyStore();
                    });

                    var editKeyStoreButton = query(".editStoreButton", contentPane.containerNode)[0];
                    var node = registry.byNode(editKeyStoreButton);
                    connect.connect(node, "onClick", function (evt)
                    {
                        management.load(that.modelObj,
                            {
                                actuals: true,
                                excludeInheritedContext: true,
                                depth: 0
                            })
                            .then(function (data)
                            {
                                addStore.setupTypeStore(that.management, "KeyStore", that.modelObj);
                                addStore.show(data[0]);
                            }, util.xhrErrorHandler);
                    });
                });

        };

        KeyStore.prototype.close = function ()
        {
            updater.remove(this.keyStoreUpdater);
        };

        function KeyStoreUpdater(tabObject)
        {
            var containerNode = tabObject.contentPane.containerNode;
            var that = this;
            this.keyStoreDetailsContainer = query(".typeFieldsContainer", containerNode)[0];
            this.management = tabObject.controller.management;
            this.modelObj = tabObject.modelObj;
            this.tabObject = tabObject
            function findNode(name)
            {
                return query("." + name, containerNode)[0];
            }

            function storeNodes(names)
            {
                for (var i = 0; i < names.length; i++)
                {
                    that[names[i]] = findNode(names[i]);
                }
            }

            storeNodes(["name", "type", "state"]);
        }

        KeyStoreUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.keyStoreData["name"]));
            this.type.innerHTML = entities.encode(String(this.keyStoreData["type"]));
            this.state.innerHTML = entities.encode(String(this.keyStoreData["state"]));
        };

        KeyStoreUpdater.prototype.update = function (callback)
        {

            var that = this;

            this.management.load(that.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 0
                })
                .then(function (data)
                {
                    that.keyStoreData = data[0];
                    that.updateHeader();

                    if (callback)
                    {
                        callback();
                    }

                    if (that.details)
                    {
                        that.details.update(that.keyStoreData);
                    }
                    else
                    {
                        require(["qpid/management/store/" + encodeURIComponent(that.keyStoreData.type.toLowerCase())
                                 + "/show"], function (DetailsUI)
                        {
                            that.details = new DetailsUI({
                                containerNode: that.keyStoreDetailsContainer,
                                parent: that
                            });
                            that.details.update(that.keyStoreData);
                        });
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Key Store"
                    });
                });
        };

        KeyStore.prototype.deleteKeyStore = function ()
        {
            if (confirm("Are you sure you want to delete key store '" + this.keyStoreName + "'?"))
            {
                var that = this;
                this.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.contentPane.onClose()
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                        that.close();
                    }, util.xhrErrorHandler);
            }
        }

        return KeyStore;
    });
