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
define(["dojo/_base/lang",
        "dojo/parser",
        "dojo/query",
        "dojo/_base/connect",
        "dijit/registry",
        "qpid/management/store/CertificateGridWidget",
        "dojox/html/entities",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/management/addStore",
        "dojo/text!showTrustStore.html",
        "dojo/domReady!"],
    function (lang, parser, query, connect, registry, CertificateGridWidget, entities, updater, util, addStore, template)
    {
        function TrustStore(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        TrustStore.prototype.getTitle = function ()
        {
            return "TrustStore: " + this.name;
        };

        TrustStore.prototype.open = function (contentPane)
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

                    var deleteTrustStoreButton = query(".deleteStoreButton", contentPane.containerNode)[0];
                    var deleteButtonNode = registry.byNode(deleteTrustStoreButton);
                    connect.connect(deleteButtonNode, "onClick", function (evt)
                    {
                        that.deleteKeyStore();
                    });

                    var editTrustStoreButton = query(".editStoreButton", contentPane.containerNode)[0];
                    var editButtonNode = registry.byNode(editTrustStoreButton);
                    connect.connect(editButtonNode, "onClick", function (evt)
                    {
                        that.management.load(that.modelObj,
                            {
                                actuals: true,
                                excludeInheritedContext: true
                            })
                            .then(function (data)
                            {
                                addStore.setupTypeStore(that.management, "TrustStore", that.modelObj);
                                addStore.show(data, that.keyStoreUpdater.trustStoreData);
                            }, util.xhrErrorHandler);
                    });

                    var gridNode = query(".managedCertificatesGrid", contentPane.containerNode)[0];
                    that.certificatesGrid = new CertificateGridWidget({
                        management: that.management,
                        modelObj: that.modelObj
                    }, gridNode);
                    that.certificatesGrid.startup();
                    that.certificatesGrid.on("certificatesUpdated", lang.hitch(this, function()
                    {
                        that.keyStoreUpdater.update();
                    }));
                });
        };

        TrustStore.prototype.close = function ()
        {
            updater.remove(this.keyStoreUpdater);
            this.certificatesGrid.destroy();
        };

        function KeyStoreUpdater(tabObject)
        {
            var that = this;
            var containerNode = tabObject.contentPane.containerNode;
            this.keyStoreDetailsContainer = query(".typeFieldsContainer", containerNode)[0];
            this.management = tabObject.management;
            this.modelObj = tabObject.modelObj;
            this.tabObject = tabObject;
            this.contentPane = tabObject.contentPane;

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

            storeNodes(["name", "type", "state", "exposedAsMessageSource", "trustAnchorValidityEnforced",
                "certificateRevocationCheckEnabled", "certificateRevocationCheckOfOnlyEndEntityCertificates",
                "certificateRevocationCheckWithPreferringCertificateRevocationList",
                "certificateRevocationCheckWithNoFallback", "certificateRevocationCheckWithIgnoringSoftFailures",
                "certificateRevocationListUrl"]);

        }

        KeyStoreUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.trustStoreData["name"]));
            this.type.innerHTML = entities.encode(String(this.trustStoreData["type"]));
            this.state.innerHTML = entities.encode(String(this.trustStoreData["state"]));
            this.exposedAsMessageSource.innerHTML =
                entities.encode(String(this.trustStoreData["exposedAsMessageSource"]));
            this.trustAnchorValidityEnforced.innerHTML =
                entities.encode(String(this.trustStoreData["trustAnchorValidityEnforced"]));
            this.certificateRevocationCheckEnabled.innerHTML =
                entities.encode(String(this.trustStoreData["certificateRevocationCheckEnabled"]));
            this.certificateRevocationCheckOfOnlyEndEntityCertificates.innerHTML =
                entities.encode(String(this.trustStoreData["certificateRevocationCheckOfOnlyEndEntityCertificates"]));
            this.certificateRevocationCheckWithPreferringCertificateRevocationList.innerHTML =
                entities.encode(String(this.trustStoreData["certificateRevocationCheckWithPreferringCertificateRevocationList"]));
            this.certificateRevocationCheckWithNoFallback.innerHTML =
                entities.encode(String(this.trustStoreData["certificateRevocationCheckWithNoFallback"]));
            this.certificateRevocationCheckWithIgnoringSoftFailures.innerHTML =
                entities.encode(String(this.trustStoreData["certificateRevocationCheckWithIgnoringSoftFailures"]));
            this.certificateRevocationListUrl.innerHTML = this.trustStoreData["certificateRevocationListUrl"] ?
                entities.encode(String(this.trustStoreData["certificateRevocationListUrl"])) : "";
        };

        KeyStoreUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;
            this.management.load(this.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    that.trustStoreData = data;
                    that.updateHeader();
                    that.tabObject.certificatesGrid.update(that.trustStoreData.certificateDetails);

                    if (callback)
                    {
                        callback();
                    }

                    if (that.details)
                    {
                        that.details.update(that.trustStoreData);
                    }
                    else
                    {
                        var implementsManagedInterface = that.management.metadata.implementsManagedInterface("TrustStore",
                            that.trustStoreData.type,
                            "MutableCertificateTrustStore");
                        that.tabObject.certificatesGrid.enableCertificateControls(implementsManagedInterface);

                        require(["qpid/management/store/" + encodeURIComponent(that.trustStoreData.type.toLowerCase())
                                 + "/show"], function (DetailsUI)
                        {
                            that.details = new DetailsUI({
                                containerNode: that.keyStoreDetailsContainer,
                                parent: that
                            });
                            that.details.update(that.trustStoreData);
                        });
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Trust Store"
                    });
                });
        };

        TrustStore.prototype.deleteKeyStore = function ()
        {
            if (confirm("Are you sure you want to delete trust store '" + this.name + "'?"))
            {
                var that = this;
                this.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.contentPane.onClose();
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                        that.close();
                    }, util.xhrErrorHandler);
            }
        };

        return TrustStore;
    });
