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
        "dojo/_base/lang",
        "dijit/registry",
        "dijit/layout/ContentPane",
        "dijit/form/CheckBox",
        "dojox/html/entities",
        "qpid/common/updater",
        "qpid/management/Broker",
        "qpid/management/VirtualHost",
        "qpid/management/Exchange",
        "qpid/management/Queue",
        "qpid/management/Connection",
        "qpid/management/AuthenticationProvider",
        "qpid/management/GroupProvider",
        "qpid/management/group/Group",
        "qpid/management/KeyStore",
        "qpid/management/TrustStore",
        "qpid/management/AccessControlProvider",
        "qpid/management/Port",
        "qpid/management/Plugin",
        "qpid/management/VirtualHostNode",
        "qpid/management/Logger",
        "qpid/management/QueryTab",
        "qpid/management/QueryBrowserTab",
        "qpid/management/DashboardTab",
        "qpid/management/DashboardBrowserTab",
        "qpid/common/util",
        "dojo/ready",
        "dojox/uuid/generateRandomUuid",
        "dojo/domReady!"],
    function (dom,
              lang,
              registry,
              ContentPane,
              CheckBox,
              entities,
              updater,
              Broker,
              VirtualHost,
              Exchange,
              Queue,
              Connection,
              AuthProvider,
              GroupProvider,
              Group,
              KeyStore,
              TrustStore,
              AccessControlProvider,
              Port,
              Plugin,
              VirtualHostNode,
              Logger,
              QueryTab,
              QueryBrowserTab,
              DashboardTab,
              DashboardBrowserTab,
              util,
              ready)
    {
        var controller = {};

        var constructors = {
            broker: Broker,
            virtualhost: VirtualHost,
            exchange: Exchange,
            queue: Queue,
            connection: Connection,
            authenticationprovider: AuthProvider,
            groupprovider: GroupProvider,
            group: Group,
            keystore: KeyStore,
            truststore: TrustStore,
            accesscontrolprovider: AccessControlProvider,
            virtualhostaccesscontrolprovider: AccessControlProvider,
            port: Port,
            plugin: Plugin,
            virtualhostnode: VirtualHostNode,
            brokerlogger: Logger,
            virtualhostlogger: Logger,
            query: QueryTab,
            dashboard: DashboardTab,
            queryBrowser: QueryBrowserTab,
            dashboardBrowser: DashboardBrowserTab
        };

        ready(function ()
        {
            controller.tabContainer = registry.byId("managedViews");
            controller.tabContainer.watch("selectedChildWidget", function(name, oval, nval){
                updater.restartTimer();
            });
        });

        controller.viewedObjects = {};

        var generateTabObjId = function(tabData)
        {
            if (tabData.preferenceId)
            {
                return tabData.preferenceId;
            }
            else if (tabData.modelObject && tabData.modelObject.id)
            {
                return tabData.modelObject.id;
            }
            else
            {
                return tabData.tabType;
            }
        };

        controller.showById = function(id)
        {
            var modelObject = this.structure.findById(id);
            if (modelObject != null)
            {
                this.showTab({
                    tabType: modelObject.type,
                    modelObject: modelObject
                });
            }
        };

        controller.showTab = function (tabData)
        {
            var tabType = tabData.tabType;

            var that = this;
            var tabObjectId = generateTabObjId(tabData);

            var tabObject = this.viewedObjects[tabObjectId];
            if (tabObject)
            {
                this.tabContainer.selectChild(tabObject.contentPane);
            }
            else
            {
                var Constructor = constructors[tabType];
                if (Constructor)
                {
                    tabObject = new Constructor({
                        tabData: tabData,
                        controller: this
                    });
                    this.viewedObjects[tabObjectId] = tabObject;

                    var contentPane = new ContentPane({
                        region: "center",
                        title: entities.encode(tabObject.getTitle()),
                        closable: true,
                        onClose: function ()
                        {
                            var closeResult = tabObject.close();
                            if (typeof closeResult === "undefined" || (typeof closeResult === "boolean" && closeResult))
                            {
                                delete that.viewedObjects[tabObjectId];
                                return true;
                            }
                            return false;
                        }
                    });
                    this.tabContainer.addChild(contentPane);
                    var userPreferences = this.management.userPreferences;
                    if (tabType != "broker")
                    {
                        var preferencesCheckBox = new dijit.form.CheckBox({
                            checked: userPreferences.isTabStored(tabData),
                            title: "If checked the tab will be restored on next login"
                        });
                        var tabs = this.tabContainer.tablist.getChildren();
                        preferencesCheckBox.placeAt(tabs[tabs.length - 1].titleNode, "first");
                        preferencesCheckBox.on("change", function (value)
                        {
                            if (value)
                            {
                                userPreferences.appendTab(tabData);
                            }
                            else
                            {
                                userPreferences.removeTab(tabData);
                            }
                        });
                    }
                    tabObject.open(contentPane);
                    contentPane.startup();
                    if (tabObject.startup)
                    {
                        tabObject.startup();
                    }
                    this.tabContainer.selectChild(contentPane);
                }

            }

        };

        var openTabs = function (controller, management, structure)
        {
            try
            {
                var brokers = structure.findByType("broker");
                if (brokers[0])
                {
                    controller.showById(brokers[0].id);
                }

                var savedTabs = management.userPreferences.getSavedTabs();
                if (savedTabs)
                {
                    for (var i in savedTabs)
                    {
                        var savedTab = savedTabs[i];
                        if (savedTab.configuredObjectId)
                        {
                            var modelObject = structure.findById(savedTab.configuredObjectId);
                            if (modelObject)
                            {
                                var tabData = {
                                    tabType: savedTab.tabType,
                                    modelObject: modelObject
                                };

                                if (savedTab.preferenceId)
                                {
                                    tabData["preferenceId"] = savedTab.preferenceId;
                                }

                                controller.showTab(tabData);
                            }
                            else
                            {
                                management.userPreferences.removeTab(savedTab);
                            }
                        }
                        else
                        {
                            controller.showTab(savedTab);
                        }
                    }
                }
            }
            catch (e)
            {
                console.error(e);
            }
        };

        controller.init = function (management, structure, treeView)
        {
            controller.management = management;
            controller.structure = structure;

            var structureUpdate = function()
            {
              var promise = management.get({url: "service/structure"});
              return promise.then(lang.hitch(this, function (data)
              {
                  structure.update(data);
                  treeView.update(data);
              }));
            };

            var initialUpdate = structureUpdate();
            initialUpdate.then(lang.hitch(this, function ()
            {
                updater.add({update : structureUpdate});

                openTabs(controller, management, structure);
            }));
        };

        return controller;
    });
