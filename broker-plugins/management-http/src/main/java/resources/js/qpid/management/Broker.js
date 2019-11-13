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
define(["dojo/parser",
        "dojo/query",
        "dojo/json",
        "dojo/dom",
        "dojo/_base/lang",
        "dojo/_base/event",
        "dojo/_base/connect",
        "dojo/store/Memory",
        "dojo/promise/all",
        "dojo/request/xhr",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojox/widget/Standby",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/management/addAuthenticationProvider",
        "qpid/management/addVirtualHostNodeAndVirtualHost",
        "qpid/management/addVirtualHost",
        "qpid/management/addPort",
        "qpid/management/addStore",
        "qpid/management/addGroupProvider",
        "qpid/management/addAccessControlProvider",
        "qpid/management/editBroker",
        "qpid/management/addLogger",
        "dojo/text!showBroker.html",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dijit/layout/AccordionContainer",
        "dijit/layout/AccordionPane",
        "dijit/form/FilteringSelect",
        "dijit/form/NumberSpinner",
        "dijit/form/ValidationTextBox",
        "dijit/form/CheckBox",
        "dojo/store/Memory",
        "dijit/form/DropDownButton",
        "dijit/Menu",
        "dijit/MenuItem",
        "qpid/common/StatisticsWidget",
        "dojo/domReady!"],
    function (parser,
              query,
              json,
              dom,
              lang,
              event,
              connect,
              memory,
              all,
              xhr,
              properties,
              updater,
              util,
              UpdatableStore,
              EnhancedGrid,
              Standby,
              registry,
              entities,
              addAuthenticationProvider,
              addVirtualHostNodeAndVirtualHost,
              AddVirtualHostDialog,
              addPort,
              addStore,
              addGroupProvider,
              addAccessControlProvider,
              editBroker,
              addLogger,
              template)
    {

        var brokerAttributeNames = ["name",
                                    "operatingSystem",
                                    "platform",
                                    "productVersion",
                                    "processPid",
                                    "modelVersion",
                                    "statisticsReportingPeriod",
                                    "statisticsReportingResetEnabled",
                                    "confidentialConfigurationEncryptionProvider"];

        function Broker(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        Broker.prototype.getTitle = function ()
        {
            return "Broker";
        };

        Broker.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            {
                contentPane.containerNode.innerHTML = template;
                parser.parse(contentPane.containerNode)
                    .then(function (instances)
                    {
                        that.brokerUpdater = new BrokerUpdater(that);

                        var addProviderButton = query(".addAuthenticationProvider", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addProviderButton), "onClick", function (evt)
                        {
                            addAuthenticationProvider.show(that.management, that.modelObj);
                        });

                        var deleteProviderButton = query(".deleteAuthenticationProvider", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deleteProviderButton), "onClick", function (evt)
                        {
                            var warning = "";
                            var data = that.brokerUpdater.authenticationProvidersGrid.grid.selection.getSelected();
                            if (data.length && data.length > 0)
                            {
                                for (var i = 0; i < data.length; i++)
                                {
                                    if (data[i].type.indexOf("File") != -1)
                                    {
                                        warning =
                                            "NOTE: provider deletion will also remove the password file on disk.\n\n"
                                        break;
                                    }
                                }
                            }

                            util.deleteSelectedObjects(that.brokerUpdater.authenticationProvidersGrid.grid,
                                warning + "Are you sure you want to delete authentication provider",
                                that.management,
                                {
                                    type: "authenticationprovider",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                        var addVHNAndVHButton = query(".addVirtualHostNodeAndVirtualHostButton",
                            contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addVHNAndVHButton), "onClick", function (evt)
                        {
                            addVirtualHostNodeAndVirtualHost.show(that.controller.management);
                        });

                        var addPortButton = query(".addPort", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addPortButton), "onClick", function (evt)
                        {
                            addPort.show(that.management,
                                that.modelObj,
                                "AMQP",
                                that.brokerUpdater.brokerData.authenticationproviders,
                                that.brokerUpdater.brokerData.keystores,
                                that.brokerUpdater.brokerData.truststores);
                        });

                        var deletePort = query(".deletePort", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deletePort), "onClick", function (evt)
                        {
                            util.deleteSelectedObjects(that.brokerUpdater.portsGrid.grid,
                                "Are you sure you want to delete port",
                                that.management,
                                {
                                    type: "port",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);

                        });

                        var editButton = query(".editBroker", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(editButton), "onClick", function (evt)
                        {
                            editBroker.show(that.management, that.brokerUpdater.brokerData);
                        });
                        var restartButton = query(".restartBroker", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(restartButton), "onClick", function (evt)
                        {
                            that.restartBroker();
                            event.stop(evt);
                        });

                        var brokerDiagnosticsMenu = registry.byNode(query(".brokerDiagnosticsMenu",
                                                                          contentPane.containerNode)[0]);

                        var hostMenuItems = brokerDiagnosticsMenu.dropDown.getChildren();
                        var downloadJvmThreadDumpButton = hostMenuItems[0];
                        downloadJvmThreadDumpButton.on("click", function (evt)
                        {
                            that.downloadJvmThreadDump();
                            event.stop(evt);
                        });

                        var addKeystoreButton = query(".addKeystore", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addKeystoreButton), "onClick", function (evt)
                        {
                            addStore.setupTypeStore(that.management, "KeyStore", that.modelObj);
                            addStore.show();
                        });

                        var deleteKeystore = query(".deleteKeystore", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deleteKeystore), "onClick", function (evt)
                        {
                            util.deleteSelectedObjects(that.brokerUpdater.keyStoresGrid.grid,
                                "Are you sure you want to delete key store",
                                that.management,
                                {
                                    type: "keystore",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                        var addTruststoreButton = query(".addTruststore", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addTruststoreButton), "onClick", function (evt)
                        {
                            addStore.setupTypeStore(that.management, "TrustStore", that.modelObj);
                            addStore.show();
                        });

                        var deleteTruststore = query(".deleteTruststore", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deleteTruststore), "onClick", function (evt)
                        {
                            util.deleteSelectedObjects(that.brokerUpdater.trustStoresGrid.grid,
                                "Are you sure you want to delete trust store",
                                that.management,
                                {
                                    type: "truststore",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                        var addGroupProviderButton = query(".addGroupProvider", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addGroupProviderButton), "onClick", function (evt)
                        {
                            addGroupProvider.show(that.controller.management, that.modelObj);
                        });

                        var deleteGroupProvider = query(".deleteGroupProvider", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deleteGroupProvider), "onClick", function (evt)
                        {
                            var warning = "";
                            var data = that.brokerUpdater.groupProvidersGrid.grid.selection.getSelected();
                            if (data.length && data.length > 0)
                            {
                                for (var i = 0; i < data.length; i++)
                                {
                                    if (data[i].type.indexOf("File") !== -1)
                                    {
                                        warning = "NOTE: provider deletion will also remove the group file on disk.\n\n"
                                        break;
                                    }
                                }
                            }

                            util.deleteSelectedObjects(that.brokerUpdater.groupProvidersGrid.grid,
                                warning + "Are you sure you want to delete group provider",
                                that.management,
                                {
                                    type: "groupprovider",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                        var addAccessControlButton = query(".addAccessControlProvider", contentPane.containerNode)[0];
                        connect.connect(registry.byNode(addAccessControlButton), "onClick", function (evt)
                        {
                            addAccessControlProvider.show(that.management, that.modelObj);
                        });

                        var deleteAccessControlProviderButton = query(".deleteAccessControlProvider",
                            contentPane.containerNode)[0];
                        connect.connect(registry.byNode(deleteAccessControlProviderButton), "onClick", function (evt)
                        {
                            util.deleteSelectedObjects(that.brokerUpdater.accessControlProvidersGrid.grid,
                                "Are you sure you want to delete access control provider",
                                that.management,
                                {
                                    type: "accesscontrolprovider",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                        var addLoggerButtonNode = query(".addBrokerLogger", contentPane.containerNode)[0];
                        var addLoggerButton = registry.byNode(addLoggerButtonNode);
                        addLoggerButton.on("click", function (evt)
                        {
                            addLogger.show(that.management, that.modelObj, "BrokerLogger");
                        });

                        var deleteLoggerButtonNode = query(".deleteBrokerLogger", contentPane.containerNode)[0];
                        var deleteLoggerButton = registry.byNode(deleteLoggerButtonNode);
                        deleteLoggerButton.on("click", function (evt)
                        {
                            util.deleteSelectedObjects(that.brokerUpdater.brokerLoggersGrid.grid,
                                "Are you sure you want to delete broker logger",
                                that.management,
                                {
                                    type: "brokerlogger",
                                    parent: that.modelObj
                                },
                                that.brokerUpdater);
                        });

                    });
            }
        };

        Broker.prototype.restartBroker = function ()
        {
            if (confirm("Warning! Restart will disconnect all applications and logoff all users, including you."
                        + " All messaging operations will be interrupted until applications reconnect."
                        + " Are you certain you wish to proceed?"))
            {
                var brokerUrl = this.management.objectToURL(this.modelObj);
                this.management.post({url: brokerUrl + "/restart"}, {})
                    .then(lang.hitch(this, function ()
                    {
                        updater.cancel();
                        var standby = new Standby({target: dojo.body()});
                        document.body.appendChild(standby.domNode);
                        standby.startup();
                        standby.show();
                        var logout = function ()
                        {
                            window.location = "logout";
                        };
                        var ping = function ()
                        {
                            xhr(brokerUrl, {method: "GET"})
                                .then(logout,
                                    function (error) {
                                        if (error.response.status === 401 || error.response.status === 403)
                                        {
                                            logout();
                                        }
                                        else
                                        {
                                            ping();
                                        }
                                    });
                        };
                        var timer = setInterval(lang.hitch(this, ping), 5000);
                    }));
            }
        };

        Broker.prototype.downloadJvmThreadDump = function()
        {
            var suggestedAttachmentName = encodeURIComponent("QpidBroker_" + this.name + "_ThreadDump.txt");
            this.management.downloadIntoFrame(this.modelObj, {
                contentDispositionAttachmentFilename: suggestedAttachmentName
            }, "getThreadStackTraces");
        };

        Broker.prototype.close = function ()
        {
            updater.remove(this.brokerUpdater);
        };

        function BrokerUpdater(brokerTab)
        {
            var node = brokerTab.contentPane.containerNode;
            this.controller = brokerTab.controller;
            this.brokerObj = brokerTab.modelObj;
            this.contentPane = brokerTab.contentPane;

            this.accessControlProvidersWarn = query(".broker-access-control-providers-warning", node)[0];
            this.management = this.controller.management;
            var that = this;

            var gridProperties = {
                height: 400,
                selectionMode: "single",
                plugins: {
                    pagination: {
                        pageSizes: [10, 25, 50, 100],
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    }
                }
            };

            function isActiveVH(item)
            {
                return item && item.vhId && item.vhState === "ACTIVE";
            }

            this.vhostsGrid =
                new UpdatableStore([], query(".broker-virtualhosts")[0], [{
                    name: "Node Name",
                    field: "name",
                    width: "8%"
                }, {
                    name: "Node State",
                    field: "state",
                    width: "8%"
                }, {
                    name: "Node Type",
                    field: "type",
                    width: "8%"
                }, {
                    name: "Default",
                    field: "default",
                    width: "8%",
                    formatter: function (item)
                    {
                        return "<input type='checkbox' disabled='disabled' " + (item ? "checked='checked'" : "")
                               + " />";
                    }
                }, {
                    name: "Host Name",
                    field: "vhName",
                    width: "8%"
                }, {
                    name: "Host State",
                    field: "vhState",
                    width: "15%"
                }, {
                    name: "Host Type",
                    field: "vhType",
                    width: "15%"
                }, {
                    name: "Connections",
                    field: "_item",
                    width: "8%",
                    formatter: function (item)
                    {
                        return isActiveVH(item) ? item.connectionCount : "N/A";
                    }
                }, {
                    name: "Queues",
                    field: "_item",
                    width: "8%",
                    formatter: function (item)
                    {
                        return isActiveVH(item) ? item.queueCount : "N/A";
                    }
                }, {
                    name: "Exchanges",
                    field: "_item",
                    width: "8%",
                    formatter: function (item)
                    {
                        return isActiveVH(item) ? item.exchangeCount : "N/A";
                    }
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var idx = evt.rowIndex, theItem = this.getItem(idx);
                        if (theItem.vhId)
                        {
                            that.showVirtualHost(theItem);
                        }
                    });
                }, gridProperties, EnhancedGrid, true);

            this.virtualHostNodeMenuButton = registry.byNode(query(".virtualHostNodeMenuButton", node)[0]);
            this.virtualHostMenuButton = registry.byNode(query(".virtualHostMenuButton", node)[0]);

            var hostMenuItems = this.virtualHostMenuButton.dropDown.getChildren();
            var addVirtualHostItem = hostMenuItems[0];
            var viewVirtualHostItem = hostMenuItems[1];
            var startVirtualHostItem = hostMenuItems[2];
            var stopVirtualHostItem = hostMenuItems[3];

            var nodeMenuItems = this.virtualHostNodeMenuButton.dropDown.getChildren();
            var viewNodeItem = nodeMenuItems[0];
            var deleteNodeItem = nodeMenuItems[1];
            var startNodeItem = nodeMenuItems[2];
            var stopNodeItem = nodeMenuItems[3];

            var toggler = function (index)
            {
                that.toggleVirtualHostNodeNodeMenus(index);
            };
            connect.connect(this.vhostsGrid.grid.selection, 'onSelected', toggler);
            connect.connect(this.vhostsGrid.grid.selection, 'onDeselected', toggler);

            addVirtualHostItem.on("click", function ()
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length === 1)
                {
                    var item = data[0];
                    var nodeModelObject = that.controller.structure.findById(item.id);
                    var dialog = new AddVirtualHostDialog({
                        management: that.controller.management,
                        virtualhostNodeType: item.type,
                        virtualhostNodeModelObject: nodeModelObject
                    });
                    dialog.show();
                    dialog.on("done", function (update)
                    {
                        dialog.hideAndDestroy();
                        that.vhostsGrid.grid.selection.clear();
                        if (update)
                        {
                            that.update();
                        }
                    });
                }
            });

            viewVirtualHostItem.on("click", function ()
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1)
                {
                    that.showVirtualHost(data[0]);
                    that.vhostsGrid.grid.selection.clear();
                }
            });

            viewNodeItem.on("click", function (evt)
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1)
                {
                    var item = data[0];
                    that.controller.showById(item.id);
                    that.vhostsGrid.grid.selection.clear();
                }
            });

            deleteNodeItem.on("click", function (evt)
            {
                util.deleteSelectedObjects(that.vhostsGrid.grid,
                    "Deletion of virtual host node will delete both configuration and message data.\n\n Are you sure you want to delete virtual host node",
                    that.management,
                    {
                        type: "virtualhostnode",
                        parent: that.modelObj
                    },
                    that.brokerUpdater);
            });

            startNodeItem.on("click", function (event)
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1)
                {
                    var item = data[0];
                    that.management.update({
                            type: "virtualhostnode",
                            name: item.name,
                            parent: that.modelObj
                        }, {desiredState: "ACTIVE"})
                        .then(function (data)
                        {
                            that.vhostsGrid.grid.selection.clear();
                        });
                }
            });

            stopNodeItem.on("click", function (event)
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1)
                {
                    var item = data[0];
                    if (confirm("Stopping the node will also shutdown the virtual host. "
                                + "Are you sure you want to stop virtual host node '" + entities.encode(String(
                                item.name)) + "'?"))
                    {
                        that.management.update({
                                type: "virtualhostnode",
                                name: item.name,
                                parent: that.modelObj
                            }, {desiredState: "STOPPED"})
                            .then(function (data)
                            {
                                that.vhostsGrid.grid.selection.clear();
                            });
                    }
                }
            });

            startVirtualHostItem.on("click", function (event)
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1 && data[0].vhId)
                {
                    var item = data[0];
                    that.management.update({
                            type: "virtualhost",
                            name: item.vhName,
                            parent: {
                                type: "virtualhostnode",
                                name: item.name,
                                parent: that.modelObj
                            }
                        }, {desiredState: "ACTIVE"})
                        .then(function (data)
                        {
                            that.vhostsGrid.grid.selection.clear();
                        });
                }
            });

            stopVirtualHostItem.on("click", function (event)
            {
                var data = that.vhostsGrid.grid.selection.getSelected();
                if (data.length == 1 && data[0].vhId)
                {
                    var item = data[0];
                    if (confirm("Are you sure you want to stop virtual host '"
                                + entities.encode(String(item.vhName)) + "'?"))
                    {
                        that.management.update({
                                type: "virtualhost",
                                name: item.vhName,
                                parent: {
                                    type: "virtualhostnode",
                                    name: item.name,
                                    parent: that.modelObj
                                }
                            }, {desiredState: "STOPPED"})
                            .then(function (data)
                            {
                                that.vhostsGrid.grid.selection.clear();
                            });
                    }
                }
            });
            gridProperties.selectionMode = "extended";
            gridProperties.plugins.indirectSelection = true;

            this.portsGrid = new UpdatableStore([], query(".broker-ports")[0], [{
                name: "Name",
                field: "name",
                width: "15%"
            }, {
                name: "State",
                field: "state",
                width: "10%"
            }, {
                name: "Auth Provider",
                field: "authenticationProvider",
                width: "15%"
            }, {
                name: "Address",
                field: "bindingAddress",
                width: "15%"
            }, {
                name: "Port",
                field: "port",
                width: "5%"
            }, {
                name: "Transports",
                field: "transports",
                width: "15%"
            }, {
                name: "Protocols",
                field: "protocols",
                width: "20%"
            }, {
                name: "Bound Port",
                field: "boundPort",
                width: "5%",
                formatter: function (val)
                {
                    return val > -1 ? val : "-";
                }
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var theItem = this.getItem(evt.rowIndex);
                    that.controller.showById(theItem.id);
                });
            }, gridProperties, EnhancedGrid);

            gridProperties = {
                keepSelection: true,
                plugins: {
                    indirectSelection: true
                }
            };

            this.authenticationProvidersGrid =
                new UpdatableStore([], query(
                    ".broker-authentication-providers")[0], [{
                    name: "Name",
                    field: "name",
                    width: "40%"
                }, {
                    name: "State",
                    field: "state",
                    width: "20%"
                }, {
                    name: "Type",
                    field: "type",
                    width: "20%"
                }, {
                    name: "User Management",
                    field: "type",
                    width: "20%",
                    formatter: function (val)
                    {
                        var isProviderManagingUsers = false;
                        try
                        {
                            isProviderManagingUsers = that.management.metadata.implementsManagedInterface(
                                "AuthenticationProvider",
                                val,
                                "PasswordCredentialManagingAuthenticationProvider");
                        }
                        catch (e)
                        {
                            console.error(e);
                        }
                        return "<input type='radio' disabled='disabled' " + (isProviderManagingUsers
                                ? "checked='checked'"
                                : "") + " />";
                    }
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var theItem = this.getItem(evt.rowIndex);
                        that.controller.showById(theItem.id);
                    });
                }, gridProperties, EnhancedGrid);

            this.keyStoresGrid =
                new UpdatableStore([], query(".broker-key-stores")[0], [{
                    name: "Name",
                    field: "name",
                    width: "20%"
                }, {
                    name: "State",
                    field: "state",
                    width: "10%"
                }, {
                    name: "Type",
                    field: "type",
                    width: "10%"
                }, {
                    name: "Path",
                    field: "path",
                    width: "60%"
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var theItem = this.getItem(evt.rowIndex);
                        that.controller.showById(theItem.id);
                    });
                }, gridProperties, EnhancedGrid);

            this.trustStoresGrid =
                new UpdatableStore([], query(".broker-trust-stores")[0], [{
                    name: "Name",
                    field: "name",
                    width: "20%"
                }, {
                    name: "State",
                    field: "state",
                    width: "10%"
                }, {
                    name: "Type",
                    field: "type",
                    width: "10%"
                }, {
                    name: "Path",
                    field: "path",
                    width: "50%"
                }, {
                    name: "Peers only",
                    field: "peersOnly",
                    width: "10%",
                    formatter: function (val)
                    {
                        return "<input type='radio' disabled='disabled' " + (val ? "checked='checked'" : "") + " />";
                    }
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var theItem = this.getItem(evt.rowIndex);
                        that.controller.showById(theItem.id);
                    });
                }, gridProperties, EnhancedGrid);
            this.groupProvidersGrid =
                new UpdatableStore([], query(".broker-group-providers")[0], [{
                    name: "Name",
                    field: "name",
                    width: "40%"
                }, {
                    name: "State",
                    field: "state",
                    width: "30%"
                }, {
                    name: "Type",
                    field: "type",
                    width: "30%"
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var theItem = this.getItem(evt.rowIndex);
                        that.controller.showById(theItem.id);
                    });
                }, gridProperties, EnhancedGrid);
            this.accessControlProvidersGrid =
                new UpdatableStore([], query(".broker-access-control-providers")[0], [{
                    name: "Name",
                    field: "name",
                    width: "20%"
                }, {
                    name: "State",
                    field: "state",
                    width: "20%"
                }, {
                    name: "Type",
                    field: "type",
                    width: "40%"
                }, {
                    name: "Priority",
                    field: "priority",
                    width: "20%"
                }], function (obj)
                {
                    connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                    {
                        var theItem = this.getItem(evt.rowIndex);
                        that.controller.showById(theItem.id);
                    });
                }, gridProperties, EnhancedGrid);

            this.brokerLoggersGrid = new UpdatableStore([], query(".broker-loggers")[0], [{
                name: "Name",
                field: "name",
                width: "40%"
            }, {
                name: "State",
                field: "state",
                width: "15%"
            }, {
                name: "Type",
                field: "type",
                width: "15%"
            }, {
                name: "Exclude Virtual Host Logs",
                field: "virtualHostLogEventExcluded",
                width: "20%",
                formatter: function (val)
                {
                    return util.buildCheckboxMarkup(val);
                }
            }, {
                name: "Errors",
                field: "errorCount",
                width: "5%"
            }, {
                name: "Warnings",
                field: "warnCount",
                width: "5%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var theItem = this.getItem(evt.rowIndex);
                    that.controller.showById(theItem.id);
                });
            }, gridProperties, EnhancedGrid);
            this.update(function ()
            {
                updater.add(that);
            });
        }

        BrokerUpdater.prototype.showVirtualHost = function (item)
        {
            this.controller.showById(item.vhId);
        };

        BrokerUpdater.prototype.updateHeader = function ()
        {
            var brokerData = this.brokerData;
            window.document.title = "Qpid: " + brokerData.name + " Management";

            for (var i in brokerAttributeNames)
            {
                var propertyName = brokerAttributeNames[i];
                var element = dojo.byId("brokerAttribute." + propertyName);
                if (element)
                {
                    if (brokerData.hasOwnProperty(propertyName))
                    {
                        var container = dojo.byId("brokerAttribute." + propertyName + ".container");
                        if (container)
                        {
                            container.style.display = "block";
                        }
                        element.innerHTML = entities.encode(String(brokerData [propertyName]));
                    }
                    else
                    {
                        element.innerHTML = "";
                    }
                }
            }
        };

        BrokerUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;

            var brokerResponsePromise = this.management.load(this.brokerObj,
                {
                    depth: 1,
                    excludeInheritedContext: true
                });
            var virtualHostsResponsePromise = this.management.query({
                category: "VirtualHost",
                select: "$parent.id AS id, $parent.name AS name, $parent.state AS state,"
                        + " $parent.defaultVirtualHostNode AS default, $parent.type AS type,"
                        + " id AS vhId, name AS vhName, type AS vhType, state AS vhState,"
                        + " connectionCount, queueCount, exchangeCount",
                orderBy: "name"
            });
            all({
                broker: brokerResponsePromise,
                virtualHosts: virtualHostsResponsePromise
            })
                .then(function (data)
                {
                    that.brokerData = data.broker;

                    if (!that.brokerStatistics)
                    {
                        that.brokerStatistics = new qpid.common.StatisticsWidget({
                            category:  "Broker",
                            type: null,
                            management: that.controller.management,
                            defaultStatistics: ["messagesIn", "messagesOut"]
                        });
                        that.brokerStatistics.placeAt(dom.byId("showBroker.statistics"));
                        that.brokerStatistics.startup();
                    }
                    var virtualHostData = util.queryResultToObjects(data.virtualHosts);
                    var queryVirtualHostNodes = {};
                    for (var i = 0; i < virtualHostData.length; ++i)
                    {
                        queryVirtualHostNodes[virtualHostData[i].id] = null;
                    }
                    if (that.brokerData.virtualhostnodes && that.brokerData.virtualhostnodes.length)
                    {
                        for (var i = 0;i < that.brokerData.virtualhostnodes.length; i++)
                        {
                            var node = that.brokerData.virtualhostnodes[i];
                            var nodeId = node.id;
                            if (!(nodeId in queryVirtualHostNodes))
                            {
                                virtualHostData.push({
                                    id: nodeId,
                                    name: node.name,
                                    type: node.type,
                                    state: node.state,
                                    "default": node.defaultVirtualHostNode,
                                    vhId: null,
                                    vhName: "N/A",
                                    vhType: "N/A",
                                    vhState: "N/A",
                                    connectionCount: "N/A",
                                    queueCount: "N/A",
                                    exchangeCount: "N/A"
                                });
                            }
                        }
                    }
                    that.brokerStatistics.update(that.brokerData.statistics);

                    util.flattenStatistics(that.brokerData);

                    that.updateHeader();

                    if (that.vhostsGrid.update(virtualHostData))
                    {
                        that.vhostsGrid.grid._refresh();
                        that.toggleVirtualHostNodeNodeMenus();
                    }

                    that.portsGrid.update(that.brokerData.ports);

                    that.authenticationProvidersGrid.update(that.brokerData.authenticationproviders);

                    if (that.keyStoresGrid)
                    {
                        that.keyStoresGrid.update(that.brokerData.keystores);
                    }
                    if (that.trustStoresGrid)
                    {
                        that.trustStoresGrid.update(that.brokerData.truststores);
                    }
                    if (that.groupProvidersGrid)
                    {
                        that.groupProvidersGrid.update(that.brokerData.groupproviders);
                    }
                    if (that.accessControlProvidersGrid)
                    {
                        var data = that.brokerData.accesscontrolproviders ? that.brokerData.accesscontrolproviders : [];
                        that.accessControlProvidersGrid.update(data);
                    }
                    if (that.brokerLoggersGrid)
                    {
                        that.brokerLoggersGrid.update(that.brokerData.brokerloggers);
                    }
                    if (callback)
                    {
                        callback();
                    }
                });
        };

        BrokerUpdater.prototype.toggleVirtualHostNodeNodeMenus = function (rowIndex)
        {
            var data = this.vhostsGrid.grid.selection.getSelected();
            var selected = data && data[0];
            this.virtualHostNodeMenuButton.set("disabled", !selected);
            this.virtualHostMenuButton.set("disabled", !selected);
            if (selected)
            {
                var nodeMenuItems = this.virtualHostNodeMenuButton.dropDown.getChildren();
                var hostMenuItems = this.virtualHostMenuButton.dropDown.getChildren();

                var startNodeItem = nodeMenuItems[2];
                var stopNodeItem = nodeMenuItems[3];

                var addVirtualHostItem = hostMenuItems[0];
                var viewVirtualHostItem = hostMenuItems[1];
                var startVirtualHostItem = hostMenuItems[2];
                var stopVirtualHostItem = hostMenuItems[3];

                var item = data[0];
                startNodeItem.set("disabled", item.state != "STOPPED");
                stopNodeItem.set("disabled", item.state != "ACTIVE");

                if (item.vhId)
                {
                    addVirtualHostItem.set("disabled", true);
                    viewVirtualHostItem.set("disabled", false);
                    startVirtualHostItem.set("disabled", item.vhState != "STOPPED");
                    stopVirtualHostItem.set("disabled", item.vhState != "ACTIVE");
                }
                else
                {
                    addVirtualHostItem.set("disabled", item.state != "ACTIVE");
                    viewVirtualHostItem.set("disabled", true);
                    startVirtualHostItem.set("disabled", true);
                    stopVirtualHostItem.set("disabled", true);
                }
            }
        };
        return Broker;
    });
