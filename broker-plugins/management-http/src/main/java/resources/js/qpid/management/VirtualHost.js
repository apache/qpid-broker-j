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
        "dojo/_base/lang",
        "dojo/_base/declare",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/management/addQueue",
        "qpid/management/addExchange",
        "qpid/management/addLogger",
        "qpid/management/query/QueryGrid",
        "qpid/management/editVirtualHost",
        "qpid/common/StatisticsWidget",
        "dojo/text!showVirtualHost.html",
        "dijit/Dialog",
        "dgrid/Grid",
        "dgrid/Selector",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/DijitRegistry",
        "dstore/Memory",
        "dstore/Trackable",
        "dojo/aspect",
        "dojo/domReady!"],
    function (parser,
              query,
              lang,
              declare,
              registry,
              entities,
              updater,
              util,
              formatter,
              addQueue,
              addExchange,
              addLogger,
              QueryGrid,
              editVirtualHost,
              StatisticsWidget,
              template,
              Dialog,
              Grid,
              Selector,
              Keyboard,
              Selection,
              Pagination,
              ColumnResizer,
              DijitRegistry,
              MemoryStore,
              TrackableStore,
              aspect)
    {

        function VirtualHost(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        VirtualHost.prototype.getTitle = function ()
        {
            return "VirtualHost: " + this.name;
        };

        VirtualHost.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;

            var containerNode = contentPane.containerNode;
            containerNode.innerHTML = template;
            parser.parse(containerNode)
                .then(function (instances)
                {
                    that.vhostUpdater = new Updater(that);

                    var addQueueButton = query(".addQueueButton", containerNode)[0];
                    registry.byNode(addQueueButton).on("click", function (evt)
                    {
                        addQueue.show(that.management, that.modelObj)
                    });

                    var deleteQueueButton = query(".deleteQueueButton", containerNode)[0];
                    registry.byNode(deleteQueueButton).on("click", function (evt)
                    {
                        that._deleteSelectedItems(that.vhostUpdater.queuesGrid,
                                                  {type: "queue", parent: that.modelObj},
                                                  "delete", "queue");
                    });

                    var addExchangeButton = query(".addExchangeButton", containerNode)[0];
                    registry.byNode(addExchangeButton).on("click", function (evt)
                    {
                        addExchange.show(that.management, that.modelObj);
                    });

                    var deleteExchangeButton = query(".deleteExchangeButton", containerNode)[0];
                    registry.byNode(deleteExchangeButton).on("click", function (evt)
                    {
                        that._deleteSelectedItems(that.vhostUpdater.exchangesGrid,
                                                 {type: "exchange", parent: that.modelObj},
                                                 "delete", "exchange");
                    });

                    var closeConnectionButton = query(".closeConnectionButton", containerNode)[0];
                    registry.byNode(closeConnectionButton).on("click", function (evt)
                    {
                        that._deleteSelectedItems(that.vhostUpdater.connectionsGrid,
                                                  {type: "connection"},
                                                  "close", "connection");
                    });

                    var addLoggerButtonNode = query(".addVirtualHostLogger", contentPane.containerNode)[0];
                    var addLoggerButton = registry.byNode(addLoggerButtonNode);
                    addLoggerButton.on("click", function (evt)
                    {
                        addLogger.show(that.management, that.modelObj, "VirtualHostLogger");
                    });

                    var deleteLoggerButtonNode = query(".deleteVirtualHostLogger", contentPane.containerNode)[0];
                    var deleteLoggerButton = registry.byNode(deleteLoggerButtonNode);
                    deleteLoggerButton.on("click", function (evt)
                    {
                        that._deleteSelectedItems(that.vhostUpdater.virtualHostLoggersGrid,
                                                  {type: "virtualhostlogger", parent: that.modelObj},
                                                  "delete", "virtual host logger");
                    });

                    that.stopButton = registry.byNode(query(".stopButton", containerNode)[0]);
                    that.startButton = registry.byNode(query(".startButton", containerNode)[0]);
                    that.editButton = registry.byNode(query(".editButton", containerNode)[0]);
                    that.downloadButton = registry.byNode(query(".downloadButton", containerNode)[0]);
                    that.downloadButton.on("click", function (e)
                    {
                        var suggestedAttachmentName = encodeURIComponent(that.name + ".json");
                        that.management.download(that.modelObj, {
                            contentDispositionAttachmentFilename: suggestedAttachmentName
                        }, "extractConfig");
                    });

                    that.deleteButton = registry.byNode(query(".deleteButton", containerNode)[0]);
                    that.deleteButton.on("click", function (e)
                    {
                        if (confirm("Deletion of virtual host will delete message data.\n\n"
                                    + "Are you sure you want to delete virtual host  '"
                                    + entities.encode(String(that.name)) + "'?"))
                        {
                            that.management.remove(that.modelObj)
                                .then(function (result)
                                {
                                    that.destroy();
                                });
                        }
                    });
                    that.startButton.on("click", function (event)
                    {
                        that.startButton.set("disabled", true);
                        that.management.update(that.modelObj, {desiredState: "ACTIVE"})
                            .then();
                    });

                    that.stopButton.on("click", function (event)
                    {
                        if (confirm("Stopping the virtual host will also stop its children. "
                                    + "Are you sure you want to stop virtual host '"
                                    + entities.encode(String(that.name)) + "'?"))
                        {
                            that.stopButton.set("disabled", true);
                            that.management.update(that.modelObj, {desiredState: "STOPPED"})
                                .then();
                        }
                    });

                    that.editButton.on("click", function (event)
                    {
                        editVirtualHost.show(that.management, that.modelObj);
                    });

                    var addVirtualHostAccessControlProviderButton = registry.byNode(query(
                        ".addVirtualHostAccessControlProvider",
                        contentPane.containerNode)[0]);
                    addVirtualHostAccessControlProviderButton.on("click", function () {
                        require(["qpid/management/addAccessControlProvider"],
                            function (addAccessControlProvider) {
                                addAccessControlProvider.show(that.management, that.modelObj);
                            });
                    });

                    var deleteVirtualHostAccessControlProviderButton = registry.byNode(query(
                        ".deleteVirtualHostAccessControlProvider",
                        contentPane.containerNode)[0]);
                    deleteVirtualHostAccessControlProviderButton.on("click", function () {
                        that._deleteSelectedItems(that.vhostUpdater.virtualHostAccessControlProviderGrid,
                            {
                                type: "virtualhostaccesscontrolprovider",
                                parent: that.modelObj
                            },
                            "delete", "virtual host access control provider");
                    });

                    that.vhostUpdater.update(function ()
                    {
                        updater.add(that.vhostUpdater);
                    });
                });
        };

        VirtualHost.prototype._deleteSelectedItems = function (dgrid, modelObj, friendlyAction, friendlyCategoryName)
        {
            var selected = [];
            var selection = dgrid.selection;
            for(var item in selection)
            {
                if (selection.hasOwnProperty(item) && selection[item])
                {
                    selected.push(item);
                }
            }
            if (selected.length > 0)
            {
                var plural = selected.length === 1 ? "" : "s";
                if (confirm(lang.replace("Are you sure you want to {0} {1} {2}{3}?",
                        [friendlyAction,
                         selected.length,
                         entities.encode(friendlyCategoryName),
                         plural])))
                {
                    this.management
                        .remove(modelObj, {"id": selected})
                        .then(lang.hitch(this, function (responseData)
                        {
                            dgrid.clearSelection();
                            this.vhostUpdater.update();
                        }));
                }
            }
        };

        VirtualHost.prototype.close = function ()
        {
            updater.remove(this.vhostUpdater);
        };

        VirtualHost.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        function Updater(virtualHost)
        {
            var vhost = virtualHost.modelObj;
            var controller = virtualHost.controller;
            var node = virtualHost.contentPane.containerNode;

            this.tabObject = virtualHost;
            this.contentPane = virtualHost.contentPane;
            this.management = controller.management;
            this.modelObj = vhost;
            this.vhostData = {};
            var that = this;

            function findNode(name)
            {
                return query("." + name, node)[0];
            }

            function storeNodes(names)
            {
                for (var i = 0; i < names.length; i++)
                {
                    that[names[i]] = findNode(names[i]);
                }
            }

            this.virtualhostStatisticsNode = findNode("virtualhostStatistics");

            storeNodes(["name",
                        "type",
                        "state",
                        "durable",
                        "lifetimePolicy",
                        "virtualHostDetailsContainer",
                        "connectionThreadPoolSize",
                        "statisticsReportingPeriod",
                        "housekeepingCheckPeriod",
                        "housekeepingThreadCount",
                        "storeTransactionIdleTimeoutClose",
                        "storeTransactionIdleTimeoutWarn",
                        "storeTransactionOpenTimeoutClose",
                        "storeTransactionOpenTimeoutWarn",
                        "virtualHostConnections",
                        "virtualHostChildren"]);



            var CustomGrid = declare([QueryGrid, Selector]);

            this.queuesGrid = new CustomGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: util.queryResultToObjects,
                management: this.management,
                parentObject: this.modelObj,
                category: "Queue",
                selectClause: "id, name, type, consumerCount, queueDepthMessages, queueDepthBytes",
                orderBy: "name",
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                columns: [
                    {
                        field: "selected",
                        label: 'All',
                        selector: 'checkbox'
                    }, {
                        label: "Name",
                        field: "name"
                    }, {
                        label: "Type",
                        field: "type"
                    }, {
                        label: "Consumers",
                        field: "consumerCount"
                    }, {
                        label: "Depth (msgs)",
                        field: "queueDepthMessages"
                    }, {
                        label: "Depth (bytes)",
                        field: "queueDepthBytes",
                        formatter: function (value, object)
                        {
                            var bytesFormat = formatter.formatBytes(value);
                            return bytesFormat.toString();
                        }
                    }
                ]
            }, findNode("queues"));
            this.queuesGrid.on('rowBrowsed', function(event){controller.showById(event.id);});
            this.queuesGrid.startup();

            this.exchangesGrid = new CustomGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: util.queryResultToObjects,
                management: this.management,
                parentObject: this.modelObj,
                category: "Exchange",
                selectClause: "id, name, type, bindingCount",
                orderBy: "name",
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                allowSelect: function (row)
                {
                    var item = row.data;
                    var isStandard = item && item.name && util.isReservedExchangeName(item.name);
                    return !isStandard;
                },
                columns: [
                    {
                        field: "selected",
                        label: 'All',
                        selector: 'checkbox'
                    },  {
                        label: "Name",
                        field: "name"
                    }, {
                        label: "Type",
                        field: "type"
                    }, {
                        label: "Binding Count",
                        field: "bindingCount"
                    }
                ]
            }, findNode("exchanges"));
            this.exchangesGrid.on('rowBrowsed', function(event){controller.showById(event.id);});
            this.exchangesGrid.startup();

            this.connectionsGrid = new CustomGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: lang.hitch(this, this._transformConnectionData),
                management: this.management,
                parentObject: this.modelObj,
                category: "Connection",
                selectClause: "id, name, principal, port.name AS port, transport, sessionCount, messagesIn, bytesIn, messagesOut, bytesOut",
                orderBy: "name",
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                columns: [
                {
                    field: "selected",
                    label: 'All',
                    selector: 'checkbox'
                }, {
                    label: "Name",
                    field: "name"
                }, {
                    label: "User",
                    field: "principal"
                }, {
                    label: "Port",
                    field: "port"
                }, {
                    label: "Transport",
                    field: "transport"
                }, {
                    label: "Sessions",
                    field: "sessionCount"
                }, {
                    label: "Msgs In",
                    field: "msgInRate"
                }, {
                    label: "Bytes In",
                    field: "bytesInRate"
                }, {
                    label: "Msgs Out",
                    field: "msgOutRate"
                }, {
                    label: "Bytes Out",
                    field: "bytesOutRate"
                }
                ]
            }, findNode("connections"));
            this.connectionsGrid.on('rowBrowsed', function(event){controller.showById(event.id);});
            this.connectionsGrid.startup();

            this.virtualHostLoggersGrid = new CustomGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: util.queryResultToObjects,
                management: this.management,
                parentObject: this.modelObj,
                category: "VirtualHostLogger",
                selectClause: "id, name, type, errorCount, warnCount",
                orderBy: "name",
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                columns: [
                    {
                        field: "selected",
                        label: 'All',
                        selector: 'checkbox'
                    },  {
                        label: "Name",
                        field: "name"
                    }, {
                        label: "Type",
                        field: "type"
                    }, {
                        label: "Errors",
                        field: "errorCount"
                    }, {
                        label: "Warnings",
                        field: "warnCount"
                    }
                ]
            }, findNode("loggers"));
            this.virtualHostLoggersGrid.on('rowBrowsed', function(event){controller.showById(event.id);});
            this.virtualHostLoggersGrid.startup();

            var Store = MemoryStore.createSubclass(TrackableStore);
            this._policyStore = new Store({
                data: [],
                idProperty: "pattern"
            });

            var PolicyGrid = declare([Grid, Keyboard, Selection, Pagination, ColumnResizer, DijitRegistry]);
            this._policyGrid = new PolicyGrid({
                rowsPerPage: 10,
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                cellNavigation: true,
                className: 'dgrid-autoheight',
                pageSizeOptions: [10, 20, 30, 40, 50, 100],
                adjustLastColumn: true,
                collection: this._policyStore,
                highlightRow: function (){},
                columns: [
                    {
                        label: 'Node Type',
                        field: "nodeType"
                    }, {
                        label: "Pattern",
                        field: "pattern"
                    }, {
                        label: "Create On Publish",
                        field: "createdOnPublish",
                        selector: 'checkbox'
                    }, {
                        label: "Create On Consume",
                        field: "createdOnConsume",
                        selector: 'checkbox'
                    }, {
                        label: "Attributes",
                        field: "attributes",
                        sortable: false,
                        formatter: function(value, object)
                        {
                            var markup = "";
                            if (value)
                            {
                                markup = "<div class='keyValuePair'>";
                                for(var key in value)
                                {
                                    markup += "<div>" + entities.encode(String(key)) + "="
                                              + entities.encode(String(value[key])) + "</div>";
                                }
                                markup += "</div>"
                            }
                            return markup;
                        }
                    }
                ]
            }, findNode("policies"));

            this._policyGrid.startup();
            this._nodeAutoCreationPolicies = registry.byNode(findNode("nodeAutoCreationPolicies"));

            aspect.after(this._nodeAutoCreationPolicies, "toggle", lang.hitch(this, function() {
                if (this._nodeAutoCreationPolicies.get("open") === true)
                {
                    this._policyGrid.refresh();
                }
            }));

            this.virtualHostAccessControlProviderGrid = new CustomGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: util.queryResultToObjects,
                management: this.management,
                parentObject: this.modelObj,
                category: "VirtualHostAccessControlProvider",
                selectClause: "id, name, state, type, priority",
                orderBy: "name",
                selectionMode: 'none',
                deselectOnRefresh: false,
                allowSelectAll: true,
                columns: [
                    {
                        field: "selected",
                        label: 'All',
                        selector: 'checkbox'
                    },  {
                        label: "Name",
                        field: "name"
                    }, {
                        label: "State",
                        field: "state"
                    }, {
                        label: "Type",
                        field: "type"
                    }, {
                        label: "Priority",
                        field: "priority"
                    }
                ]
            }, findNode("virtualHostAccessControlProviders"));
            this.virtualHostAccessControlProviderGrid.on('rowBrowsed', function(event){controller.showById(event.id);});
            this.virtualHostAccessControlProviderGrid.startup();

        }

        Updater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }
            this.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 0
                })
                .then(lang.hitch(this, function (data)
                {
                    this._updateFromData(data, callback);
                }), lang.hitch(this, function (error)
                {
                    util.tabErrorHandler(error,
                        {
                            updater: this,
                            contentPane: this.tabObject.contentPane,
                            tabContainer: this.tabObject.controller.tabContainer,
                            name: this.modelObj.name,
                            category: "Virtual Host"
                        });
                }));
        };

        Updater.prototype._updateFromData = function(data, callback)
        {
            this.vhostData = data || {name: this.modelObj.name};

            if (!this.virtualhostStatistics)
            {
                this.virtualhostStatistics = new StatisticsWidget({
                    category: "VirtualHost",
                    type: this.vhostData.type,
                    management: this.management,
                    defaultStatistics: ["messagesIn", "messagesOut", "totalDepthOfQueuesMessages"]
                });
                this.virtualhostStatistics.placeAt(this.virtualhostStatisticsNode);
                this.virtualhostStatistics.startup();
            }

            if (callback)
            {
                callback();
            }

            try
            {
                this._update();
            }
            catch (e)
            {
                if (console && console.error)
                {
                    console.error(e);
                }
            }
        };

        Updater.prototype._update = function ()
        {
            this.tabObject.startButton.set("disabled", !this.vhostData.state || this.vhostData.state !== "STOPPED");
            this.tabObject.stopButton.set("disabled", !this.vhostData.state || this.vhostData.state !== "ACTIVE");
            this.tabObject.editButton.set("disabled", !this.vhostData.state || this.vhostData.state === "UNAVAILABLE");
            this.tabObject.downloadButton.set("disabled", !this.vhostData.state || this.vhostData.state !== "ACTIVE");
            this.tabObject.deleteButton.set("disabled", !this.vhostData.state);

            this.virtualhostStatistics.update(this.vhostData.statistics);

            this._updateHeader();

            this.virtualHostChildren.style.display = this.vhostData.state === "ACTIVE" ? "block" : "none";

            if (this.vhostData.state === "ACTIVE")
            {
                this.connectionsGrid.updateData();
                this.queuesGrid.updateData();
                this.exchangesGrid.updateData();
                this.virtualHostLoggersGrid.updateData();
                this.virtualHostAccessControlProviderGrid.updateData();
            }

            if (this.details)
            {
                this.details.update(this.vhostData);
            }
            else
            {
                var thisObj = this;
                require(["qpid/management/virtualhost/" + this.vhostData.type.toLowerCase() + "/show"],
                    function (VirtualHostDetails)
                    {
                        thisObj.details = new VirtualHostDetails({
                            containerNode: thisObj.virtualHostDetailsContainer,
                            parent: thisObj
                        });
                        thisObj.details.update(thisObj.vhostData);
                    });
            }
            util.updateSyncDStore(this._policyStore, this.vhostData.nodeAutoCreationPolicies || [], "pattern");
        };

        Updater.prototype._updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.vhostData["name"]));
            this.type.innerHTML = entities.encode(String(this.vhostData["type"]));
            this.state.innerHTML = entities.encode(String(this.vhostData["state"]));
            this.durable.innerHTML = entities.encode(String(this.vhostData["durable"]));
            this.lifetimePolicy.innerHTML = entities.encode(String(this.vhostData["lifetimePolicy"]));
            util.updateUI(this.vhostData,
                ["housekeepingCheckPeriod",
                 "housekeepingThreadCount",
                 "storeTransactionIdleTimeoutClose",
                 "storeTransactionIdleTimeoutWarn",
                 "storeTransactionOpenTimeoutClose",
                 "storeTransactionOpenTimeoutWarn",
                 "statisticsReportingPeriod",
                 "connectionThreadPoolSize"],
                this)
        };

        Updater.prototype._transformConnectionData = function (data)
        {

            var sampleTime = new Date();
            var connections = util.queryResultToObjects(data);
            if (this._previousConnectionSampleTime)
            {
                var samplePeriod = sampleTime.getTime() - this._previousConnectionSampleTime.getTime();
                for (var i = 0; i < connections.length; i++)
                {
                    var connection = connections[i];
                    var oldConnection = null;
                    for (var j = 0; j < this._previousConsumers.length; j++)
                    {
                        if (this._previousConsumers[j].id === connection.id)
                        {
                            oldConnection = this._previousConsumers[j];
                            break;
                        }
                    }
                    var msgOutRate = 0;
                    var bytesOutRate = 0;
                    var bytesInRate = 0;
                    var msgInRate = 0;

                    if (oldConnection)
                    {
                        msgOutRate = (1000 * (connection.messagesOut - oldConnection.messagesOut))
                                         / samplePeriod;
                        bytesOutRate = (1000 * (connection.bytesOut - oldConnection.bytesOut)) / samplePeriod;
                        msgInRate = (1000 * (connection.messagesIn - oldConnection.messagesIn)) / samplePeriod;
                        bytesInRate = (1000 * (connection.bytesIn - oldConnection.bytesIn)) / samplePeriod;
                    }

                    connection.msgOutRate = msgOutRate.toFixed(0) + "msg/s";
                    var bytesOutRateFormat = formatter.formatBytes(bytesOutRate);
                    connection.bytesOutRate = bytesOutRateFormat.value + bytesOutRateFormat.units + "/s";
                    connection.msgInRate = msgInRate.toFixed(0) + "msg/s";
                    var bytesInRateFormat = formatter.formatBytes(bytesInRate);
                    connection.bytesInRate = bytesInRateFormat.value + bytesInRateFormat.units + "/s";
                }
            }
            this._previousConnectionSampleTime = sampleTime;
            this._previousConsumers = connections;
            return connections;
        };

        return VirtualHost;
    });
