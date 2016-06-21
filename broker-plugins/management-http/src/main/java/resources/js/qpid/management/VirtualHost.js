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
        "dojo/_base/connect",
        "dojo/_base/lang",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "qpid/management/addQueue",
        "qpid/management/addExchange",
        "qpid/management/addLogger",
        "qpid/management/query/QueryGrid",
        "dojox/grid/EnhancedGrid",
        "qpid/management/editVirtualHost",
        "dojo/text!showVirtualHost.html",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              lang,
              registry,
              entities,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              addQueue,
              addExchange,
              addLogger,
              QueryGrid,
              EnhancedGrid,
              editVirtualHost,
              template)
    {

        function VirtualHost(name, parent, controller)
        {
            this.name = name;
            this.controller = controller;
            this.management = controller.management;
            this.modelObj = {
                type: "virtualhost",
                name: name,
                parent: parent
            };
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
                    connect.connect(registry.byNode(addQueueButton), "onClick", function (evt)
                    {
                        addQueue.show(that.management, that.modelObj)
                    });

                    var deleteQueueButton = query(".deleteQueueButton", containerNode)[0];
                    connect.connect(registry.byNode(deleteQueueButton), "onClick", function (evt)
                    {
                        util.deleteSelectedObjects(that.vhostUpdater.queuesGrid.grid,
                            "Are you sure you want to delete queue",
                            that.management,
                            {
                                type: "queue",
                                parent: that.modelObj
                            },
                            that.vhostUpdater);
                    });

                    var addExchangeButton = query(".addExchangeButton", containerNode)[0];
                    connect.connect(registry.byNode(addExchangeButton), "onClick", function (evt)
                    {
                        addExchange.show(that.management, that.modelObj);
                    });

                    var deleteExchangeButton = query(".deleteExchangeButton", containerNode)[0];
                    connect.connect(registry.byNode(deleteExchangeButton), "onClick", function (evt)
                    {
                        util.deleteSelectedObjects(that.vhostUpdater.exchangesGrid.grid,
                            "Are you sure you want to delete exchange",
                            that.management,
                            {
                                type: "exchange",
                                parent: that.modelObj
                            },
                            that.vhostUpdater);
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
                        util.deleteSelectedObjects(that.vhostUpdater.virtualHostLoggersGrid.grid,
                            "Are you sure you want to delete virtual host logger",
                            that.management,
                            {
                                type: "virtualhostlogger",
                                parent: that.modelObj
                            },
                            that.vhostUpdater);
                    });

                    that.stopButton = registry.byNode(query(".stopButton", containerNode)[0]);
                    that.startButton = registry.byNode(query(".startButton", containerNode)[0]);
                    that.editButton = registry.byNode(query(".editButton", containerNode)[0]);
                    that.downloadButton = registry.byNode(query(".downloadButton", containerNode)[0]);
                    that.downloadButton.on("click", function (e)
                    {
                        var suggestedAttachmentName = encodeURIComponent(that.name + ".json");
                        that.management.download(that.modelObj, {
                            extractInitialConfig: true,
                            contentDispositionAttachmentFilename: suggestedAttachmentName
                        });
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

                    that.addQueryButton = registry.byNode(query(".addQuery", containerNode)[0]);
                    that.addQueryButton.on("click", function (e)
                    {
                        that.controller.show('queryTab', '', that.modelObj);
                    });

                    that.vhostUpdater.update(function ()
                    {
                        updater.add(that.vhostUpdater);
                    });
                });
        };

        VirtualHost.prototype.close = function ()
        {
            updater.remove(this.vhostUpdater);
        };

        VirtualHost.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose()
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        }

        function Updater(virtualHost)
        {
            var vhost = virtualHost.modelObj;
            var controller = virtualHost.controller;
            var node = virtualHost.contentPane.containerNode;

            this.tabObject = virtualHost;
            this.contentPane = virtualHost.contentPane;
            this.management = controller.management;
            this.modelObj = vhost;
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

            storeNodes(["name",
                        "type",
                        "state",
                        "durable",
                        "lifetimePolicy",
                        "msgInRate",
                        "bytesInRate",
                        "bytesInRateUnits",
                        "msgOutRate",
                        "bytesOutRate",
                        "bytesOutRateUnits",
                        "virtualHostDetailsContainer",
                        "deadLetterQueueEnabled",
                        "connectionThreadPoolSize",
                        "housekeepingCheckPeriod",
                        "housekeepingThreadCount",
                        "storeTransactionIdleTimeoutClose",
                        "storeTransactionIdleTimeoutWarn",
                        "storeTransactionOpenTimeoutClose",
                        "storeTransactionOpenTimeoutWarn",
                        "virtualHostConnections",
                        "virtualHostChildren"]);

            that.vhostData = {};

            var gridProperties = {
                keepSelection: true,
                plugins: {
                    pagination: {
                        pageSizes: [10, 25, 50, 100],
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    },
                    indirectSelection: true

                }
            };

            that.queuesGrid = new UpdatableStore([], findNode("queues"), [{
                name: "Name",
                field: "name",
                width: "30%"
            }, {
                name: "Type",
                field: "type",
                width: "20%"
            }, {
                name: "Consumers",
                field: "consumerCount",
                width: "10%"
            }, {
                name: "Depth (msgs)",
                field: "queueDepthMessages",
                width: "20%"
            }, {
                name: "Depth (bytes)",
                field: "queueDepthBytes",
                width: "20%",
                get: function (rowIndex, item)
                {
                    if (!item)
                    {
                        return;
                    }
                    var store = this.grid.store;
                    var qdb = store.getValue(item, "queueDepthBytes");
                    var bytesFormat = formatter.formatBytes(qdb);
                    return bytesFormat.value + " " + bytesFormat.units;
                }
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex, theItem = this.getItem(idx);
                    var queueName = obj.dataStore.getValue(theItem, "name");
                    controller.show("queue", queueName, vhost, theItem.id);
                });
            }, gridProperties, EnhancedGrid);

            that.exchangesGrid = new UpdatableStore([], findNode("exchanges"), [{
                name: "Name",
                field: "name",
                width: "50%"
            }, {
                name: "Type",
                field: "type",
                width: "30%"
            }, {
                name: "Binding Count",
                field: "bindingCount",
                width: "20%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex, theItem = this.getItem(idx);
                    var exchangeName = obj.dataStore.getValue(theItem, "name");
                    controller.show("exchange", exchangeName, vhost, theItem.id);
                });
            }, gridProperties, EnhancedGrid);

            this.connectionsGrid = new QueryGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: lang.hitch(this, this._transformConnectionData),
                management: this.management,
                controller: controller,
                parentObject: this.modelObj,
                category: "Connection",
                selectClause: "id, name, principal, port.name AS port, transport, sessionCount, messagesIn, bytesIn, messagesOut, bytesOut",
                orderBy: "name",
                columns: [{
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
            that.connectionsGrid.startup();

            that.virtualHostLoggersGrid = new UpdatableStore([], findNode("loggers"), [{
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
                name: "Errors",
                field: "errorCount",
                width: "10%"
            }, {
                name: "Warnings",
                field: "warnCount",
                width: "10%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex, theItem = this.getItem(idx);
                    var name = obj.dataStore.getValue(theItem, "name");
                    controller.show("virtualhostlogger", name, vhost, theItem.id);
                });
            }, gridProperties, EnhancedGrid);

        }

        Updater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.vhostData["name"]));
            this.type.innerHTML = entities.encode(String(this.vhostData["type"]));
            this.state.innerHTML = entities.encode(String(this.vhostData["state"]));
            this.durable.innerHTML = entities.encode(String(this.vhostData["durable"]));
            this.lifetimePolicy.innerHTML = entities.encode(String(this.vhostData["lifetimePolicy"]));
            this.deadLetterQueueEnabled.innerHTML =
                entities.encode(String(this.vhostData["queue.deadLetterQueueEnabled"]));
            util.updateUI(this.vhostData,
                ["housekeepingCheckPeriod",
                 "housekeepingThreadCount",
                 "storeTransactionIdleTimeoutClose",
                 "storeTransactionIdleTimeoutWarn",
                 "storeTransactionOpenTimeoutClose",
                 "storeTransactionOpenTimeoutWarn",
                 "connectionThreadPoolSize"],
                this)
        };

        Updater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var thisObj = this;

            thisObj.connectionsGrid.updateData();
            thisObj.connectionsGrid.resize();

            this.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 1
                })
                .then(function (data)
                {
                    thisObj.vhostData = data[0] || {
                            name: thisObj.modelObj.name,
                            statistics: {
                                messagesIn: 0,
                                bytesIn: 0,
                                messagesOut: 0,
                                bytesOut: 0
                            }
                        };

                    if (callback)
                    {
                        callback();
                    }

                    try
                    {
                        thisObj._update();
                    }
                    catch (e)
                    {
                        if (console && console.error)
                        {
                            console.error(e);
                        }
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: thisObj,
                        contentPane: thisObj.tabObject.contentPane,
                        tabContainer: thisObj.tabObject.controller.tabContainer,
                        name: thisObj.modelObj.name,
                        category: "Virtual Host"
                    });
                });
        };

        Updater.prototype._update = function ()
        {
            var thisObj = this;
            this.tabObject.startButton.set("disabled", !this.vhostData.state || this.vhostData.state != "STOPPED");
            this.tabObject.stopButton.set("disabled", !this.vhostData.state || this.vhostData.state != "ACTIVE");
            this.tabObject.editButton.set("disabled", !this.vhostData.state || this.vhostData.state == "UNAVAILABLE");
            this.tabObject.downloadButton.set("disabled", !this.vhostData.state || this.vhostData.state != "ACTIVE");
            this.tabObject.deleteButton.set("disabled", !this.vhostData.state);

            util.flattenStatistics(thisObj.vhostData);
            var queues = thisObj.vhostData["queues"];
            var exchanges = thisObj.vhostData["exchanges"];

            thisObj.updateHeader();

            var stats = thisObj.vhostData["statistics"];

            var sampleTime = new Date();
            var messageIn = stats["messagesIn"];
            var bytesIn = stats["bytesIn"];
            var messageOut = stats["messagesOut"];
            var bytesOut = stats["bytesOut"];

            if (thisObj.sampleTime)
            {
                var samplePeriod = sampleTime.getTime() - thisObj.sampleTime.getTime();

                var msgInRate = (1000 * (messageIn - thisObj.messageIn)) / samplePeriod;
                var msgOutRate = (1000 * (messageOut - thisObj.messageOut)) / samplePeriod;
                var bytesInRate = (1000 * (bytesIn - thisObj.bytesIn)) / samplePeriod;
                var bytesOutRate = (1000 * (bytesOut - thisObj.bytesOut)) / samplePeriod;

                thisObj.msgInRate.innerHTML = msgInRate.toFixed(0);
                var bytesInFormat = formatter.formatBytes(bytesInRate);
                thisObj.bytesInRate.innerHTML = "(" + bytesInFormat.value;
                thisObj.bytesInRateUnits.innerHTML = bytesInFormat.units + "/s)";

                thisObj.msgOutRate.innerHTML = msgOutRate.toFixed(0);
                var bytesOutFormat = formatter.formatBytes(bytesOutRate);
                thisObj.bytesOutRate.innerHTML = "(" + bytesOutFormat.value;
                thisObj.bytesOutRateUnits.innerHTML = bytesOutFormat.units + "/s)";
            }

            thisObj.sampleTime = sampleTime;
            thisObj.messageIn = messageIn;
            thisObj.bytesIn = bytesIn;
            thisObj.messageOut = messageOut;
            thisObj.bytesOut = bytesOut;

            this._updateGrids(thisObj.vhostData)

            if (thisObj.details)
            {
                thisObj.details.update(thisObj.vhostData);
            }
            else
            {
                require(["qpid/management/virtualhost/" + thisObj.vhostData.type.toLowerCase() + "/show"],
                    function (VirtualHostDetails)
                    {
                        thisObj.details = new VirtualHostDetails({
                            containerNode: thisObj.virtualHostDetailsContainer,
                            parent: thisObj
                        });
                        thisObj.details.update(thisObj.vhostData);
                    });
            }

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
                    for (var j = 0; j < this._previousConnections.length; j++)
                    {
                        if (this._previousConnections[j].id == connection.id)
                        {
                            oldConnection = this._previousConnections[j];
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
            this._previousConnections = connections;
            return connections;
        };

        Updater.prototype._updateGrids = function (data)
        {
            this.virtualHostChildren.style.display = data.state == "ACTIVE" ? "block" : "none";
            if (data.state == "ACTIVE")
            {
                util.updateUpdatableStore(this.queuesGrid, data.queues);
                util.updateUpdatableStore(this.exchangesGrid, data.exchanges);
                util.updateUpdatableStore(this.virtualHostLoggersGrid, data.virtualhostloggers);

                var exchangesGrid = this.exchangesGrid.grid;
                for (var i = 0; i < data.exchanges.length; i++)
                {
                    var item = exchangesGrid.getItem(i);
                    var isStandard = item && item.name && util.isReservedExchangeName(item.name);
                    exchangesGrid.rowSelectCell.setDisabled(i, isStandard);
                }
            }
        };

        return VirtualHost;
    });
