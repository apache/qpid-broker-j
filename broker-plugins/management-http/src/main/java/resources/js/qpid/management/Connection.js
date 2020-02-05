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
        "dijit/registry",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/StatisticsWidget",
        "qpid/management/query/QueryGrid",
        "dojox/html/entities",
        "dojo/text!showConnection.html",
        "dojo/domReady!"],
    function (parser,
              query,
              lang,
              registry,
              updater,
              util,
              formatter,
              StatisticsWidget,
              QueryGrid,
              entities,
              template)
    {

        function Connection(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        Connection.prototype.getTitle = function ()
        {
            return "Connection: " + this.name;
        };

        Connection.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            var containerNode = contentPane.containerNode;
            containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.connectionUpdater = new ConnectionUpdater(that);
                    that.connectionUpdater.update(function ()
                    {
                        updater.add(that.connectionUpdater);
                    });

                    that.closeButton = registry.byNode(query(".closeButton", containerNode)[0]);
                    that.closeButton.on("click", function (e)
                    {
                        if (confirm("Are you sure you want to close the connection?"))
                        {
                            that.management.remove(that.modelObj)
                                .then(function (result)
                                {
                                    that.destroy();
                                });
                        }
                    });

                });
        };

        Connection.prototype.close = function ()
        {
            updater.remove(this.connectionUpdater);
        };

        Connection.prototype.destroy = function ()
        {
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        function ConnectionUpdater(connectionTab)
        {
            var that = this;
            this.tabObject = connectionTab;
            this.contentPane = connectionTab.contentPane;
            this.controller = connectionTab.controller;
            this.management = connectionTab.controller.management;
            this.modelObj = connectionTab.modelObj;
            var containerNode = connectionTab.contentPane.containerNode;

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

            this.connectionStatisticsNode = findNode("connectionStatistics");

            storeNodes(["name",
                        "clientVersion",
                        "clientId",
                        "principal",
                        "port",
                        "transport",
                        "protocol",
                        "remoteProcessPid",
                        "createdTime",
                        "transportInfo",
                        "transportInfoContainer",
                        "sessionCountLimit"]);

            var userPreferences = this.management.userPreferences;

            this.connectionData = {};

            this.sessionsGrid = new QueryGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: util.queryResultToObjects,
                management: this.management,
                parentObject: this.modelObj,
                category: "Session",
                selectClause: "id,name,consumerCount,unacknowledgedMessages,transactionStartTime,transactionUpdateTime",
                where: "to_string($parent.id) = '" + this.modelObj.id + "'",
                orderBy: "name",
                columns: [
                    {
                        label: "Name",
                        field: "name"
                    }, {
                        label: "Consumers",
                        field: "consumerCount"
                    }, {
                        label: "Unacknowledged messages",
                        field: "unacknowledgedMessages"
                    }
                ]
            }, findNode("sessions"));
            this.sessionsGrid.on('rowBrowsed', lang.hitch(this, function(event){this.controller.showById(event.id);}));
            this.sessionsGrid.startup();

            this.consumersGrid = new QueryGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: lang.hitch(this, this._transformConsumerData),
                management: this.management,
                parentObject: this.modelObj,
                category: "Consumer",
                selectClause: "id, name, distributionMode, $parent.id AS queueId, $parent.name AS queueName, "
                              + "messagesOut, bytesOut, unacknowledgedMessages, unacknowledgedBytes",
                where: "to_string(session.$parent.id) = '" + this.modelObj.id + "'",
                orderBy: "name",
                columns: [{
                    label: "Name",
                    field: "name"
                }, {
                    label: "Mode",
                    field: "distributionMode"
                }, {
                    label: "Queue",
                    field: "queueName"
                }, {
                    label: "Unacknowledged (msgs)",
                    field: "unacknowledgedMessages"
                }, {
                    label: "Unacknowledged (bytes)",
                    field: "unacknowledgedBytes",
                    formatter: formatter.formatBytes
                }, {
                    label: "Msgs Rate",
                    field: "msgOutRate"
                }, {
                    label: "Bytes Rate",
                    field: "bytesOutRate"
                }
                ]
            }, findNode("consumers"));

            this.consumersGrid.on('rowBrowsed',
                lang.hitch(this, function(event)
                {
                    var queueId = this.consumersGrid.row(event.id).data.queueId;
                    this.controller.showById(queueId);
                }));
            this.consumersGrid.startup();

            // Add onShow handler to work around an issue with not rendering of grid columns before first update.
            // It seems if dgrid is created when tab is not shown (not active) the grid columns are not rendered.
            this.contentPane.on("show",
                lang.hitch(this, function()
                {
                    this.consumersGrid.resize();
                    this.sessionsGrid.resize();
                }));
        }

        ConnectionUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            this.management.load(this.modelObj, { excludeInheritedContext: true, depth: 0})
                .then(lang.hitch(this, function (data)
                {
                    this.connectionData = data;

                    if (!this.connectionStatistics)
                    {
                        this.connectionStatistics = new StatisticsWidget({
                            category:  "Connection",
                            type: null,
                            management: this.management,
                            defaultStatistics: ["messagesIn", "messagesOut", "lastIoTime"]
                        });
                        this.connectionStatistics.placeAt(this.connectionStatisticsNode);
                        this.connectionStatistics.startup();
                    }

                    if (callback)
                    {
                        callback();
                    }

                    this._updateHeader();
                    this.connectionStatistics.update(this.connectionData.statistics);
                    this.connectionStatistics.resize();
                    this.consumersGrid.updateData();
                    this.sessionsGrid.updateData();
                }), lang.hitch(this, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: this,
                        contentPane: this.tabObject.contentPane,
                        tabContainer: this.tabObject.controller.tabContainer,
                        name: this.modelObj.name,
                        category: "Connection"
                    });
                }));
        };


        ConnectionUpdater.prototype._updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.connectionData["name"]));
            this.clientId.innerHTML = entities.encode(String(this.connectionData["clientId"]));
            this.clientVersion.innerHTML = entities.encode(String(this.connectionData["clientVersion"]));
            this.principal.innerHTML = entities.encode(String(this.connectionData["principal"]));
            this.port.innerHTML = entities.encode(String(this.connectionData["port"]));
            this.transport.innerHTML = entities.encode(String(this.connectionData["transport"]));
            this.protocol.innerHTML = entities.encode(String(this.connectionData["protocol"]));
            var transportInfo = entities.encode(String(this.connectionData["transportInfo"]));
            this.transportInfo.innerHTML = transportInfo;
            this.transportInfoContainer.style.display = transportInfo === "" ? "none": "";
            var remoteProcessPid = this.connectionData["remoteProcessPid"];
            this.remoteProcessPid.innerHTML = entities.encode(String(remoteProcessPid ? remoteProcessPid : "N/A"));
            this.sessionCountLimit.innerHTML = entities.encode(String(this.connectionData["sessionCountLimit"]));

            var userPreferences = this.management.userPreferences;
            this.createdTime.innerHTML = userPreferences.formatDateTime(this.connectionData["createdTime"], {
                addOffset: true,
                appendTimeZone: true
            });
        };

        ConnectionUpdater.prototype._transformConsumerData = function (data)
        {
            var sampleTime = new Date();
            var consumers = util.queryResultToObjects(data);
            if (this._previousConsumerSampleTime)
            {
                var samplePeriod = sampleTime.getTime() - this._previousConsumerSampleTime.getTime();
                for (var i = 0; i < consumers.length; i++)
                {
                    var consumer = consumers[i];
                    var oldConsumer = null;
                    for (var j = 0; j < this._previousConsumers.length; j++)
                    {
                        if (this._previousConsumers[j].id === consumer.id)
                        {
                            oldConsumer = this._previousConsumers[j];
                            break;
                        }
                    }
                    var msgOutRate = 0;
                    var bytesOutRate = 0;

                    if (oldConsumer)
                    {
                        msgOutRate = (1000 * (consumer.messagesOut - oldConsumer.messagesOut))
                                     / samplePeriod;
                        bytesOutRate = (1000 * (consumer.bytesOut - oldConsumer.bytesOut)) / samplePeriod;
                    }

                    consumer.msgOutRate = msgOutRate.toFixed(0) + " msg/s";
                    var bytesOutRateFormat = formatter.formatBytes(bytesOutRate);
                    consumer.bytesOutRate = bytesOutRateFormat.value + " " + bytesOutRateFormat.units + "/s";
                }
            }
            this._previousConsumerSampleTime = sampleTime;
            this._previousConsumers = consumers;
            return consumers;
        };

        return Connection;
    });
