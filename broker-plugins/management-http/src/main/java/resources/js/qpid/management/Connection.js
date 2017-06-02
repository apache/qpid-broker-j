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
        "dojo/_base/connect",
        "dijit/registry",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "qpid/management/query/QueryGrid",
        "dojox/html/entities",
        "dojo/text!showConnection.html",
        "dojo/domReady!"],
    function (parser,
              query,
              lang,
              connect,
              registry,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
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

            storeNodes(["name",
                        "clientVersion",
                        "clientId",
                        "principal",
                        "port",
                        "transport",
                        "protocol",
                        "remoteProcessPid",
                        "createdTime",
                        "lastIoTime",
                        "msgInRate",
                        "bytesInRate",
                        "bytesInRateUnits",
                        "msgOutRate",
                        "bytesOutRate",
                        "bytesOutRateUnits"]);

            var userPreferences = this.management.userPreferences;

            that.connectionData = {};
            that.sessionsGrid = new UpdatableStore([], findNode("sessions"), [{
                name: "Name",
                field: "name",
                width: "20%"
            }, {
                name: "Consumers",
                field: "consumerCount",
                width: "15%"
            }, {
                name: "Unacknowledged messages",
                field: "unacknowledgedMessages",
                width: "15%"
            }, {
                name: "Current store transaction start",
                field: "transactionStartTime",
                width: "25%",
                formatter: function (transactionStartTime)
                {
                    if (transactionStartTime > 0)
                    {
                        return userPreferences.formatDateTime(transactionStartTime, {
                            selector: "time",
                            addOffset: true,
                            appendTimeZone: true
                        });
                    }
                    else
                    {
                        return "N/A";
                    }
                }
            }, {
                name: "Current store transaction update",
                field: "transactionUpdateTime",
                width: "25%",
                formatter: function (transactionUpdateTime)
                {
                    if (transactionUpdateTime > 0)
                    {
                        return userPreferences.formatDateTime(transactionUpdateTime, {
                            selector: "time",
                            addOffset: true,
                            appendTimeZone: true
                        });
                    }
                    else
                    {
                        return "N/A";
                    }
                }
            }]);

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
                function()
                {
                    that.consumersGrid.resize();
                });
        }

        ConnectionUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.connectionData["name"]));
            this.clientId.innerHTML = entities.encode(String(this.connectionData["clientId"]));
            this.clientVersion.innerHTML = entities.encode(String(this.connectionData["clientVersion"]));
            this.principal.innerHTML = entities.encode(String(this.connectionData["principal"]));
            this.port.innerHTML = entities.encode(String(this.connectionData["port"]));
            this.transport.innerHTML = entities.encode(String(this.connectionData["transport"]));
            this.protocol.innerHTML = entities.encode(String(this.connectionData["protocol"]));
            var remoteProcessPid = this.connectionData["remoteProcessPid"];
            this.remoteProcessPid.innerHTML = entities.encode(String(remoteProcessPid ? remoteProcessPid : "N/A"));
            var userPreferences = this.management.userPreferences;
            this.createdTime.innerHTML = userPreferences.formatDateTime(this.connectionData["createdTime"], {
                addOffset: true,
                appendTimeZone: true
            });
            this.lastIoTime.innerHTML = userPreferences.formatDateTime(this.connectionData["lastIoTime"], {
                addOffset: true,
                appendTimeZone: true
            });
        };

        ConnectionUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;

            this.consumersGrid.updateData();
            this.consumersGrid.resize();

            that.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 1
                })
                .then(function (data)
                {
                    that.connectionData = data[0];

                    util.flattenStatistics(that.connectionData);

                    if (callback)
                    {
                        callback();
                    }

                    var sessions = that.connectionData["sessions"];

                    that.updateHeader();

                    var sampleTime = new Date();
                    var messageIn = that.connectionData["messagesIn"];
                    var bytesIn = that.connectionData["bytesIn"];
                    var messageOut = that.connectionData["messagesOut"];
                    var bytesOut = that.connectionData["bytesOut"];

                    if (that.sampleTime)
                    {
                        var samplePeriod = sampleTime.getTime() - that.sampleTime.getTime();

                        var msgInRate = (1000 * (messageIn - that.messageIn)) / samplePeriod;
                        var msgOutRate = (1000 * (messageOut - that.messageOut)) / samplePeriod;
                        var bytesInRate = (1000 * (bytesIn - that.bytesIn)) / samplePeriod;
                        var bytesOutRate = (1000 * (bytesOut - that.bytesOut)) / samplePeriod;

                        that.msgInRate.innerHTML = msgInRate.toFixed(0);
                        var bytesInFormat = formatter.formatBytes(bytesInRate);
                        that.bytesInRate.innerHTML = "(" + bytesInFormat.value;
                        that.bytesInRateUnits.innerHTML = bytesInFormat.units + "/s)";

                        that.msgOutRate.innerHTML = msgOutRate.toFixed(0);
                        var bytesOutFormat = formatter.formatBytes(bytesOutRate);
                        that.bytesOutRate.innerHTML = "(" + bytesOutFormat.value;
                        that.bytesOutRateUnits.innerHTML = bytesOutFormat.units + "/s)";

                        if (sessions && that.sessions)
                        {
                            for (var i = 0; i < sessions.length; i++)
                            {
                                var session = sessions[i];
                                for (var j = 0; j < that.sessions.length; j++)
                                {
                                    var oldSession = that.sessions[j];
                                    if (oldSession.id == session.id)
                                    {
                                        var msgRate = (1000 * (session.messagesOut - oldSession.messagesOut))
                                                      / samplePeriod;
                                        session.msgRate = msgRate.toFixed(0) + "msg/s";

                                        var bytesRate = (1000 * (session.bytesOut - oldSession.bytesOut))
                                                        / samplePeriod;
                                        var bytesRateFormat = formatter.formatBytes(bytesRate);
                                        session.bytesRate = bytesRateFormat.value + bytesRateFormat.units + "/s";
                                    }

                                }

                            }
                        }

                    }

                    that.sampleTime = sampleTime;
                    that.messageIn = messageIn;
                    that.bytesIn = bytesIn;
                    that.messageOut = messageOut;
                    that.bytesOut = bytesOut;
                    that.sessions = sessions;

                    // update sessions
                    that.sessionsGrid.update(that.connectionData.sessions)
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Connection"
                    });
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
                        if (this._previousConsumers[j].id == consumer.id)
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
