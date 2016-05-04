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
        "dijit/registry",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "dojox/html/entities",
        "dojo/text!showConnection.html",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              registry,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              entities,
              template)
    {

        function Connection(name, parent, controller)
        {
            this.name = name;
            this.controller = controller;
            this.management = controller.management;
            this.modelObj = {
                type: "connection",
                name: name,
                parent: parent
            };
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

            var that = this;

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

        return Connection;
    });
