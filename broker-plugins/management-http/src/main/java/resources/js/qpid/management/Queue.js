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
define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojo/parser",
        "dojo/query",
        "dijit/registry",
        "dojo/_base/connect",
        "dojo/_base/event",
        "dojo/json",
        "dojo/promise/all",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "qpid/management/addBinding",
        "qpid/management/moveCopyMessages",
        "qpid/management/showMessage",
        "qpid/management/editQueue",
        "qpid/common/JsonRest",
        "dojox/grid/EnhancedGrid",
        "dojo/data/ObjectStore",
        "dojox/html/entities",
        "dojo/text!showQueue.html",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojo/domReady!"],
    function (declare,
              lang,
              parser,
              query,
              registry,
              connect,
              event,
              json,
              all,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              addBinding,
              moveMessages,
              showMessage,
              editQueue,
              JsonRest,
              EnhancedGrid,
              ObjectStore,
              entities,
              template)
    {

        function Queue(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        Queue.prototype.getQueueName = function ()
        {
            return this.name;
        };

        Queue.prototype.getVirtualHostName = function ()
        {
            return this.modelObj.parent.name;
        };

        Queue.prototype.getVirtualHostNodeName = function ()
        {
            return this.modelObj.parent.parent.name;
        };

        Queue.prototype.getTitle = function ()
        {
            return "Queue: " + this.name;
        };

        Queue.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;

            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.queueUpdater = new QueueUpdater(that);

                    // double encode to allow slashes in object names.
                    var myStore = new JsonRest({
                        management: that.management,
                        modelObject: that.modelObj,
                        queryOperation: "getMessageInfo",
                        queryParams: {includeHeaders: false},
                        totalRetriever: function ()
                        {
                            if (that.queueUpdater.queueData && that.queueUpdater.queueData.queueDepthMessages != undefined)
                            {
                                return that.queueUpdater.queueData.queueDepthMessages;
                            }
                            return that.management.query(
                                {
                                    parent: that.modelObj.parent,
                                    category: "queue",
                                    select: "queueDepthMessages",
                                    where: "name='" + that.modelObj.name.replace(/'/g, "'''") + "'"
                                })
                                .then(function (data)
                                {
                                    return data && data.results && data.results[0] ? data.results[0][0] : 0;
                                }, function (error)
                                {
                                    return undefined;
                                });
                        }
                    });
                    var messageGridDiv = query(".messages", contentPane.containerNode)[0];
                    that.dataStore = new ObjectStore({objectStore: myStore});
                    var userPreferences = this.management.userPreferences;
                    that.grid = new EnhancedGrid({
                        store: that.dataStore,
                        autoHeight: 10,
                        keepSelection: true,
                        structure: [{
                            name: "Size",
                            field: "size",
                            width: "40%"
                        }, {
                            name: "State",
                            field: "state",
                            width: "30%"
                        },

                            {
                                name: "Arrival",
                                field: "arrivalTime",
                                width: "30%",
                                formatter: function (val)
                                {
                                    return userPreferences.formatDateTime(val, {
                                        addOffset: true,
                                        appendTimeZone: true
                                    });
                                }
                            }],
                        plugins: {
                            pagination: {
                                pageSizes: ["10", "25", "50", "100"],
                                description: true,
                                sizeSwitch: true,
                                pageStepper: true,
                                gotoButton: true,
                                maxPageStep: 4,
                                position: "bottom"
                            },
                            indirectSelection: true
                        },
                        canSort: function (col)
                        {
                            return false;
                        }
                    }, messageGridDiv);

                    connect.connect(that.grid, "onRowDblClick", that.grid, function (evt)
                    {
                        var idx = evt.rowIndex, theItem = this.getItem(idx);
                        showMessage.show(that.management, that.modelObj, theItem);
                    });

                    var deleteMessagesButton = query(".deleteMessagesButton", contentPane.containerNode)[0];
                    var deleteWidget = registry.byNode(deleteMessagesButton);
                    connect.connect(deleteWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.deleteMessages();
                    });
                    var clearQueueButton = query(".clearQueueButton", contentPane.containerNode)[0];
                    var clearQueueWidget = registry.byNode(clearQueueButton);
                    connect.connect(clearQueueWidget, "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.clearQueue();
                    });
                    var moveMessagesButton = query(".moveMessagesButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(moveMessagesButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.moveOrCopyMessages({move: true});
                    });

                    var copyMessagesButton = query(".copyMessagesButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(copyMessagesButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.moveOrCopyMessages({move: false});
                    });

                    var refreshMessagesButton =  query(".refreshMessagesButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(refreshMessagesButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.reloadGridData();
                    });

                    var addBindingButton = query(".addBindingButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(addBindingButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        addBinding.show(that.management, that.modelObj);
                    });

                    var deleteQueueButton = query(".deleteQueueButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(deleteQueueButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        that.deleteQueue();
                    });
                    var editQueueButton = query(".editQueueButton", contentPane.containerNode)[0];
                    connect.connect(registry.byNode(editQueueButton), "onClick", function (evt)
                    {
                        event.stop(evt);
                        editQueue.show(that.management, that.modelObj);
                    });
                    that.queueUpdater.update(function ()
                    {
                        updater.add(that.queueUpdater);
                    });
                });
        };

        Queue.prototype.deleteMessages = function ()
        {
            var data = this.grid.selection.getSelected();
            if (data.length)
            {
                if (confirm("Delete " + data.length + " messages?"))
                {
                    var modelObj = {
                        type: "queue",
                        name: "deleteMessages",
                        parent: this.modelObj
                    };
                    var parameters = {messageIds: []};
                    for (var i = 0; i < data.length; i++)
                    {
                        parameters.messageIds.push(data[i].id);
                    }

                    this.management.update(modelObj, parameters)
                        .then(lang.hitch(this, function (result)
                        {
                            this.grid.selection.deselectAll();
                            this.reloadGridData();
                        }));
                }
            }
        };
        Queue.prototype.clearQueue = function ()
        {
            if (confirm("Clear all messages from queue?"))
            {
                var modelObj = {
                    type: "queue",
                    name: "clearQueue",
                    parent: this.modelObj
                };
                this.management.update(modelObj, {}).then(lang.hitch(this, this.reloadGridData));
            }
        };
        Queue.prototype.refreshMessages = function ()
        {
            var currentPage = this.grid.pagination.currentPage;
            var currentPageSize = this.grid.pagination.currentPageSize;
            var first = (currentPage - 1 ) * currentPageSize;
            var last = currentPage * currentPageSize;
            this.grid.setQuery({
                first: first,
                last: last
            });
        };
        Queue.prototype.reloadGridData = function ()
        {
            this.queueUpdater.update(lang.hitch(this, this.refreshMessages));
        };
        Queue.prototype.moveOrCopyMessages = function (obj)
        {
            var that = this;
            var move = obj.move;
            var data = this.grid.selection.getSelected();
            if (data.length)
            {
                var that = this;
                var i, putData = {messages: []};
                if (move)
                {
                    putData.move = true;
                }
                for (i = 0; i < data.length; i++)
                {
                    putData.messages.push(data[i].id);
                }
                moveMessages.show(that.management, that.modelObj, putData, function ()
                {
                    if (move)
                    {
                        that.reloadGridData();
                    }
                });

            }

        };

        Queue.prototype.startup = function ()
        {
            this.grid.startup();
        };

        Queue.prototype.close = function ()
        {
            updater.remove(this.queueUpdater);
        };

        var queueTypeKeys = {
            priority: "priorities",
            lvq: "lvqKey",
            sorted: "sortKey"
        };

        var queueTypeKeyNames = {
            priority: "Number of priorities",
            lvq: "LVQ key",
            sorted: "Sort key"
        };

        function QueueUpdater(tabObject)
        {
            var that = this;
            this.management = tabObject.management;
            this.modelObj = tabObject.modelObj;
            this.tabObject = tabObject;
            this.contentPane = tabObject.contentPane;
            var containerNode = tabObject.contentPane.containerNode;

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
                        "state",
                        "durable",
                        "messageDurability",
                        "maximumMessageTtl",
                        "minimumMessageTtl",
                        "exclusive",
                        "owner",
                        "lifetimePolicy",
                        "type",
                        "typeQualifier",
                        "alertRepeatGap",
                        "alertRepeatGapUnits",
                        "alertThresholdMessageAge",
                        "alertThresholdMessageAgeUnits",
                        "alertThresholdMessageSize",
                        "alertThresholdMessageSizeUnits",
                        "alertThresholdQueueDepthBytes",
                        "alertThresholdQueueDepthBytesUnits",
                        "alertThresholdQueueDepthMessages",
                        "alternateExchange",
                        "messageGroups",
                        "messageGroupKey",
                        "messageGroupSharedGroups",
                        "queueDepthMessages",
                        "queueDepthBytes",
                        "queueDepthBytesUnits",
                        "unacknowledgedMessages",
                        "unacknowledgedBytes",
                        "unacknowledgedBytesUnits",
                        "msgInRate",
                        "bytesInRate",
                        "bytesInRateUnits",
                        "msgOutRate",
                        "bytesOutRate",
                        "bytesOutRateUnits",
                        "queueFlowResumeSizeBytes",
                        "queueFlowControlSizeBytes",
                        "maximumDeliveryAttempts",
                        "holdOnPublishEnabled",
                        "oldestMessageAge"]);

            that.queueData = {};
            that.bindingsGrid = new UpdatableStore([], findNode("bindings"), [{
                name: "Binding Key",
                field: "bindingKey",
                width: "60%"
            }, {
                name: "Arguments",
                field: "arguments",
                width: "40%",
                formatter: function (arguments)
                {
                    return arguments ? json.stringify(arguments) : ""
                }
            }]);

            that.consumersGrid = new UpdatableStore([], findNode("consumers"), [{
                name: "Name",
                field: "name",
                width: "40%"
            }, {
                name: "Mode",
                field: "distributionMode",
                width: "20%"
            }, {
                name: "Msgs Rate",
                field: "msgRate",
                width: "20%"
            }, {
                name: "Bytes Rate",
                field: "bytesRate",
                width: "20%"
            }]);

        }

        QueueUpdater.prototype.updateHeader = function ()
        {

            var bytesDepth;
            this.name.innerHTML = entities.encode(String(this.queueData["name"]));
            this.state.innerHTML = entities.encode(String(this.queueData["state"]));
            this.durable.innerHTML = entities.encode(String(this.queueData["durable"]));
            this.exclusive.innerHTML = entities.encode(String(this.queueData["exclusive"]));
            this.owner.innerHTML = this.queueData["owner"] ? entities.encode(String(this.queueData["owner"])) : "";
            this.lifetimePolicy.innerHTML = entities.encode(String(this.queueData["lifetimePolicy"]));
            this.messageDurability.innerHTML = entities.encode(String(this.queueData["messageDurability"]));
            this.minimumMessageTtl.innerHTML = entities.encode(String(this.queueData["minimumMessageTtl"]));
            this.maximumMessageTtl.innerHTML = entities.encode(String(this.queueData["maximumMessageTtl"]));

            this.alternateExchange.innerHTML =
                this.queueData["alternateExchange"] ? entities.encode(String(this.queueData["alternateExchange"])) : "";

            this.queueDepthMessages.innerHTML = entities.encode(String(this.queueData["queueDepthMessages"]));
            bytesDepth = formatter.formatBytes(this.queueData["queueDepthBytes"]);
            this.queueDepthBytes.innerHTML = "(" + bytesDepth.value;
            this.queueDepthBytesUnits.innerHTML = bytesDepth.units + ")";

            this.unacknowledgedMessages.innerHTML = entities.encode(String(this.queueData["unacknowledgedMessages"]));
            bytesDepth = formatter.formatBytes(this.queueData["unacknowledgedBytes"]);
            this.unacknowledgedBytes.innerHTML = "(" + bytesDepth.value;
            this.unacknowledgedBytesUnits.innerHTML = bytesDepth.units + ")";
            this["type"].innerHTML = entities.encode(this.queueData["type"]);
            if (this.queueData["type"] == "standard")
            {
                this.typeQualifier.style.display = "none";
            }
            else
            {
                this.typeQualifier.innerHTML = entities.encode("(" + queueTypeKeyNames[this.queueData["type"]] + ": "
                                                               + this.queueData[queueTypeKeys[this.queueData["type"]]]
                                                               + ")");
            }

            if (this.queueData["messageGroupKey"])
            {
                this.messageGroupKey.innerHTML = entities.encode(String(this.queueData["messageGroupKey"]));
                this.messageGroupSharedGroups.innerHTML =
                    entities.encode(String(this.queueData["messageGroupSharedGroups"]));
                this.messageGroups.style.display = "block";
            }
            else
            {
                this.messageGroups.style.display = "none";
            }

            this.queueFlowControlSizeBytes.innerHTML =
                entities.encode(String(this.queueData["queueFlowControlSizeBytes"]));
            this.queueFlowResumeSizeBytes.innerHTML =
                entities.encode(String(this.queueData["queueFlowResumeSizeBytes"]));

            this.oldestMessageAge.innerHTML = entities.encode(String(this.queueData["oldestMessageAge"] / 1000));
            var maximumDeliveryAttempts = this.queueData["maximumDeliveryAttempts"];
            this.maximumDeliveryAttempts.innerHTML =
                entities.encode(String(maximumDeliveryAttempts == 0 ? "" : maximumDeliveryAttempts));
            this.holdOnPublishEnabled.innerHTML = entities.encode(String(this.queueData["holdOnPublishEnabled"]));
        };

        QueueUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var thisObj = this;

            var queuePromise = this.management.load(this.modelObj, {excludeInheritedContext: true, depth: 1 });
            var publishingLinkPromise = this.management.load({type: "queue", name: "getPublishingLinks", parent: this.modelObj});

            all({queue: queuePromise, publishingLinks: publishingLinkPromise})
                .then(function (data)
                {
                    var i, j;
                    thisObj.queueData = data.queue[0];
                    util.flattenStatistics(thisObj.queueData);
                    if (callback)
                    {
                        callback();
                    }
                    var bindings = thisObj.queueData["bindings"];
                    var consumers = thisObj.queueData["consumers"];

                    var bindings = data.publishingLinks || [];
                    for (i = 0; i < bindings.length; i++)
                    {
                        bindings[i].id = i;
                    }
                    thisObj.updateHeader();

                    // update alerting info
                    var alertRepeatGap = formatter.formatTime(thisObj.queueData["alertRepeatGap"]);

                    thisObj.alertRepeatGap.innerHTML = alertRepeatGap.value;
                    thisObj.alertRepeatGapUnits.innerHTML = alertRepeatGap.units;

                    var alertMsgAge = formatter.formatTime(thisObj.queueData["alertThresholdMessageAge"]);

                    thisObj.alertThresholdMessageAge.innerHTML = alertMsgAge.value;
                    thisObj.alertThresholdMessageAgeUnits.innerHTML = alertMsgAge.units;

                    var alertMsgSize = formatter.formatBytes(thisObj.queueData["alertThresholdMessageSize"]);

                    thisObj.alertThresholdMessageSize.innerHTML = alertMsgSize.value;
                    thisObj.alertThresholdMessageSizeUnits.innerHTML = alertMsgSize.units;

                    var alertQueueDepth = formatter.formatBytes(thisObj.queueData["alertThresholdQueueDepthBytes"]);

                    thisObj.alertThresholdQueueDepthBytes.innerHTML = alertQueueDepth.value;
                    thisObj.alertThresholdQueueDepthBytesUnits.innerHTML = alertQueueDepth.units;

                    thisObj.alertThresholdQueueDepthMessages.innerHTML =
                        entities.encode(String(thisObj.queueData["alertThresholdQueueDepthMessages"]));

                    var sampleTime = new Date();
                    var messageIn = thisObj.queueData["totalEnqueuedMessages"];
                    var bytesIn = thisObj.queueData["totalEnqueuedBytes"];
                    var messageOut = thisObj.queueData["totalDequeuedMessages"];
                    var bytesOut = thisObj.queueData["totalDequeuedBytes"];

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

                        if (consumers && thisObj.consumers)
                        {
                            for (i = 0; i < consumers.length; i++)
                            {
                                var consumer = consumers[i];
                                for (j = 0; j < thisObj.consumers.length; j++)
                                {
                                    var oldConsumer = thisObj.consumers[j];
                                    if (oldConsumer.id == consumer.id)
                                    {
                                        var msgRate = (1000 * (consumer.messagesOut - oldConsumer.messagesOut))
                                                      / samplePeriod;
                                        consumer.msgRate = msgRate.toFixed(0) + "msg/s";

                                        var bytesRate = (1000 * (consumer.bytesOut - oldConsumer.bytesOut))
                                                        / samplePeriod;
                                        var bytesRateFormat = formatter.formatBytes(bytesRate);
                                        consumer.bytesRate = bytesRateFormat.value + bytesRateFormat.units + "/s";
                                    }
                                }
                            }
                        }

                    }

                    thisObj.sampleTime = sampleTime;
                    thisObj.messageIn = messageIn;
                    thisObj.bytesIn = bytesIn;
                    thisObj.messageOut = messageOut;
                    thisObj.bytesOut = bytesOut;
                    thisObj.consumers = consumers;

                    // update bindings
                    thisObj.bindingsGrid.update(bindings);

                    // update consumers
                    thisObj.consumersGrid.update(thisObj.queueData.consumers)

                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: thisObj,
                        contentPane: thisObj.tabObject.contentPane,
                        tabContainer: thisObj.tabObject.controller.tabContainer,
                        name: thisObj.modelObj.name,
                        category: "Queue"
                    });
                });
        };

        Queue.prototype.deleteQueue = function ()
        {
            if (confirm("Are you sure you want to delete queue '" + this.name + "'?"))
            {
                var that = this;
                this.management.remove(this.modelObj)
                    .then(function (data)
                    {
                        that.contentPane.onClose();
                        that.controller.tabContainer.removeChild(that.contentPane);
                        that.contentPane.destroyRecursive();
                    }, util.xhrErrorHandler);
            }
        };

        return Queue;
    });
