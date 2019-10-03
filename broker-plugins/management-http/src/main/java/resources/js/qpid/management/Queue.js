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
        "qpid/management/addQueue",
        "qpid/management/query/QueryGrid",
        "qpid/common/StatisticsWidget",
        "dojox/html/entities",
        "dojo/text!showQueue.html",
        "dgrid/Grid",
        "dgrid/Selector",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/DijitRegistry",
        "dojo/aspect",
        "qpid/management/MessageStore",
        "qpid/management/query/StoreUpdater",
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
              addQueue,
              QueryGrid,
              StatisticsWidget,
              entities,
              template,
              Grid,
              Selector,
              Keyboard,
              Selection,
              Pagination,
              ColumnResizer,
              DijitRegistry,
              aspect,
              MessageStore,
              StoreUpdater)
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
                        addQueue.show(that.management, that.modelObj, that.queueUpdater.queueData);
                    });
                    that.queueUpdater.update(function ()
                    {
                        updater.add(that.queueUpdater);
                    });
                });
        };

        Queue.prototype.deleteMessages = function ()
        {
            var selected = this.queueUpdater.getSelectedMessageIDs();
            if (selected.length > 0)
            {
                var confirmationRequest = "Delete " + selected.length + " message" + (selected.length > 1 ? "s" : "" )+ "?";
                if (confirm(confirmationRequest))
                {
                    var modelObj = {
                        type: "queue",
                        name: "deleteMessages",
                        parent: this.modelObj
                    };
                    this.management.update(modelObj, {messageIds: selected})
                        .then(lang.hitch(this, function ()
                        {
                            this.queueUpdater.clearMessageSelection();
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
                this.management.update(modelObj, {}).then(lang.hitch(this, function ()
                {
                    this.queueUpdater.clearMessageSelection();
                    this.reloadGridData();
                }));
            }
        };
        Queue.prototype.refreshMessages = function ()
        {
            this.queueUpdater.refreshMessages();
        };
        Queue.prototype.reloadGridData = function ()
        {
            this.queueUpdater.update(lang.hitch(this, this.refreshMessages));
        };
        Queue.prototype.moveOrCopyMessages = function (obj)
        {
            var move = obj.move;
            var data = this.queueUpdater.getSelectedMessageIDs();
            if (data.length > 0)
            {
                var putData = {messages: data};
                if (move)
                {
                    putData.move = true;
                }

                moveMessages.show(this.management, this.modelObj, putData, lang.hitch(this, function ()
                {
                    if (move)
                    {
                        this.queueUpdater.clearMessageSelection();
                        this.reloadGridData();
                    }
                }));

            }

        };

        Queue.prototype.startup = function ()
        {
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
            this.controller = tabObject.controller;
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

            this.queueStatisticsNode = findNode("queueStatistics");

            storeNodes(["name",
                        "state",
                        "durable",
                        "messageDurability",
                        "maximumMessageTtl",
                        "minimumMessageTtl",
                        "exclusive",
                        "owner",
                        "lifetimePolicy",
                        "overflowPolicy",
                        "maximumQueueDepth",
                        "maximumQueueDepthBytes",
                        "maximumQueueDepthBytesUnits",
                        "maximumQueueDepthMessages",
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
                        "alternateBinding",
                        "messageGroups",
                        "messageGroupKeyOverride",
                        "messageGroupType",
                        "maximumDeliveryAttempts",
                        "holdOnPublishEnabled"]);

            that.queueData = {};
            that.bindingsGrid = new UpdatableStore([], findNode("bindings"), [{
                name: "Binding Key",
                field: "bindingKey",
                width: "60%"
            }, {
                name: "Arguments",
                field: "arguments",
                width: "40%",
                formatter: function (args)
                {
                    return args ? json.stringify(args) : "";
                }
            }]);

            this.consumersGrid = new QueryGrid({
                detectChanges: true,
                rowsPerPage: 10,
                transformer: lang.hitch(this, this._transformConsumerData),
                management: this.management,
                parentObject: this.modelObj,
                category: "Consumer",
                selectClause: "id, name, distributionMode, "
                              + "session.$parent.id AS connectionId, session.$parent.name AS connectionName, session.$parent.principal AS connectionPrincipal,"
                              + "messagesOut, bytesOut, unacknowledgedMessages, unacknowledgedBytes",
                where: "to_string($parent.id) = '" + this.modelObj.id + "'",
                orderBy: "name",
                columns: [{
                    label: "Name",
                    field: "name"
                }, {
                    label: "Mode",
                    field: "distributionMode"
                }, {
                    label: "Connection",
                    field: "connectionName"
                },{
                    label: "User",
                    field: "connectionPrincipal"
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
                                      var connectionId = this.consumersGrid.row(event.id).data.connectionId;
                                      this.controller.showById(connectionId);
                                  }));
            this.consumersGrid.startup();

            // Add onShow handler to work around an issue with not rendering of grid columns before first update.
            // It seems if dgrid is created when tab is not shown (not active) the grid columns are not rendered.
            this.contentPane.on("show",
                function()
                {
                    that.consumersGrid.resize();
                });

            this._messagesGrid = this._buildMessagesGrid(findNode);
        }

        QueueUpdater.prototype.clearMessageSelection = function()
        {
            this._messagesGrid.clearSelection();
        };

        QueueUpdater.prototype.refreshMessages = function()
        {
            this._messagesStore.updateRange();
        };

        QueueUpdater.prototype.getSelectedMessageIDs = function ()
        {
            var selected = [];
            var selection = this._messagesGrid.selection || {};
            for (var item in selection)
            {
                if (selection.hasOwnProperty(item) && selection[item])
                {
                    selected.push(parseInt(item));
                }
            }
            return selected;
        };

        QueueUpdater.prototype._buildMessagesGrid = function(findNode)
        {
            var Store = declare([MessageStore, StoreUpdater],
                {
                    track: function () {
                        return this;
                    },

                    _createSubCollection: function () {
                        return this;
                    }
                });

            this._messagesStore = new Store({
                idProperty: "id",
                modelObject: this.modelObj,
                management: this.management
            });

            this._messagesStore.on("unexpected", util.xhrErrorHandler);

            var userPreferences = this.management.userPreferences;
            var MessagesGrid = declare([Grid,
                                        Keyboard,
                                        Selection,
                                        Selector,
                                        Pagination,
                                        ColumnResizer,
                                        DijitRegistry
            ]);

            var messagesGrid = new MessagesGrid({
                rowsPerPage: 10,
                selectionMode: 'none',
                detectChanges: true,
                deselectOnRefresh: false,
                allowSelectAll: true,
                cellNavigation: true,
                className: 'dgrid-autoheight',
                pageSizeOptions: [10, 20, 30, 40, 50, 100],
                adjustLastColumn: true,
                collection: this._messagesStore,
                highlightRow: function () {
                },
                columns: [
                    {
                        label: 'All',
                        selector: 'checkbox',
                        field: "selected",
                        sortable: false
                    },
                    {
                        label: "ID",
                        field: "id",
                        sortable: false
                    }, {
                        label: "Payload Size",
                        field: "size",
                        formatter: formatter.formatBytes,
                        sortable: false
                    }, {
                        label: "State",
                        field: "state",
                        sortable: false
                    }, {
                        label: "Arrival",
                        field: "arrivalTime",
                        sortable: false,
                        formatter: function (value, object) {
                            return userPreferences.formatDateTime(value,
                                {
                                    addOffset: true,
                                    appendTimeZone: true
                                });
                        }
                    }
                ]
            }, findNode("messagesGrid"));

            var management = this.management;
            var modelObj = this.modelObj;
            messagesGrid.on('.dgrid-row:dblclick', lang.hitch(this, function (event) {
                showMessage.show(management, modelObj, messagesGrid.row(event));
            }));
            messagesGrid.on('.dgrid-row:keypress', lang.hitch(this, function (event) {
                if (event.keyCode === keys.ENTER)
                {
                    showMessage.show(management, modelObj, messagesGrid.row(event));
                }
            }));

            messagesGrid.startup();
            var queueMessagesPanel = registry.byNode(findNode("queueMessages"));
            aspect.after(queueMessagesPanel, "toggle", function () {
                if (queueMessagesPanel.get("open") === true)
                {
                    messagesGrid.refresh();
                }
            });
            return messagesGrid;
        };

        function renderMaximumQueueDepthMessages(valueElement, value, bytes)
        {
            var text;
            if (value < 0)
            {
                text = "unlimited";
                valueElement.classList.add("notApplicable");
            }
            else
            {
                valueElement.classList.remove("notApplicable");

                if (bytes)
                {
                    var formatted = formatter.formatBytes(value);

                    text = formatted.value + " " + formatted.units;
                }
                else
                {
                    text = new String(value);
                }
            }
            valueElement.innerHTML = text;
        }

        function renderMaximumQueueDepthBytes(valueElement, unitElement, value)
        {
            var text;
            var unit;
            if (value < 0)
            {
                text = "unlimited";
                unit = "B";
                valueElement.classList.add("notApplicable");
            }
            else
            {
                valueElement.classList.remove("notApplicable");

                var formatted =  formatter.formatBytes(value);
                text = formatted.value;
                unit = formatted.units;
            }
            valueElement.innerHTML = text;
            unitElement.innerHTML = unit;
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

            this.alternateBinding.innerHTML =
                this.queueData["alternateBinding"] && this.queueData["alternateBinding"]["destination"]
                    ? entities.encode(String(this.queueData["alternateBinding"]["destination"])) : "";

            this["type"].innerHTML = entities.encode(this.queueData["type"]);
            if (this.queueData["type"] === "standard")
            {
                this.typeQualifier.style.display = "none";
            }
            else
            {
                this.typeQualifier.innerHTML = entities.encode("(" + queueTypeKeyNames[this.queueData["type"]] + ": "
                                                               + this.queueData[queueTypeKeys[this.queueData["type"]]]
                                                               + ")");
            }

            var overflowPolicy = this.queueData["overflowPolicy"];
            this["overflowPolicy"].innerHTML = entities.encode(overflowPolicy);

            if (overflowPolicy && overflowPolicy !== "NONE")
            {
                this.maximumQueueDepth.style.display = "block";
                renderMaximumQueueDepthBytes(this["maximumQueueDepthBytes"], this["maximumQueueDepthBytesUnits"], this.queueData.maximumQueueDepthBytes);
                renderMaximumQueueDepthMessages(this["maximumQueueDepthMessages"], this.queueData.maximumQueueDepthMessages);
            }
            else
            {
                this.maximumQueueDepth.style.display = "none";
            }
            var messageGroupType = this.queueData["messageGroupType"];
            this["messageGroupType"].innerHTML = entities.encode(messageGroupType);
            if (this.queueData["messageGroupKey"] || (messageGroupType && messageGroupType !== "NONE"))
            {
                this.messageGroupKeyOverride.innerHTML = entities.encode(String(this.queueData["messageGroupKeyOverride"]));
                this.messageGroupKeyOverride.style.display = "block";
            }
            else
            {
                this.messageGroups.style.display = "none";
            }

            var maximumDeliveryAttempts = this.queueData["maximumDeliveryAttempts"];
            this.maximumDeliveryAttempts.innerHTML =
                entities.encode(String(maximumDeliveryAttempts === 0 ? "" : maximumDeliveryAttempts));
            this.holdOnPublishEnabled.innerHTML = entities.encode(String(this.queueData["holdOnPublishEnabled"]));

            this.queueStatistics.update(this.queueData.statistics);
        };

        QueueUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var thisObj = this;

            thisObj.consumersGrid.updateData();
            thisObj.consumersGrid.resize();

            var queuePromise = this.management.load(this.modelObj, {excludeInheritedContext: true, depth: 1 });
            var publishingLinkPromise = this.management.load({type: "queue", name: "getPublishingLinks", parent: this.modelObj});

            all({queue: queuePromise, publishingLinks: publishingLinkPromise})
                .then(function (data)
                {
                    var i, j;
                    thisObj.queueData = data.queue;

                    if (!thisObj.queueStatistics)
                    {
                        thisObj.queueStatistics = new StatisticsWidget({
                            category:  "Queue",
                            type: thisObj.queueData.type,
                            management: thisObj.management,
                            defaultStatistics: ["totalEnqueuedMessages", "totalDequeuedMessages",
                                                "unacknowledgedMessages", "queueDepthMessages",
                                                "oldestMessageAge"]
                        });
                        thisObj.queueStatistics.placeAt(thisObj.queueStatisticsNode);
                        thisObj.queueStatistics.startup();
                    }


                    if (callback)
                    {
                        callback();
                    }
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

                    thisObj.consumers = consumers;

                    // update bindings
                    thisObj.bindingsGrid.update(bindings);
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

        QueueUpdater.prototype._transformConsumerData = function (data)
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
