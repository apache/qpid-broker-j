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
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojo/text!showLogger.html",
        "qpid/management/addLogger",
        "qpid/management/addLogInclusionRule",
        "dojo/domReady!"],
    function (parser,
              query,
              connect,
              registry,
              entities,
              properties,
              updater,
              util,
              formatter,
              UpdatableStore,
              EnhancedGrid,
              template,
              addLogger,
              addLogInclusionRule)
    {

        function Logger(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
            var isBrokerLogger = this.modelObj.type == "brokerlogger";
            this.category = isBrokerLogger ? "BrokerLogger" : "VirtualHostLogger";
            this.logInclusionRuleCategory = isBrokerLogger ? "BrokerLogInclusionRule" : "VirtualHostLogInclusionRule";
            this.userFriendlyName = (isBrokerLogger ? "Broker Logger" : "Virtual Host Logger");
        }

        Logger.prototype.getTitle = function ()
        {
            return this.userFriendlyName + ": " + this.name;
        };

        Logger.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.onOpen(contentPane.containerNode)
                });

        };

        Logger.prototype.onOpen = function (containerNode)
        {
            var that = this;
            this.editLoggerButton = registry.byNode(query(".editLoggerButton", containerNode)[0]);
            this.deleteLoggerButton = registry.byNode(query(".deleteLoggerButton", containerNode)[0]);
            this.deleteLoggerButton.on("click", function (e)
            {
                if (confirm("Are you sure you want to delete logger '" + entities.encode(String(that.name)) + "'?"))
                {
                    that.management.remove(that.modelObj)
                        .then(function (x)
                        {
                            that.destroy();
                        }, util.xhrErrorHandler);
                }
            });
            this.editLoggerButton.on("click", function (event)
            {
                that.management.load(that.modelObj, {
                        actuals: true,
                        excludeInheritedContext: true,
                        depth: 0
                    })
                    .then(function (data)
                    {
                        addLogger.show(that.management, that.modelObj, that.category, data);
                    });
            });

            var gridProperties = {
                selectionMode: "extended",
                plugins: {
                    indirectSelection: true
                }
            };

            this.logInclusionRuleGrid = new UpdatableStore([], query(".logInclusionRuleGrid", containerNode)[0], [{
                name: "Rule Name",
                field: "name",
                width: "20%"
            }, {
                name: "Type",
                field: "type",
                width: "20%"
            }, {
                name: "Logger Name",
                field: "loggerName",
                width: "30%"
            }, {
                name: "Level",
                field: "level",
                width: "20%"
            }, {
                name: "Durable",
                field: "durable",
                width: "10%",
                formatter: util.buildCheckboxMarkup
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex;
                    var theItem = this.getItem(idx);
                    that.showLogInclusionRule(theItem);
                });
            }, gridProperties, EnhancedGrid);

            this.addLogInclusionRuleButton = registry.byNode(query(".addLogInclusionRuleButton", containerNode)[0]);
            this.deleteLogInclusionRuleButton =
                registry.byNode(query(".deleteLogInclusionRuleButton", containerNode)[0]);

            this.deleteLogInclusionRuleButton.on("click", function (e)
            {
                util.deleteSelectedObjects(that.logInclusionRuleGrid.grid,
                    "Are you sure you want to delete log inclusion rule",
                    that.management,
                    {
                        type: that.logInclusionRuleCategory.toLowerCase(),
                        parent: that.modelObj
                    },
                    that.loggerUpdater);
            });

            this.addLogInclusionRuleButton.on("click", function (e)
            {
                addLogInclusionRule.show(that.management, that.modelObj, that.logInclusionRuleCategory);
            });

            this.loggerUpdater = new Updater(this);
            this.loggerUpdater.update(function (x)
            {
                updater.add(that.loggerUpdater);
            });
        };

        Logger.prototype.showLogInclusionRule = function (item)
        {
            var ruleModelObj = {
                name: item.name,
                type: this.logInclusionRuleCategory.toLowerCase(),
                parent: this.modelObj
            };
            var that = this;
            this.management.load(ruleModelObj,
                {
                    actuals: true,
                    excludeInheritedContext: true
                })
                .then(function (data)
                {
                    addLogInclusionRule.show(that.management, ruleModelObj, that.logInclusionRuleCategory, data);
                });
        };

        Logger.prototype.close = function ()
        {
            updater.remove(this.loggerUpdater);
        };

        Logger.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        function Updater(logger)
        {
            var domNode = logger.contentPane.containerNode;
            this.tabObject = logger;
            this.contentPane = logger.contentPane;
            this.modelObj = logger.modelObj;
            var that = this;

            function findNode(name)
            {
                return query("." + name, domNode)[0];
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
                        "type",
                        "loggerAttributes",
                        "loggerTypeSpecificDetails",
                        "logInclusionRuleWarning",
                        "durable",
                        "errorCount",
                        "warnCount"]);
        }

        Updater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;
            that.tabObject.management.load(this.modelObj,
                {
                    excludeInheritedContext: true,
                    depth: 1
                })
                .then(function (data)
                {
                    that.loggerData = data || {};
                    that.updateUI(that.loggerData);

                    if (callback)
                    {
                        callback();
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.tabObject.contentPane,
                        tabContainer: that.tabObject.controller.tabContainer,
                        name: that.modelObj.name,
                        category: that.tabObject.userFriendlyName
                    });
                });
        };

        Updater.prototype.updateUI = function (data)
        {
            this.name.innerHTML = entities.encode(String(data["name"]));
            this.state.innerHTML = entities.encode(String(data["state"]));
            this.type.innerHTML = entities.encode(String(data["type"]));
            this.durable.innerHTML = util.buildCheckboxMarkup(data["durable"]);
            this.errorCount.innerHTML = String(data["statistics"]["errorCount"]);
            this.warnCount.innerHTML = String(data["statistics"]["warnCount"]);

            if (!this.details)
            {
                var that = this;
                require(["qpid/management/logger/" + this.tabObject.modelObj.type + "/show"], function (Details)
                {
                    that.details = new Details({
                        containerNode: that.loggerAttributes,
                        contentPane: that.contentPane,
                        typeSpecificDetailsNode: that.loggerTypeSpecificDetails,
                        metadata: that.tabObject.management.metadata,
                        data: data,
                        management: that.tabObject.management,
                        modelObj: that.tabObject.modelObj
                    });
                });
            }
            else
            {
                this.details.update(data);
            }

            var ruleFieldName = this.tabObject.logInclusionRuleCategory.toLowerCase() + "s"; // add plural "s"
            if (data[ruleFieldName])
            {
                this.tabObject.logInclusionRuleGrid.grid.domNode.style.display = "block";
                this.logInclusionRuleWarning.style.display = "none";
            }
            else
            {
                this.tabObject.logInclusionRuleGrid.grid.domNode.style.display = "none";
                this.logInclusionRuleWarning.style.display = "block";
            }
            util.updateUpdatableStore(this.tabObject.logInclusionRuleGrid, data[ruleFieldName]);
        };

        return Logger;
    });
