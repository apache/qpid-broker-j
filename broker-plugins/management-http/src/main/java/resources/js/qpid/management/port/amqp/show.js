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
define(["dojo/query",
        "dojo/text!port/amqp/show.html",
        "dojox/grid/EnhancedGrid",
        "dijit/registry",
        "qpid/common/util",
        "qpid/common/UpdatableStore",
        "qpid/management/addVirtualHostAlias",
        "qpid/common/StatisticsWidget",
        "dojo/domReady!"], function (query, template, EnhancedGrid, registry, util, UpdatableStore, addVirtualHostAlias, StatisticsWidget)
{
    function AmqpPort(params)
    {
        var that = this;
        util.parse(params.typeSpecificDetailsNode, template, function ()
        {
            that.postParse(params);
        });
        this.modelObj = params.modelObj;
        this.portUpdater = params.portUpdater;
        this.management = params.management;
    }

    AmqpPort.prototype.postParse = function (params)
    {
        this.portStatisticsNode=query(".portStatistics", params.typeSpecificDetailsNode)[0]
        var that = this;
        var gridProperties = {
            height: 400,
            sortInfo: 2,
            plugins: {
                indirectSelection: true,  // KW TODO checkme
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

        this.addButton = registry.byNode(query(".addButton", params.typeSpecificDetailsNode)[0]);
        this.addButton.on("click", function (e)
        {
            addVirtualHostAlias.show(that.management, that.modelObj);
        });
        this.deleteButton = registry.byNode(query(".deleteButton", params.typeSpecificDetailsNode)[0]);
        this.deleteButton.on("click", function (e)
        {
            util.deleteSelectedObjects(that.virtualHostAliasesGrid.grid,
                "Are you sure you want to delete virtual host alias",
                that.management,
                {
                    type: "virtualhostalias",
                    parent: that.modelObj
                },
                that.portUpdater);
        });

        this.virtualHostAliasesGrid =
            new UpdatableStore(params.data.virtualhostaliases, query(".virtualHostAliasesGrid",
                params.typeSpecificDetailsNode)[0], [{
                name: "Priority",
                field: "priority",
                width: "20%"
            }, {
                name: "Name",
                field: "name",
                width: "40%"
            }, {
                name: "Type",
                field: "type",
                width: "40%"
            }], function (obj)
            {
                obj.grid.on("rowDblClick", function (evt)
                {
                    var idx = evt.rowIndex;
                    var theItem = this.getItem(idx);
                    var aliasModelObj = {
                        name: theItem.name,
                        type: "virtualhostalias",
                        parent: that.modelObj
                    };
                    that.management.load(aliasModelObj, {
                            actuals: true,
                            excludeInheritedContext: true,
                            depth: 0
                        })
                        .then(function (data)
                        {
                            addVirtualHostAlias.show(that.management, aliasModelObj, data);
                        });
                });
            }, gridProperties, EnhancedGrid);
    };

    AmqpPort.prototype.update = function (restData)
    {
        if (this.virtualHostAliasesGrid)
        {
            this.virtualHostAliasesGrid.update(restData.virtualhostaliases);
        }

        if (!this.portStatistics)
        {
            this.portStatistics = new StatisticsWidget({
                category: "Port",
                type: restData.type,
                management: this.management,
                defaultStatistics: ["connectionCount", "totalConnectionCount"]
            });
            this.portStatistics.placeAt(this.portStatisticsNode);
            this.portStatistics.allStatsToggle.domNode.style.display = 'none';
            this.portStatistics.startup();
        }

        this.portStatistics.update(restData.statistics);
    };

    return AmqpPort;
});
