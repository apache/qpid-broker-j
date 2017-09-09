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
define(["dojox/lang/functional/object",
        "dojo/_base/declare",
        "dojo/_base/array",
        "dojo/_base/lang",
        "dojo/_base/connect",
        "dojo/on",
        "dojo/mouse",
        "dojo/number",
        "dojo/store/Memory",
        "dojox/grid/EnhancedGrid",
        "dojo/data/ObjectStore",
        "dojo/store/Observable",
        "dijit/_WidgetBase",
        "dijit/Tooltip",
        "dijit/registry",
        "qpid/common/formatter",
        "dojo/text!common/StatisticsWidget.html",
        "dijit/form/ToggleButton",
        "dojo/domReady!"],
    function (fobject,
              declare,
              array,
              lang,
              connect,
              on,
              mouse,
              number,
              Memory,
              EnhancedGrid,
              ObjectStore,
              Observable,
              _WidgetBase,
              Tooltip,
              registry,
              formatter,
              template) {

        return declare("qpid.common.StatisticsWidget",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                msgBytePairCumulativeStatisticsGridContainer: null,
                otherCumulativeStatisticsGridContainer: null,
                msgBytePairPointInTimeStatisticsGridContainer: null,
                otherPointInTimeStatisticsGridContainer: null,

                statisticsPane: null,
                allStatsToggle: null,

                category: null,
                type: null,
                management: null,
                defaultStatistics: null,

                _msgBytePairCumulativeStatisticsGrid: null,
                _otherCumulativeStatisticsGrid: null,
                _msgBytePairPointInTimeStatisticsGrid: null,
                _otherPointInTimeStatisticsGrid: null,
                _allGrids: [],

                _showAllStats : false,
                _sampleTime: null,
                _previousSampleTime: null,
                _nodes: {},

                postCreate: function ()
                {
                    this.inherited(arguments);

                    var metadata = this.management.metadata;

                    var type = this.type ? this.type : this.category;

                    this.userPreferences = this.management.userPreferences;
                    var allStatistics = metadata.getStatisticsMetadata(this.category, type);
                    var allAugmentedStatistics = this._augmentStatistics(allStatistics);
                    var pairedByteMessageStatistic = [];
                    var otherStatistics = [];

                    this._filterMessageByteStatisticPairs(allAugmentedStatistics, pairedByteMessageStatistic, otherStatistics);

                    this._store = new Memory({
                        data: allAugmentedStatistics,
                        idProperty: "id"
                    });

                    this._dataStore = ObjectStore({objectStore: new Observable(this._store)});

                    var formatRate = function (fields)
                    {
                        var value = fields[0] ? fields[0] : 0;
                        var previousValue = fields[1] ? fields[1] : 0;
                        var units = fields[2];
                        var rateWithUnit = this._formatRate(value, previousValue, units);
                        return rateWithUnit ? rateWithUnit.toString() : "N/A";
                    };

                    var formatValue = function (fields) {
                        var value = fields[0] ? fields[0] : 0;
                        var units = fields[1];
                        return this._formatValue(value, units);
                    };

                    var formatByteStatValueFromMsgStat = function (msgStatItem) {
                        var byteStatItem = this._queryStatItem(msgStatItem, "BYTES");
                        var value = this._formatValue(byteStatItem.value ? byteStatItem.value : 0, byteStatItem.units);
                        return value;
                    };

                    var formatByteStatRateFromMsgStat = function (msgStatItem) {
                        var byteStatItem = this._queryStatItem(msgStatItem, "BYTES");
                        var rateWithUnit = this._formatRate(byteStatItem.value ? byteStatItem.value : 0, byteStatItem.previousValue ? byteStatItem.previousValue : 0, byteStatItem.units);
                        return rateWithUnit ? rateWithUnit.toString() : "N/A";
                    };

                    this._msgBytePairCumulativeStatisticsGrid =
                        new EnhancedGrid({
                            store: this._dataStore,
                            class: "statisticGrid",
                            autoHeight: true,
                            structure: [{
                                name: "Name",
                                field: "label",
                                width: "52%"
                            }, {
                                name: "Messages",
                                fields: ["value", "units"],
                                width: "12%",
                                formatter: lang.hitch(this, formatValue)
                            }, {
                                name: "Message Rate",
                                fields: ["value", "previousValue", "units"],
                                width: "12%",
                                formatter: lang.hitch(this, formatRate)
                            }, {
                                name: "Bytes",
                                field: "_item",
                                width: "12%",
                                formatter: lang.hitch(this, formatByteStatValueFromMsgStat)
                            }, {
                                name: "Byte Rate",
                                field: "_item",
                                width: "12%",
                                formatter: lang.hitch(this, formatByteStatRateFromMsgStat)
                            }]
                        }, this.msgBytePairCumulativeStatisticsGridContainer);

                    this._msgBytePairCumulativeStatisticsGrid.query = lang.hitch(this, function (statItem) {
                        return statItem.statisticType === "CUMULATIVE" &&
                               statItem.units === "MESSAGES" &&
                               array.indexOf(pairedByteMessageStatistic, statItem) > -1 &&
                               this._isStatItemShown(statItem);
                    });

                    this._otherCumulativeStatisticsGrid =
                        new EnhancedGrid({
                            store: this._dataStore,
                            class: "statisticGrid",
                            autoHeight: true,
                            structure: [{
                                name: "Name",
                                field: "label",
                                width: "52%"
                            }, {
                                name: "Value",
                                fields: ["value", "units"],
                                width: "24%",
                                formatter: lang.hitch(this, formatValue)
                            }, {
                                name: "Rate",
                                fields: ["value", "previousValue", "units"],
                                width: "24%",
                                formatter: lang.hitch(this, formatRate)
                            }]
                        }, this.otherCumulativeStatisticsGridContainer);
                    this._otherCumulativeStatisticsGrid.query = lang.hitch(this, function (statItem) {
                        return statItem.statisticType === "CUMULATIVE" &&
                               array.indexOf(pairedByteMessageStatistic, statItem) === -1 &&
                               this._isStatItemShown(statItem);
                    });

                    this._msgBytePairPointInTimeStatisticsGrid =
                        new EnhancedGrid({
                            store: this._dataStore,
                            class: "statisticGrid",
                            autoHeight: true,
                            structure: [{
                                name: "Name",
                                field: "label",
                                width: "52%"
                            }, {
                                name: "Message Value",
                                fields: ["value", "units"],
                                width: "24%",
                                formatter: lang.hitch(this, formatValue)
                            }, {
                                name: "Byte Value",
                                field: "_item",
                                width: "24%",
                                formatter: lang.hitch(this, formatByteStatValueFromMsgStat)
                            }]
                        }, this.msgBytePairPointInTimeStatisticsGridContainer);

                    this._msgBytePairPointInTimeStatisticsGrid.query = lang.hitch(this, function (statItem) {
                        return statItem.statisticType === "POINT_IN_TIME" &&
                               statItem.units === "MESSAGES" &&
                               array.indexOf(pairedByteMessageStatistic, statItem) > -1 &&
                               this._isStatItemShown(statItem);
                    });

                    this._otherPointInTimeStatisticsGrid =
                        new EnhancedGrid({
                            store: this._dataStore,
                            class: "statisticGrid",
                            autoHeight: true,
                            structure: [{
                                name: "Name",
                                field: "label",
                                width: "52%"
                            }, {
                                name: "Value",
                                fields: ["value", "units"],
                                width: "48%",
                                formatter: lang.hitch(this, formatValue)
                            }]
                        }, this.otherPointInTimeStatisticsGridContainer);

                    this._otherPointInTimeStatisticsGrid.query = lang.hitch(this, function (statItem) {
                        return statItem.statisticType === "POINT_IN_TIME" &&
                               array.indexOf(pairedByteMessageStatistic, statItem) === -1 &&
                               this._isStatItemShown(statItem);
                    });

                    this._allGrids.push(this._msgBytePairCumulativeStatisticsGrid,
                                        this._otherCumulativeStatisticsGrid,
                                        this._msgBytePairPointInTimeStatisticsGrid,
                                        this._otherPointInTimeStatisticsGrid);

                    connect.connect(this.statisticsPane, "toggle", lang.hitch(this, this.resize));
                    array.forEach(this._allGrids, function(grid)
                    {
                        connect.connect(grid, "onCellMouseOver", lang.hitch(this, this._addDynamicTooltipToGridRow));
                    }, this);

                    this.allStatsToggle.on("change", lang.hitch(this, this._onStatsToggleChange));
                },
                startup: function ()
                {
                    if (!this._started)
                    {
                        this.inherited(arguments);

                        array.forEach(this._allGrids, function (grid) {
                            grid.startup();
                        }, this);
                    }
                },
                resize: function ()
                {
                    this.inherited(arguments);
                    this._showHideGrids();
                    array.forEach(this._allGrids, function(grid)
                    {
                        grid.resize();
                    }, this);

                },
                update: function (statistics)
                {
                    this._previousSampleTime = this._sampleTime;
                    this._sampleTime = new Date();

                    array.forEach(this._allGrids, function(grid)
                    {
                        grid.beginUpdate();
                    }, this);

                    this._updateStoreData(statistics, this._store.data);

                    array.forEach(this._allGrids, function(grid)
                    {
                        grid.endUpdate();
                    }, this);
                },
                uninitialize: function ()
                {
                    this.inherited(arguments);
                    this._dataStore.close();
                },
                _isStatItemShown: function (statItem)
                {
                    return this._showAllStats || (!this.defaultStatistics || array.indexOf(this.defaultStatistics, statItem.name) > -1);
                },
                _onStatsToggleChange: function (value)
                {
                    this.allStatsToggle.set("label", value ? "Show fewer statistics" : "Show more statistics");
                    this.allStatsToggle.set("iconClass", value ? "minusIcon" : "addIcon");
                    this._showAllStats = value;

                    array.forEach(this._allGrids, function(grid)
                    {
                        grid._refresh();
                    }, this);

                    this._showHideGrids();
                    this.resize();
                },
                _formatRate : function (value, previousValue, units)
                {
                    if (this._previousSampleTime)
                    {
                        var samplePeriod = this._sampleTime.getTime() - this._previousSampleTime.getTime();

                        var rate = number.round((1000 * (value - previousValue)) / samplePeriod);

                        var valueWithUnit = {
                            units: null,
                            value: rate,
                            toString : function()
                            {
                                return this.value + " " + this.units;
                            }
                        };

                        if (units === "BYTES")
                        {
                            var byteValueWithUnit = formatter.formatBytes(rate);
                            valueWithUnit.units = byteValueWithUnit.units  + "/s";
                            valueWithUnit.value = byteValueWithUnit.value;
                        }
                        else if (units === "MESSAGES")
                        {
                            valueWithUnit.units = " msg/s";
                        }
                        else
                        {
                            valueWithUnit.units = " &Delta;s"
                        }
                        return valueWithUnit;
                    }

                    return null;
                },
                _formatValue : function (value, units)
                {
                    if (units === "BYTES")
                    {
                        return formatter.formatBytes(value);
                    }
                    else if (units === "MESSAGES")
                    {
                        return "" + value + " msg"
                    }
                    else if (units === "ABSOLUTE_TIME")
                    {
                        return this.userPreferences.formatDateTime(value, {
                            addOffset: true,
                            appendTimeZone: true
                        });
                    }
                    else
                    {
                        return value;
                    }
                },
                _showHideGrids: function ()
                {
                    array.forEach(this._allGrids, function(grid)
                    {
                        grid.set("hidden", grid.rowCount === 0);
                    }, this);
                },
                _updateStoreData: function (statistics, data) {
                    array.forEach(data, function (storeItem) {
                        storeItem["previousValue"] = storeItem["value"];
                        storeItem["value"] = statistics[storeItem.id];
                    }, this);
                },
                _augmentStatistics : function(statItems)
                {
                    var items = [];
                    fobject.forIn(statItems, function(statItem) {
                        var item = lang.mixin(statItem,
                            {id: statItem.name,
                                value: null,
                                previousValue: null});
                        items.push(item);
                    }, this);
                    items.sort(function(x,y) {return ((x.label === y.label) ? 0 : ((x.label > y.label) ? 1 : -1 ))});
                    return items;
                },
                _filterMessageByteStatisticPairs : function(augmentedStatistics, byteMsgPairStatistics, otherStatistics)
                {
                    array.forEach(augmentedStatistics, function (item) {
                        otherStatistics.push(item);
                    }, this);

                    var msgCandidates = array.filter(otherStatistics, function(item)
                    {return item.units === "MESSAGES"}, this);

                    array.forEach(msgCandidates, function(msgItemCandidate)
                    {
                        var byteItemCandidates = array.filter(otherStatistics, function(item)
                        {
                            return item.units === "BYTES" && item.label === msgItemCandidate.label;
                        });

                        if (byteItemCandidates.length === 1)
                        {
                            // Found a msg/byte statistic pair
                            var byteItemCandidate = byteItemCandidates[0];
                            otherStatistics.splice(array.indexOf(otherStatistics, msgItemCandidate), 1);
                            otherStatistics.splice(array.indexOf(otherStatistics, byteItemCandidate), 1);

                            byteMsgPairStatistics.push(msgItemCandidate);
                            byteMsgPairStatistics.push(byteItemCandidate);
                        }

                    }, this);
                },
                _addDynamicTooltipToGridRow: function(e)
                {
                    var statItem = e.grid.getItem(e.rowIndex);
                    if (statItem && statItem.description)
                    {
                        Tooltip.show(statItem.description, e.rowNode);
                        on.once(e.rowNode, mouse.leave, function()
                        {
                            Tooltip.hide(e.rowNode);
                        });
                    }
                },
                _queryStatItem: function (statItem, units)
                {
                    var result = this._store.query({
                        label: statItem.label,
                        statisticType: statItem.statisticType,
                        units: units
                    });
                    return result[0];
                }
            });
    });
