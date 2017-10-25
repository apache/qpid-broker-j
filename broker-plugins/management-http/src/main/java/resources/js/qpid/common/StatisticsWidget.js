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
        "dojo/promise/all",
        "dojo/Deferred",
        "dojo/dom-style",
        "dojo/on",
        "dojo/mouse",
        "dojo/number",
        "dstore/Memory",
        'dstore/Trackable',
        "dojox/html/entities",
        "dgrid/OnDemandGrid",
        "dgrid/extensions/ColumnResizer",
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
              all,
              Deferred,
              domStyle,
              on,
              mouse,
              number,
              Memory,
              Trackable,
              entities,
              OnDemandGrid,
              ColumnResizer,
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

                // template fields
                msgBytePairCumulativeStatisticsGridContainer: null,
                otherCumulativeStatisticsGridContainer: null,
                msgBytePairPointInTimeStatisticsGridContainer: null,
                otherPointInTimeStatisticsGridContainer: null,

                statisticsPane: null,
                allStatsToggle: null,

                // constructor arguments
                category: null,
                type: null,
                management: null,
                defaultStatistics: null,

                // inner fields
                _msgBytePairCumulativeStatisticsGrid: null,
                _otherCumulativeStatisticsGrid: null,
                _msgBytePairPointInTimeStatisticsGrid: null,
                _otherPointInTimeStatisticsGrid: null,
                _allGrids: null,

                _showAllStats : false,
                _sampleTime: null,
                _previousSampleTime: null,
                _userPreferences: null,

                postCreate: function ()
                {
                    this.inherited(arguments);

                    var metadata = this.management.metadata;

                    var type = this.type ? this.type : this.category;

                    this._userPreferences = this.management.userPreferences;
                    var allStatistics = metadata.getStatisticsMetadata(this.category, type);
                    var allAugmentedStatistics = this._augmentStatistics(allStatistics);
                    var pairedByteMessageStatistic = [];
                    var otherStatistics = [];

                    this._filterMessageByteStatisticPairs(allAugmentedStatistics, pairedByteMessageStatistic, otherStatistics);

                    var TrackableMemory = declare([Memory, Trackable]);

                    this._otherStatsStore = new TrackableMemory({
                        data: otherStatistics,
                        idProperty: "id"
                    });

                    this._pairedStatsStore = new TrackableMemory({
                        data: pairedByteMessageStatistic,
                        idProperty: "id"
                    });

                    var defaultFilter = function(item)
                    {
                        return this._showAllStats || item.defaultItem;
                    };

                    var gridProps = {
                        className: "dgrid-autoheight statisticGrid",
                        highlightRow : function() {return false}
                    };

                    var Grid =  declare([OnDemandGrid, ColumnResizer]);

                    this._msgBytePairCumulativeStatisticsGrid =
                        new Grid(lang.mixin(lang.clone(gridProps), {
                            collection: this._pairedStatsStore.filter({statisticType : "CUMULATIVE"})
                                                              .filter(lang.hitch(this, defaultFilter)),
                            columns: [{
                                label: "Name",
                                get: lang.hitch(this, function (obj) {return obj.msgItem.label})
                            }, {
                                label: "Messages",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj.msgItem)})
                            }, {
                                label: "Message Rate",
                                get: lang.hitch(this, function(obj) {return this._formatRate(obj.msgItem)})
                            }, {
                                label: "Bytes",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj.byteItem)})
                            }, {
                                label: "Byte Rate",
                                get: lang.hitch(this, function(obj) {return this._formatRate(obj.byteItem)})
                            }]
                        }), this.msgBytePairCumulativeStatisticsGridContainer);

                    this._otherCumulativeStatisticsGrid =
                        new Grid(lang.mixin(lang.clone(gridProps), {
                            collection: this._otherStatsStore.filter({statisticType: "CUMULATIVE"})
                                                             .filter(lang.hitch(this, defaultFilter)),
                            columns: [{
                                label: "Name",
                                field: "label"
                            }, {
                                label: "Value",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj)})
                            }, {
                                label: "Rate",
                                get: lang.hitch(this, function(obj) {return this._formatRate(obj)})
                            }]
                        }), this.otherCumulativeStatisticsGridContainer);

                    this._msgBytePairPointInTimeStatisticsGrid =
                        new Grid(lang.mixin(lang.clone(gridProps), {
                            collection: this._pairedStatsStore.filter({statisticType : "POINT_IN_TIME"})
                                                              .filter(lang.hitch(this, defaultFilter)),
                            columns: [{
                                label: "Name",
                                get: lang.hitch(this, function (obj) {return obj.msgItem.label})
                            }, {
                                label: "Message Value",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj.msgItem)})
                            }, {
                                label: "Byte Value",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj.byteItem)})
                            }]
                        }), this.msgBytePairPointInTimeStatisticsGridContainer);

                    this._otherPointInTimeStatisticsGrid =
                        new Grid(lang.mixin(lang.clone(gridProps), {
                            collection: this._otherStatsStore.filter({statisticType: "POINT_IN_TIME"})
                                                             .filter(lang.hitch(this, defaultFilter)),
                            columns: [{
                                label: "Name",
                                field: "label"
                            }, {
                                label: "Value",
                                get: lang.hitch(this, function(obj) {return this._formatValue(obj)})
                            }]
                        }), this.otherPointInTimeStatisticsGridContainer);

                    this._allGrids = [this._msgBytePairCumulativeStatisticsGrid,
                                      this._otherCumulativeStatisticsGrid,
                                      this._msgBytePairPointInTimeStatisticsGrid,
                                      this._otherPointInTimeStatisticsGrid];

                    this.statisticsPane.on("show", lang.hitch(this, this.resize));
                    this.allStatsToggle.on("change", lang.hitch(this, this._onStatsToggleChange));
                },
                startup: function ()
                {
                    if (!this._started)
                    {
                        this.inherited(arguments);

                        array.forEach(this._allGrids, function (grid)
                        {
                            grid.startup();

                            grid.on("dgrid-refresh-complete", lang.hitch(this, function (event)
                            {
                                var hide = event.grid.get("total") === 0;
                                domStyle.set(event.grid.domNode, 'display', hide ? "none": "block");
                                event.grid.resize();
                            }));

                            grid.on(".dgrid-content .dgrid-row:mouseover", lang.hitch(this, function(event)
                            {
                                var row = grid.row(event);
                                var statItem = row.data;
                                this._addDynamicTooltipToGridRow(statItem, row.element);

                            }));
                        }, this);
                    }
                },
                resize: function ()
                {
                    this.inherited(arguments);
                    array.forEach(this._allGrids, function(grid)
                    {
                        grid.resize();
                    }, this);
                },
                update: function (statistics)
                {
                    this._previousSampleTime = this._sampleTime;
                    this._sampleTime = new Date();
                    this._updateStoreData(statistics);
                },
                _updateStoreData: function (statistics)
                {
                    function updateItem(storeItem)
                    {
                        if (storeItem.id in statistics)
                        {
                            var newValue = statistics[storeItem.id];

                            storeItem["previousValue"] = storeItem["value"];
                            storeItem["value"] = newValue;
                            return true;
                        }
                        return false;
                    }

                    this._otherStatsStore.forEach(function(storeItem)
                    {
                        if (updateItem(storeItem))
                        {
                            this._otherStatsStore.put(storeItem);
                        }
                    }, this);

                    this._pairedStatsStore.forEach(function(pairedStatItem)
                    {
                        var updated = updateItem(pairedStatItem.msgItem);
                        updated = updateItem(pairedStatItem.byteItem) || updated;

                        if (updated)
                        {
                            this._pairedStatsStore.put(pairedStatItem);
                        }

                    }, this);
                },
                _onStatsToggleChange: function (value)
                {
                    this.allStatsToggle.set("label", value ? "Show fewer statistics" : "Show more statistics");
                    this.allStatsToggle.set("iconClass", value ? "minusIcon" : "addIcon");
                    this.allStatsToggle.set("disabled", true);
                    this._showAllStats = value;

                    var refreshPromises = [];

                    // Refresh the grids so they re-query the store thus taking account of the new showAllStats state.
                    array.forEach(this._allGrids, function (grid)
                    {
                        var deferred = new Deferred();
                        refreshPromises.push(deferred.promise);

                        var handler = grid.on("dgrid-refresh-complete", lang.hitch(this, function ()
                        {
                            deferred.resolve();
                            handler.remove();
                        }));

                        // grid.refresh();
                        // It seems refreshing the grid is not a reliable way to of getting it to re-query the store,
                        // bumping the collection seems to do the trick.
                        grid.set("collection", grid.get("collection"));
                    }, this);

                    all(refreshPromises).then(lang.hitch(this, function()
                    {
                        this.allStatsToggle.set("disabled", false);
                    }));
                },
                _formatRate : function (statItem)
                {
                    if (this._previousSampleTime)
                    {
                        var value = statItem.value ? statItem.value : 0;
                        var previousValue = statItem.previousValue ? statItem.previousValue : 0;
                        var units = statItem.units;

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
                            valueWithUnit.units = entities.decode("&Delta;") + "value/s";
                        }
                        return valueWithUnit;
                    }

                    return null;
                },
                _formatValue : function (statItem)
                {
                    var value = statItem.value ? statItem.value : 0;
                    var units = statItem.units;
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
                        return value > 0 ? this._userPreferences.formatDateTime(value, {
                            addOffset: true,
                            appendTimeZone: true
                        }) : "-";
                    }
                    else if (units === "TIME_DURATION")
                    {
                        return value < 0 ? "-" : (number.round(value/1000) + " s");
                    }
                    else
                    {
                        return value;
                    }
                },
                _augmentStatistics : function(statItems)
                {
                    var items = [];
                    fobject.forIn(statItems, function(statItem) {
                        var copy = lang.clone(statItem);
                        var item = lang.mixin(copy,
                            {id: copy.name,
                                value: null,
                                previousValue: null,
                                defaultItem: array.indexOf(this.defaultStatistics, copy.name) > -1});
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
                            return item.units === "BYTES" &&
                                   item.label === msgItemCandidate.label &&
                                   item.statisticType === msgItemCandidate.statisticType;
                        });

                        if (byteItemCandidates.length === 1)
                        {
                            // Found a msg/byte statistic pair
                            var byteItemCandidate = byteItemCandidates[0];
                            otherStatistics.splice(array.indexOf(otherStatistics, msgItemCandidate), 1);
                            otherStatistics.splice(array.indexOf(otherStatistics, byteItemCandidate), 1);

                            byteMsgPairStatistics.push({id: msgItemCandidate.id,
                                                        statisticType: msgItemCandidate.statisticType,
                                                        msgItem: msgItemCandidate,
                                                        byteItem: byteItemCandidate,
                                                        defaultItem: msgItemCandidate.defaultItem,
                                                        description: msgItemCandidate.description});
                        }

                    }, this);
                },
                _addDynamicTooltipToGridRow: function(statItem, rowNode)
                {
                    if (statItem && statItem.description)
                    {
                        Tooltip.show(statItem.description, rowNode);
                        on.once(rowNode, mouse.leave, function()
                        {
                            Tooltip.hide(rowNode);
                        });
                    }
                }
            });
    });
