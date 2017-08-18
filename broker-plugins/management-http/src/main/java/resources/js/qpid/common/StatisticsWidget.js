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
define(["qpid/common/UpdatableStore",
        "qpid/common/formatter",
        "dojox/grid/EnhancedGrid",
        "dojo/_base/declare",
        "dojo/_base/array",
        "dojo/_base/lang",
        "dojo/_base/connect",
        "dojo/on",
        "dojo/mouse",
        "dojo/number",
        "dijit/_WidgetBase",
        "dijit/Tooltip",
        "dijit/registry",
        "dojo/text!common/StatisticsWidget.html",
        "dojo/domReady!"],
    function (UpdatableStore,
              formatter,
              EnhancedGrid,
              declare,
              array,
              lang,
              connect,
              on,
              mouse,
              number,
              _WidgetBase,
              Tooltip,
              registry,
              template) {

        return declare("qpid.common.StatisticsWidget",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                cumulativeStatisticsGridContainer: null,
                pointInTimeStatisticsGridContainer: null,
                statisticsPane: null,

                category: null,
                type: null,
                management: null,

                _grid: null,
                _sampleTime: null,
                _previousSampleTime: null,

                postCreate: function () {
                    this.inherited(arguments);

                    this.userPreferences = this.management.userPreferences;

                    this.cumulativeStatisticsGrid =
                        new UpdatableStore([], this.cumulativeStatisticsGridContainer, [{
                            name: "Name",
                            field: "label",
                            width: "50%"
                        }, {
                            name: "Value",
                            fields: ["value","units"],
                            width: "25%",
                            formatter: lang.hitch(this, this._formatValue)
                        }, {
                            name: "Rate",
                            fields: ["value", "previousValue", "units"],
                            width: "25%",
                            formatter: lang.hitch(this, this._formatRate)
                        }], null, {}, EnhancedGrid);

                    this.pointInTimeStatisticsGrid =
                        new UpdatableStore([], this.pointInTimeStatisticsGridContainer, [{
                            name: "Name",
                            field: "label",
                            width: "50%"
                        }, {
                            name: "Value",
                            fields: ["value","units"],
                            width: "50%",
                            formatter: lang.hitch(this, this._formatValue)
                        }], null, {}, EnhancedGrid);

                    var metadata = this.management.metadata;
                    var type = this.type ? this.type : this.category;
                    var cumulativeStatistics = metadata.getStatisticsMetadata(this.category, type, "CUMULATIVE");
                    var pointInTimeStatistics = metadata.getStatisticsMetadata(this.category, type, "POINT_IN_TIME");
                    var augmentedCumulativeStatistics = this._augmentStatisticsForDisplay(cumulativeStatistics);
                    var augmentedPointInTimeStatistics = this._augmentStatisticsForDisplay(pointInTimeStatistics);

                    this.cumulativeStatisticsGrid.grid.beginUpdate();
                    this.cumulativeStatisticsGrid.update(augmentedCumulativeStatistics);
                    this.cumulativeStatisticsGrid.grid.endUpdate();

                    this.pointInTimeStatisticsGrid.grid.beginUpdate();
                    this.pointInTimeStatisticsGrid.update(augmentedPointInTimeStatistics);
                    this.pointInTimeStatisticsGrid.grid.endUpdate();

                    connect.connect(this.statisticsPane, "toggle", lang.hitch(this, this.resize));
                    connect.connect(this.cumulativeStatisticsGrid.grid, "onCellMouseOver", lang.hitch(this, function(e)
                    {
                        this._addDynamicTooltipToGridRow(e, augmentedCumulativeStatistics);
                    }));
                    connect.connect(this.pointInTimeStatisticsGrid.grid, "onCellMouseOver", lang.hitch(this, function(e)
                    {
                        this._addDynamicTooltipToGridRow(e, augmentedPointInTimeStatistics);
                    }));
                },
                _formatRate : function (fields)
                {
                    if (this._previousSampleTime)
                    {
                        var value = fields[0] ? fields[0] : 0;
                        var previousValue = fields[1] ? fields[1] : 0;
                        var units = fields[2];

                        var samplePeriod = this._sampleTime.getTime() - this._previousSampleTime.getTime();

                        var rate = number.round((1000 * (value - previousValue)) / samplePeriod);

                        if (units === "BYTES")
                        {
                            return formatter.formatBytes(rate) + "/s";
                        }
                        else if (units === "MESSAGES")
                        {
                            return "" + rate + " msg/s"
                        }
                        else
                        {
                            return "" + rate + " &Delta;s"
                        }
                    }
                    return "N/A";
                },
                _formatValue : function (fields) {
                    var value = fields[0] ? fields[0] : 0;
                    var units = fields[1];
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
                resize: function ()
                {
                    this.inherited(arguments);
                    this.cumulativeStatisticsGrid.grid.resize();
                    this.pointInTimeStatisticsGrid.grid.resize();
                },
                update: function (statistics)
                {
                    this._previousSampleTime = this._sampleTime;
                    this._sampleTime = new Date();

                    this.cumulativeStatisticsGrid.grid.beginUpdate();
                    this._updateStoreData(statistics, this.cumulativeStatisticsGrid.store.data);
                    this.cumulativeStatisticsGrid.grid.endUpdate();

                    this.pointInTimeStatisticsGrid.grid.beginUpdate();
                    this._updateStoreData(statistics, this.pointInTimeStatisticsGrid.store.data);
                    this.pointInTimeStatisticsGrid.grid.endUpdate();
                },
                _updateStoreData: function (statistics, data) {
                    array.forEach(data, function (storeItem) {
                        storeItem["previousValue"] = storeItem["value"];
                        storeItem["value"] = statistics[storeItem.id];
                    }, this);
                },
                _augmentStatisticsForDisplay : function(statItems)
                {
                    var items = [];
                    array.forEach(statItems, function(statItem) {
                        var item = lang.mixin(statItem,
                            {id: statItem.name,
                                value: null,
                                previousValue: null});
                        items.push(item);

                    }, this);
                    items.sort(function(x,y) {return ((x.label === y.label) ? 0 : ((x.label > y.label) ? 1 : -1 ))});
                    return items;
                },
                destroy: function ()
                {
                    this.inherited(arguments);
                    this.cumulativeStatisticsGrid.close();
                    this.pointInTimeStatisticsGrid.close();
                },
                _addDynamicTooltipToGridRow: function(e, data)
                {
                    var tip = data[e.rowIndex];
                    if (tip && tip.description)
                    {
                        Tooltip.show(tip.description, e.rowNode);
                        on.once(e.rowNode, mouse.leave, function()
                        {
                            Tooltip.hide(e.rowNode);
                        });
                    }
                }
            });
    });
