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
        "dojo/_base/array",
        "dojo/Evented",
        "dojo/date/locale",
        "dojo/text!logger/memory/LogViewerWidget.html",
        "qpid/common/util",
        "dgrid/Grid",
        "dgrid/Selector",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/ColumnHider",
        "dgrid/extensions/DijitRegistry",
        "dstore/Memory",
        "dijit/registry",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/Toolbar",
        "dijit/ToolbarSeparator",
        "dijit/form/ToggleButton",
        "dijit/form/NumberTextBox",
        "dijit/ConfirmDialog",
        "qpid/management/query/DropDownSelect",
        "qpid/management/query/WhereExpression"
    ],
    function (declare,
              lang,
              array,
              Evented,
              locale,
              template,
              util,
              Grid,
              Selector,
              Keyboard,
              Selection,
              Pagination,
              ColumnResizer,
              ColumnHider,
              DijitRegistry,
              MemoryStore) {

        function escapeRegExp(text)
        {
            return text.replace(/[-[\]{}()*+?.,\\/^$|#\s]/g, '\\$&');
        }

        return declare("qpid.management.logger.LogViewerWidget",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                // template fields
                lastUpdateTime: null,
                updateTimeNode: null,
                browserTimeNode: null,
                logsToolbar: null,
                refreshButton: null,
                autoRefreshButton: null,
                clearFilterButton: null,
                logEntries: null,
                columnChooser: null,
                filterChooser: null,
                filterBuilder: null,
                rowLimitDialog: null,
                limit: null,
                limitButton: null,

                // constructor mixed in fields
                controller: null,
                management: null,
                userPreferences: null,
                modelObj: null,
                maxRecords: null,

                // inner fields
                _autoRefresh: false,
                _columns: null,
                _filter: null,

                _lastSeenLogId: 0,
                _logsLimit: 0,

                postCreate: function () {
                    this.inherited(arguments);

                    this._logsLimit = this.maxRecords || 4096;
                    this.autoRefreshButton.on("change", lang.hitch(this, function (value) {
                        this._autoRefresh = value;
                        this.refreshButton.set("disabled", value);
                        if (value)
                        {
                            this._refreshGridEntries();
                        }
                    }));

                    this.refreshButton.on("click", lang.hitch(this, function () {
                        this._refreshGridEntries();
                    }));
                    this.clearFilterButton.on("click", lang.hitch(this, this._clearFilter));

                    this._logsStore = new MemoryStore({
                        data: [],
                        idProperty: 'id'
                    });

                    var userPreferences = this.userPreferences;
                    this._columns = [
                        {
                            label: 'ID',
                            field: "id",
                            hidden: false,
                            id: 0,
                            attributeName: "id",
                            type: "Long"
                        }, {
                            label: "Date",
                            field: "timestamp",
                            formatter: function (value) {
                                return userPreferences.formatDateTime(value, {appendTimeZone: true});
                            },
                            hidden: false,
                            id: 1,
                            attributeName: "timestamp",
                            type: "Date"
                        }, {
                            label: "Level",
                            field: "level",
                            hidden: true,
                            id: 2,
                            attributeName: "level",
                            validValues: ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]
                        }, {
                            label: "Logger",
                            field: "logger",
                            hidden: true,
                            id: 3,
                            attributeName: "logger"
                        }, {
                            label: "Thread",
                            field: "threadName",
                            hidden: true,
                            id: 4,
                            attributeName: "threadName"
                        }, {
                            label: "Log Message",
                            field: "message",
                            hidden: false,
                            id: 5,
                            attributeName: "message"
                        }
                    ];

                    this._createGrid();

                    this.columnChooser.on("change", lang.hitch(this, this._columnChanged));
                    this.filterBuilder.set("whereFieldsSelector", this.filterChooser);
                    this.filterBuilder.set("userPreferences", this.management.userPreferences);
                    this.filterBuilder.on("change", lang.hitch(this, this._filterChanged));

                    this.columnChooser.set("data", {
                        items: this._columns,
                        idProperty: "id",
                        selected: array.filter(this._columns, function (item) {
                            return !item.hidden;
                        }),
                        nameProperty: "label"
                    });
                    this.filterChooser.set("data", {
                        items: this._columns,
                        selected: [],
                        idProperty: "id",
                        nameProperty: "label"
                    });

                    this.limitButton.on("click", lang.hitch(this, this._showLimitDialog));
                    this.rowLimitDialog.on("execute", lang.hitch(this, function () {
                        var value = this.limit.get("value");
                        if (typeof value === 'number' || (typeof value === 'string' && util.isInteger(value) ) )
                        {
                            this._logsLimit = value;
                            this._deleteExcessiveRows();
                        }
                    }));


                    this._refreshGridEntries();
                },
                startup: function () {
                    this.inherited(arguments);
                    if (!this._started)
                    {
                        if (this._logsGrid)
                        {
                            this._logsGrid.startup();
                        }
                        this._started = true;
                    }
                },
                destroyRecursive: function (preserveDom) {
                    this.inherited(arguments);
                    if (this._logsGrid)
                    {
                        this._logsGrid.destroyRecursive(preserveDom);
                        this._logsGrid = null;
                    }
                    if (this._logsStore)
                    {
                        this._logsStore = null;
                    }
                    if (this.refreshButton)
                    {
                        this.refreshButton.destroyRecursive(preserveDom);
                        this.refreshButton = null;
                    }
                    if (this.autoRefreshButton)
                    {
                        this.autoRefreshButton.destroyRecursive(preserveDom);
                        this.autoRefreshButton = null;
                    }
                    if (this.clearFilterButton)
                    {
                        this.clearFilterButton.destroyRecursive(preserveDom);
                        this.clearFilterButton = null;
                    }
                    if (this.columnChooser)
                    {
                        this.columnChooser.destroyRecursive(preserveDom);
                        this.columnChooser = null;
                    }
                    if (this.filterChooser)
                    {
                        this.filterChooser.destroyRecursive(preserveDom);
                        this.filterChooser = null;
                    }
                    if (this.filterBuilder)
                    {
                        this.filterBuilder.destroyRecursive(preserveDom);
                        this.filterBuilder = null;
                    }
                    if (this.rowLimitDialog)
                    {
                        this.rowLimitDialog.destroyRecursive(preserveDom);
                        this.rowLimitDialog = null;
                    }
                },

                updateLogs: function () {
                    if (this._autoRefresh)
                    {
                        this._refreshGridEntries();
                    }
                },

                _refreshGridEntries: function (useFetched) {
                    if (!useFetched)
                    {
                        var modelObj = {
                            "name": "getLogEntries",
                            type: this.modelObj.type,
                            parent: this.modelObj
                        };

                        this.management
                            .load(modelObj, {lastLogId: this._lastSeenLogId})
                            .then(lang.hitch(this, this._updateLogEntries));
                    }
                    else
                    {
                        this._refreshGridStore();
                    }
                },

                _refreshGridStore: function () {
                    var store = this._logsStore;
                    if (store != null)
                    {
                        if (this._filter)
                        {
                            store = store.filter(this._filter);
                        }
                        if (this._logsGrid)
                        {
                            this._logsGrid.set("collection", store)
                        }
                    }

                },

                _updateLogEntries: function (logs) {
                    var store = this._logsStore;
                    if (store != null && logs)
                    {
                        for (var i = 0; i < logs.length; i++)
                        {
                            store.put(logs[i]);
                            this._lastSeenLogId = logs[i].id;
                        }

                        this._deleteExcessiveRows();

                        this._refreshGridStore();
                    }
                    this._refreshUpdateTime();
                },

                _deleteExcessiveRows: function () {
                    var idToDelete = this._lastSeenLogId - this._logsLimit;
                    var store = this._logsStore;
                    if (idToDelete > 0 && store != null)
                    {
                        var items = [];
                        store.filter(new store.Filter().lt("id", idToDelete))
                            .forEach(function (item) {
                                items.push(item.id);
                            });
                        for (var i = items.length - 1; i >= 0; i--)
                        {
                            store.remove(items[i]);
                        }
                    }
                },

                _columnChanged: function (selectedColumns) {
                    for (var i = 0; i < this._columns.length; i++)
                    {
                        var selected = false;
                        for (var it = 0; it < selectedColumns.length; it++)
                        {
                            var item = selectedColumns[it];
                            if (item.id === this._columns[i].id)
                            {
                                selected = true;
                                break;
                            }
                        }
                        this._columns[i].hidden = !selected;
                        this._logsGrid.toggleColumnHiddenState(i, !selected);
                    }
                },

                _columnStateChange: function (event) {
                    for (var i = 0; i < this._columns.length; i++)
                    {
                        if (event.column.label === this._columns[i].label)
                        {
                            this._columns[i].hidden = event.hidden;
                            break;
                        }
                    }

                    var selectedItems = [];
                    for (var j = 0; j < this._columns.length; j++)
                    {
                        if (!this._columns[j].hidden)
                        {
                            selectedItems.push(this._columns[j]);
                        }
                    }

                    this.columnChooser.set("data", {selected: selectedItems});
                },

                _filterChanged: function (event) {
                    var conditions = event.conditions;
                    var conditionsSet = conditions && conditions.hasOwnProperty("conditions")
                                        && conditions.conditions.length > 0;
                    this.clearFilterButton.set("disabled", !conditionsSet);
                    this.filterBuilder.domNode.style.display = conditionsSet ? "" : "none";
                    this._filter = this._buildFilter(conditions);
                    this._refreshGridEntries(true);
                },

                _showLimitDialog: function () {
                    this.limit.set("value", this._logsLimit);
                    this.rowLimitDialog.show();
                },

                _createGrid: function () {
                    var LogGrid = declare([Grid,
                                           Keyboard,
                                           Selection,
                                           Pagination,
                                           ColumnResizer,
                                           ColumnHider,
                                           DijitRegistry]);

                    this._logsGrid = new LogGrid({
                        rowsPerPage: 10,
                        selectionMode: 'none',
                        deselectOnRefresh: false,
                        allowSelectAll: true,
                        cellNavigation: true,
                        className: 'dgrid-autoheight',
                        pageSizeOptions: [10, 20, 30, 40, 50, 100],
                        adjustLastColumn: true,
                        collection: this._logsStore,
                        //highlightRow: function () {},
                        columns: lang.clone(this._columns),
                        sort: "id"
                    }, this.logEntries);
                    this._logsGrid.on("dgrid-columnstatechange", lang.hitch(this, this._columnStateChange));
                },

                _clearFilter: function () {
                    this.filterBuilder.clearWhereCriteria();
                    this.clearFilterButton.set("disabled", true);
                    this.filterBuilder.domNode.style.display = "none";
                    this._filter = null;
                    this._refreshGridEntries(true);
                },

                _refreshUpdateTime: function () {
                    if (this.userPreferences)
                    {
                        var formatOptions = {
                            selector: "time",
                            timePattern: "HH:mm:ss.SSS",
                            appendTimeZone: true,
                            addOffset: true
                        };
                        var updateTime = new Date();
                        this.updateTimeNode.innerHTML =
                            this.userPreferences.formatDateTime(updateTime.getTime(), formatOptions);
                        this.browserTimeNode.innerHTML = locale.format(updateTime, formatOptions);
                    }
                },

                _buildFilter: function (conditions) {

                    var filters = [];
                    if (conditions && conditions.hasOwnProperty("conditions"))
                    {
                        var items = conditions.conditions;
                        for (var i = 0; i < items.length; i++)
                        {
                            var item = items[i];
                            var f = null;
                            if (item.hasOwnProperty("conditions"))
                            {
                                f = this._buildFilter(item);
                            }
                            else
                            {
                                f = this._buildFilterForOperator(item.operator, item.type, item.name, item.value);
                            }
                            if (f)
                            {
                                filters.push(f);
                            }
                        }
                    }

                    var filter;
                    for (var j = 0; j < filters.length; j++)
                    {
                        if (filter)
                        {
                            if (conditions.operator === "or")
                            {

                                filter = filter.or(filter, filters[j])
                            }
                            else if (conditions.operator === "and")
                            {
                                filter = filter.and(filter, filters[j])
                            }
                        }
                        else
                        {
                            filter = filters[j];
                        }
                    }
                    return filter;
                },

                _buildFilterForOperator: function (operator, type, name, value) {
                    var f;
                    var val = this._getConditionValue(type, value);
                    switch (operator)
                    {
                        case '=':
                        {
                            f = new this._logsStore.Filter().eq(name, val);
                            break;
                        }
                        case '<>':
                        {
                            f = new this._logsStore.Filter().ne(name, val);
                            break;
                        }
                        case '>':
                        {
                            f = new this._logsStore.Filter().gt(name, val);
                            break;
                        }
                        case '<':
                        {
                            f = new this._logsStore.Filter().lt(name, val);
                            break;
                        }
                        case '>=':
                        {
                            f = new this._logsStore.Filter().gte(name, val);
                            break;
                        }
                        case '<=':
                        {
                            f = new this._logsStore.Filter().lte(name, val);
                            break;
                        }
                        case "contains":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^.*" + escapeRegExp(val) + ".*$", "i"));
                            }
                            break;
                        }
                        case "starts with":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^" + escapeRegExp(val) + ".*$", "i"));
                            }
                            break;
                        }
                        case "ends with":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^.*" + escapeRegExp(val) + "$", "i"));
                            }
                            break;
                        }
                        case "not contains":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^((?!" + escapeRegExp(val) + ").)*$", "i"));
                            }
                            break;
                        }
                        case "not starts with":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^(?!" + escapeRegExp(val) + ".*$).*$", "i"));
                            }
                            break;
                        }
                        case "not ends with":
                        {
                            if (typeof value === 'string' || value instanceof String)
                            {
                                f = new this._logsStore.Filter().match(name,
                                    new RegExp("^(.(?!" + escapeRegExp(val) + "$))+$", "i"));
                            }
                            break;
                        }
                        case "is null":
                        {
                            f = new this._logsStore.Filter().eq(name, null);
                            f = f.or(f, f.eq(name, undefined));
                            break;
                        }
                        case "is not null":
                        {
                            f = new this._logsStore.Filter().ne(name, null);
                            f = f.and(f, f.ne(name, undefined));
                            break;
                        }
                        case "in":
                        {
                            if (Array.isArray(val))
                            {
                                f = new this._logsStore.Filter()["in"](name, val);
                            }
                            break;
                        }
                        case "not in":
                        {
                            if (Array.isArray(val))
                            {
                                var fltr = function (data) {
                                    return val.indexOf(data[name]) === -1
                                };
                                f = new this._logsStore.Filter(fltr);
                            }
                            break;
                        }
                    }
                    return f;
                },
                _getConditionValue: function (type, value) {
                    if (type === "Date" && (typeof value === 'string' || value instanceof String))
                    {
                        var groups = value.match(/^to_date\('(.*)'\)$/i);
                        if (groups && groups.length === 2)
                        {
                            value = Date.parse(groups[1]);
                        }
                    }
                    return value;
                }
            });
    });
