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
        "dojo/dom-construct",
        "dojo/json",
        "dojo/text!query/QueryBuilder.html",
        "dojox/html/entities",
        "dgrid/Grid",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/Selector",
        "dgrid/extensions/ColumnReorder",
        "dgrid/extensions/ColumnHider",
        "dstore/Memory",
        'dstore/legacy/DstoreAdapter',
        "qpid/management/query/QueryGrid",
        "qpid/management/query/DropDownSelect",
        "qpid/management/query/WhereExpression",
        "dojo/Evented",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/FilteringSelect",
        "dijit/form/ComboBox",
        "dijit/form/Button",
        "dijit/form/ComboButton",
        "dijit/form/CheckBox",
        "dijit/form/DropDownButton",
        "dijit/form/NumberTextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/Select",
        "dijit/form/SimpleTextarea",
        "dijit/Menu",
        "dijit/MenuItem",
        "dijit/Toolbar",
        "dijit/TooltipDialog",
        "dijit/Dialog",
        "dojo/Deferred",
        "qpid/management/query/MessageDialog"],
    function (declare,
              lang,
              parser,
              domConstruct,
              json,
              template,
              entities,
              Grid,
              Keyboard,
              Selection,
              Pagination,
              Selector,
              ColumnReorder,
              ColumnHider,
              Memory,
              DstoreAdapter,
              QueryGrid)
    {
        var predefinedCategories = [{
            id: "queue",
            name: "Queue"
        }, {
            id: "connection",
            name: "Connection"
        }];

        var QueryBuilder = declare("qpid.management.query.QueryBuilder",
            [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                /**
                 * Fields from template
                 **/
                scope: null,
                categoryName: null,
                advancedSearch: null,
                advancedSelect: null,
                advancedWhere: null,
                standardSearch: null,
                standardSelectChooser: null,
                standardWhereChooser: null,
                searchButton: null,
                modeButton: null,
                standardWhereExpressionBuilder: null,
                queryResultGrid: null,
                advancedOrderBy: null,

                /**
                 * constructor parameter
                 */
                management: null,
                controller: null,
                parentObject: null,

                /**
                 * Inner fields
                 */
                _standardMode: true,
                _scopeModelObjects: {},
                _categorySelector: null,
                _searchScopeSelector: null,
                _lastStandardModeSelect: [],
                _lastHeaders: [],

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                _postCreate: function ()
                {
                    var promise = this._createScopeList();
                    promise.then(lang.hitch(this, this._postCreateScope));
                },
                _postCreateScope: function ()
                {
                    this._createCategoryList();

                    // advanced mode widgets
                    this.advancedSelect.on("change", lang.hitch(this, this._toggleSearchButton));
                    this.advancedSelect.on("blur", lang.hitch(this, this._advancedModeSelectChanged));
                    this.advancedWhere.on("blur", lang.hitch(this, this._advancedModeWhereChanged));
                    this.advancedOrderBy.on("blur", lang.hitch(this, this._advancedModeOrderByChanged));
                    this.advancedSelect.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));
                    this.advancedWhere.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));
                    this.advancedOrderBy.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));

                    // standard mode widgets
                    this.standardSelectChooser.on("change", lang.hitch(this, this._standardModeSelectChanged));
                    this.standardSelectChooser.startup();
                    this.standardWhereChooser.startup();
                    this.standardWhereExpressionBuilder.set("whereFieldsSelector", this.standardWhereChooser);
                    this.standardWhereExpressionBuilder.set("userPreferences", this.management.userPreferences);
                    this.standardWhereExpressionBuilder.startup();
                    this.standardWhereExpressionBuilder.on("change", lang.hitch(this, this._standardModeWhereChanged));

                    // search & mode buttons
                    this.searchButton.on("click", lang.hitch(this, this.search));
                    this.modeButton.on("click", lang.hitch(this, this._showModeSwitchWarningIfRequired));

                    this._buildGrid();
                    this._categoryChanged(this._categorySelector.value);
                    this._toggleSearchButton();
                },
                search: function ()
                {
                    var scope = this._searchScopeSelector.value;
                    this._resultsGrid.setParentObject(this._scopeModelObjects[scope]);
                    this._resultsGrid.refresh();
                },
                getDefaultColumns: function(category)
                {
                    return ["id", "name"];
                },
                _showModeSwitchWarningIfRequired: function ()
                {
                    var userPreferences = this.management.userPreferences;
                    var displayWarning = (!userPreferences || !userPreferences.query
                                          || (userPreferences.query.displaySwitchModeWarning == undefined
                                          || userPreferences.query.displaySwitchModeWarning));
                    if (this._standardMode && displayWarning && QueryBuilder.showWarningOnModeChange)
                    {
                        if (!this._switchModeWarningDialog)
                        {
                            var formattedMessage = "<div>Copying of query settings is only supported on switching from Standard view into Advanced view!<br/>"
                                                   + "Switching back from Advanced view into Standard view will completely reset the query.<br/><br/>"
                                                   + "Are you sure you want to switch from Standard view into Advanced view?"
                                                   "</div>";
                            this._switchModeWarningDialog = new qpid.management.query.MessageDialog({
                                title: "Warning!",
                                message: formattedMessage
                            }, domConstruct.create("div"));
                            this._switchModeWarningDialog.on("execute", lang.hitch(this, function (stopDisplaying)
                            {
                                if (stopDisplaying)
                                {
                                    if (!userPreferences.query)
                                    {
                                        userPreferences.query = {};
                                    }
                                    userPreferences.query.displaySwitchModeWarning = false;
                                    userPreferences.save({query: userPreferences.query}, null, function (error)
                                    {
                                        console.log("Saving user preferences failed: " + error);
                                    });
                                }
                                else
                                {
                                    QueryBuilder.showWarningOnModeChange = false;
                                }
                                this._modeChanged();
                            }));
                        }
                        this._switchModeWarningDialog.show();
                    }
                    else
                    {
                        this._modeChanged();
                    }
                },
                _setSelectClause: function (select)
                {
                    this._resultsGrid.setSelect(select ? select + ",id" : "");
                },
                _advancedModeSelectChanged: function ()
                {
                    this._setSelectClause(this.advancedSelect.value);
                },
                _advancedModeWhereChanged: function ()
                {
                    this._resultsGrid.setWhere(this.advancedWhere.value);
                },
                _advancedModeOrderByChanged: function ()
                {
                    this._resultsGrid.setOrderBy(this.advancedOrderBy.value);
                },
                _toggleSearchButton: function (select)
                {
                    var criteriaNotSet = !select;
                    this.searchButton.set("disabled", criteriaNotSet);
                    this.searchButton.set("title",
                        criteriaNotSet ? "Please, choose fields to display in order to enable search" : "Search");
                },
                _buildSelectExpression: function (value)
                {
                    var expression = "";
                    if (lang.isArray(value))
                    {
                        for (var i = 0; i < value.length; i++)
                        {
                            var selection = value[i] && value[i].hasOwnProperty("attributeName")
                                ? value[i].attributeName
                                : value[i];
                            expression = expression + (i > 0 ? "," : "") + selection;
                        }
                    }
                    return expression;
                },
                _normalizeSorting: function (selectedColumns)
                {
                    var newSort = [];
                    var sort = this._resultsGrid.getSort();
                    for (var i = 0; i < sort.length; ++i)
                    {
                        var sortColumnIndex = parseInt(sort[i].property) - 1;
                        var sortDescending = sort[i].descending;
                        if (sortColumnIndex < this._lastStandardModeSelect.length)
                        {
                            var oldSortedColumnName = this._lastStandardModeSelect[sortColumnIndex].attributeName;
                            for (var j = 0; j < selectedColumns.length; ++j)
                            {
                                if (selectedColumns[j].attributeName === oldSortedColumnName)
                                {
                                    newSort.push({
                                        property: "" + (j + 1),
                                        descending: sortDescending
                                    });
                                    break;
                                }
                            }
                        }
                    }
                    this._resultsGrid.setSort(newSort);
                },
                _processStandardModeSelectChange : function (selectedColumns)
                {
                    this._normalizeSorting(selectedColumns);
                    var selectClause = this._buildSelectExpression(selectedColumns);
                    this._setSelectClause(selectClause);
                    this._lastStandardModeSelect = lang.clone(selectedColumns);
                    this._toggleSearchButton(selectClause);
                },
                _standardModeSelectChanged: function (selectedColumns)
                {
                    this._processStandardModeSelectChange(selectedColumns);
                    this.search();
                },
                _standardModeColumnOrderChanged: function(event)
                {
                    if (this._standardMode)
                    {
                        var columnRow = event.subRow;
                        var selectedItems = this.standardSelectChooser.get("selectedItems");
                        var newSelectedItems = [];
                        for(var i = 0; i < columnRow.length; i++)
                        {
                            var field = parseInt(columnRow[i].field) - 1;
                            newSelectedItems.push(selectedItems[field]);
                        }
                        this._processStandardModeSelectChange(newSelectedItems);
                        this.standardSelectChooser.set("data", {"selected": newSelectedItems});
                    }
                    else
                    {
                        event.preventDefault();
                        event.stopPropagation();
                    }
                },
                _standardModeColumnStateChanged: function(event)
                {
                    if (event.hidden)
                    {
                        var checkNode = null;
                        if (this._resultsGrid._columnHiderCheckboxes && this._resultsGrid._columnHiderCheckboxes[event.column.id])
                        {
                            checkNode = this._resultsGrid._columnHiderCheckboxes[event.column.id].parentNode;
                            checkNode.style.display = 'none';
                        }
                        try
                        {
                            var columnIndex = parseInt(event.column.field) - 1;
                            var newSelectedItems = this.standardSelectChooser.get("selectedItems");
                            newSelectedItems.splice(columnIndex, 1);
                            this._processStandardModeSelectChange(newSelectedItems);
                            this.standardSelectChooser.set("data", {"selected": newSelectedItems});
                            this._resultsGrid.refresh();
                        }
                        finally
                        {
                            if (checkNode)
                            {
                                checkNode.style.display = '';
                            }
                        }
                    }
                },
                _standardModeWhereChanged: function (result)
                {
                    this._resultsGrid.setWhere(result);
                    this.search();
                },
                _buildGrid: function ()
                {
                    var grid = new (declare([QueryGrid,ColumnReorder,ColumnHider]))({
                        controller: this.controller,
                        management: this.management,
                        category: this._categorySelector.value.toLowerCase(),
                        parentObject: this._scopeModelObjects[this._searchScopeSelector.value],
                        zeroBased: false,
                        transformer: function (data)
                        {
                            var dataResults = data.results;

                            var results = [];
                            for (var i = 0, l = dataResults.length; i < l; ++i)
                            {
                                var result = dataResults[i];
                                var item = {id: result[result.length - 1]};

                                // excluding id, as we already added id field
                                for (var j = 0, rl = result.length - 1; j < rl; ++j)
                                {
                                    // sql uses 1-based index in ORDER BY
                                    var field = j + 1;
                                    item[new String(field)] = result[j];
                                }
                                results.push(item);
                            }
                            return results;
                        }
                    }, this.queryResultGrid);
                    grid.on('dgrid-refresh-complete', lang.hitch(this, function ()
                    {
                        this._resultsGrid.setUseCachedResults(false);
                    }));
                    grid.on('queryCompleted', lang.hitch(this, this._buildColumnsIfHeadersChanged));
                    grid.on('orderByChanged', lang.hitch(this, function (event)
                    {
                        this.advancedOrderBy.set("value", event.orderBy);
                    }));
                    grid.on('dgrid-columnreorder', lang.hitch(this, this._standardModeColumnOrderChanged));
                    grid.on('dgrid-columnstatechange', lang.hitch(this, this._standardModeColumnStateChanged))
                    grid.hiderToggleNode.title = "Remove columns";
                    this._resultsGrid = grid;
                    this._resultsGrid.startup();
                },
                _buildColumnsIfHeadersChanged: function (event)
                {
                    var headers = lang.clone(event.headers);
                    if (headers.length > 0)
                    {
                        headers.pop();
                    }
                    if (!this._equalStringArrays(headers, this._lastHeaders))
                    {
                        this._lastHeaders = headers;
                        this._resultsGrid.setUseCachedResults(true);
                        this._resultsGrid.hiderToggleNode.style.display = this._standardMode && headers.length > 0 ? '' : 'none';
                        this._resultsGrid.set("columns", this._getColumns(headers));
                        this._resultsGrid.resize();
                    }
                },
                _equalStringArrays: function (a, b)
                {
                    if (a.length != b.length)
                    {
                        return false;
                    }
                    for (var i = 0; i < a.length; ++i)
                    {
                        if (a[i] != b[i])
                        {
                            return false;
                        }
                    }
                    return true;
                },
                _getColumns: function (headers)
                {
                    var columns = [];
                    if (headers)
                    {
                        for (var i = 0; i < headers.length; ++i)
                        {
                            var attribute = headers[i];
                            var column = {
                                label: attribute,
                                field: "" + (i + 1),
                                sortable: true,
                                reorderable: !!this._standardMode,
                                unhidable: !this._standardMode
                            };
                            columns.push(column);
                            if (this._columns)
                            {
                                var columnData = this._columns[attribute];
                                if (columnData)
                                {
                                    if (columnData.type == "Date")
                                    {
                                        var that = this;
                                        column.formatter = function (value, object)
                                        {
                                            if (!isNaN(value) && parseInt(Number(value)) == value && !isNaN(parseInt(
                                                    value,
                                                    10)))
                                            {
                                                return that.management.userPreferences.formatDateTime(value, {
                                                    addOffset: true,
                                                    appendTimeZone: true
                                                });
                                            }
                                            return value ? entities.encode(String(value)) : "";
                                        };
                                    }
                                    else if (columnData.type == "Map")
                                    {
                                        column.renderCell = function (object, value, node)
                                        {
                                            if (value)
                                            {
                                                var list = domConstruct.create("div", {}, node);
                                                for (var i in value)
                                                {
                                                    domConstruct.create("div", {
                                                        innerHTML: entities.encode(String(i)) + ": " + entities.encode(
                                                            json.stringify(value[i]))
                                                    }, list);
                                                }
                                                return list;
                                            }
                                            return "";
                                        };
                                    }
                                    else if (columnData.type == "List" || columnData.type == "Set")
                                    {
                                        column.renderCell = function (object, value, node)
                                        {
                                            if (value)
                                            {
                                                var list = domConstruct.create("div", {}, node);
                                                for (var i in value)
                                                {
                                                    domConstruct.create("div", {
                                                        innerHTML: entities.encode(json.stringify(value[i]))
                                                    }, list)
                                                }
                                                return list;
                                            }
                                            return "";
                                        };
                                    }
                                }
                            }
                        }
                    }
                    return columns;
                },
                _createScopeList: function ()
                {
                    var that = this;
                    var result = this.management.query({
                        select: "id, $parent.name as parentName, name",
                        category: "virtualhost"
                    });
                    var deferred = new dojo.Deferred();
                    result.then(function (data)
                    {
                        try
                        {
                            that._scopeDataReceived(data);
                        }
                        finally
                        {
                            deferred.resolve(that._searchScopeSelector);
                        }
                    }, function (error)
                    {
                        deferred.reject(null);
                        console.error(error.message ? error.message : error);
                    });
                    return deferred.promise;
                },
                _scopeDataReceived: function (result)
                {
                    this._scopeModelObjects = {};
                    var defaultValue = undefined;
                    var items = [{
                        id: undefined,
                        name: "Broker"
                    }];
                    var data = result.results;
                    for (var i = 0; i < data.length; i++)
                    {
                        var name = data[i][2];
                        var parentName = data[i][1];
                        items.push({
                            id: data[i][0],
                            name: "VH:" + parentName + "/" + name
                        });
                        this._scopeModelObjects[data[i][0]] = {
                            name: name,
                            type: "virtualhost",
                            parent: {
                                name: parentName,
                                type: "virtualhostnode",
                                parent: {type: "broker"}
                            }
                        };
                        if (this.parentObject && this.parentObject.type == "virtualhost" && this.parentObject.name
                                                                                            == name
                            && this.parentObject.parent && this.parentObject.parent.name == parentName)
                        {
                            defaultValue = data[i][0];
                        }
                    }

                    var scopeStore = new DstoreAdapter(new Memory({
                        data: items,
                        idProperty: 'id'
                    }));
                    this._searchScopeSelector = new dijit.form.FilteringSelect({
                        name: "scope",
                        placeHolder: "Select search scope",
                        store: scopeStore,
                        value: defaultValue,
                        required: false
                    }, this.scope);
                    this._searchScopeSelector.startup();
                },
                _createCategoryList: function ()
                {
                    var categoryStore = new DstoreAdapter(new Memory({
                        idProperty: "id",
                        data: predefinedCategories
                    }));
                    var categoryList = new dijit.form.ComboBox({
                        name: "category",
                        placeHolder: "Select Category",
                        store: categoryStore,
                        value: this._category || "Queue",
                        required: true,
                        invalidMessage: "Invalid category specified"
                    }, this.categoryName);
                    categoryList.startup();
                    categoryList.on("change", lang.hitch(this, this._categoryChanged));
                    this._categorySelector = categoryList;
                },
                _categoryChanged: function (value)
                {
                    var metadata = this._getCategoryMetadata(value);
                    var columns, items, selectedItems;
                    if (!metadata)
                    {
                        dijit.showTooltip(this._categorySelector.get("invalidMessage"),
                            this._categorySelector.domNode,
                            this._categorySelector.get("tooltipPosition"),
                            !this._categorySelector.isLeftToRight());
                        columns = {};
                        items = [];
                        selectedItems = [];
                    }
                    else
                    {
                        var data = this._combineTypeAttributesAndStatistics(metadata);
                        columns = data.asObject;
                        items = data.asArray;
                        selectedItems = this.getDefaultColumns(value);
                    }

                    this.standardSelectChooser.set("data", {
                        items: items,
                        idProperty: "id",
                        selected: selectedItems,
                        nameProperty: "attributeName"
                    });
                    this.standardWhereChooser.set("data", {
                        items: items,
                        selected: [],
                        idProperty: "id",
                        nameProperty: "attributeName"
                    });
                    this.standardWhereExpressionBuilder.clearWhereCriteria();
                    this.advancedWhere.set("value", "");
                    this.advancedOrderBy.set("value", "");
                    this._columns = columns;
                    this._lastStandardModeSelect = this.standardSelectChooser.get("selectedItems");
                    var select = this._buildSelectExpression(this._lastStandardModeSelect);
                    this.advancedSelect.set("value", select);
                    this._setSelectClause(select);
                    this._resultsGrid.setWhere("");
                    this._resultsGrid.setOrderBy("");
                    this._resultsGrid.setSort([]);
                    this._resultsGrid.setCategory(value);
                    var disableMetadataDependant = !metadata;
                    this.standardWhereChooser.set("disabled", disableMetadataDependant);
                    this.standardSelectChooser.set("disabled", disableMetadataDependant);
                    this.modeButton.set("disabled", disableMetadataDependant);
                    this.advancedSelect.set("disabled", disableMetadataDependant);
                    this.advancedWhere.set("disabled", disableMetadataDependant);
                    this.advancedOrderBy.set("disabled", disableMetadataDependant);
                    this.searchButton.set("disabled", disableMetadataDependant);
                    this.search();
                },
                _advancedModeKeyPressed: function (evt)
                {
                    var key = evt.keyCode;
                    if (key == dojo.keys.ENTER)
                    {
                        evt.preventDefault();
                        evt.stopPropagation();
                        this._setSelectClause(this.advancedSelect.value);
                        this._resultsGrid.setWhere(this.advancedWhere.value);
                        this._resultsGrid.setOrderBy(this.advancedOrderBy.value);
                        this.search();
                    }
                },
                _modeChanged: function ()
                {
                    this._standardMode = !this._standardMode;
                    if (!this._standardMode)
                    {
                        this.modeButton.set("label", "Standard");
                        this.modeButton.set("title", "Switch to 'Standard' search");
                        this.advancedSelect.set("disabled", false);
                        this.advancedWhere.set("disabled", false);
                        this.standardSearch.style.display = "none";
                        this.standardWhereExpressionBuilder.domNode.style.display = "none";
                        this.advancedSearch.style.display = "";
                        this.advancedSelect.set("value",
                            this._buildSelectExpression(this.standardSelectChooser.get("selectedItems")));
                        this.advancedWhere.set("value", this._resultsGrid.getWhere());
                        this.advancedOrderBy.set("value", this._resultsGrid.getOrderBy());

                        this._resultsGrid.hiderToggleNode.style.display = 'none';

                        // rebuild columns to disable column reordering and removal
                        if (this._lastHeaders && this._lastHeaders.length)
                        {
                            this._resultsGrid.setUseCachedResults(true);
                            this._resultsGrid.set("columns", this._getColumns(this._lastHeaders));
                            this._resultsGrid.resize();
                        }
                    }
                    else
                    {
                        this.modeButton.set("label", "Advanced");
                        this.modeButton.set("title", "Switch to 'Advanced' search using SQL-like expressions");
                        this.advancedSelect.set("disabled", true);
                        this.advancedWhere.set("disabled", true);
                        this.standardSearch.style.display = "";
                        this.standardWhereExpressionBuilder.domNode.style.display = "";
                        this.advancedSearch.style.display = "none";
                        var category = this._categorySelector.value;
                        var selectedItems = this.getDefaultColumns(category);
                        this.standardSelectChooser.set("data", {selected: selectedItems});
                        this.standardWhereChooser.set("data", {selected: []});
                        this.standardWhereExpressionBuilder.clearWhereCriteria();
                        this._lastStandardModeSelect = this.standardSelectChooser.get("selectedItems");
                        this._lastHeaders = [];
                        var select = this._buildSelectExpression(this._lastStandardModeSelect);
                        this._setSelectClause(select);
                        this._resultsGrid.setWhere("");
                        this._resultsGrid.setOrderBy("");
                        this._resultsGrid.setSort([]);
                        this._toggleSearchButton(select);
                        this._resultsGrid.hiderToggleNode.style.display = '';
                        this.search();
                    }
                },
                _getCategoryMetadata: function (value)
                {
                    if (value)
                    {
                        var category = value.charAt(0)
                                           .toUpperCase() + value.substring(1);
                        return this.management.metadata.metadata[category];
                    }
                    else
                    {
                        return undefined;
                    }
                },
                _combineTypeAttributesAndStatistics: function (metadata)
                {
                    var columnsArray = [];
                    var columnsObject = {};
                    var validTypes = [];
                    var typeAttribute = null;
                    for (var i in metadata)
                    {
                        validTypes.push(i);
                        var categoryType = metadata[i];
                        var attributes = categoryType.attributes;
                        for (var name in attributes)
                        {
                            var attribute = attributes[name];
                            if (!(name in columnsObject))
                            {
                                var attributeData = {
                                    id: name,
                                    attributeName: name,
                                    type: attribute.type,
                                    validValues: attribute.validValues,
                                    description: attribute.description,
                                    columnType: "attribute"
                                };
                                if (name === "type")
                                {
                                    typeAttribute = attributeData;
                                }
                                columnsObject[name] = attributeData;
                                columnsArray.push(attributeData);
                            }
                        }

                        var statistics = categoryType.statistics;
                        for (var name in statistics)
                        {
                            var statistic = statistics[name];
                            if (!(name in columnsObject))
                            {
                                var statisticData = {
                                    id: name,
                                    attributeName: name,
                                    type: statistic.type,
                                    description: statistic.description,
                                    columnType: "statistics"
                                };
                                columnsArray.push(statisticData);
                                columnsObject[name] = statisticData;
                            }
                        }
                    }
                    if (typeAttribute != null && !typeAttribute.validValues)
                    {
                        typeAttribute.validValues = validTypes;
                    }
                    return {
                        asArray: columnsArray,
                        asObject: columnsObject
                    };
                }
            });

        QueryBuilder.showWarningOnModeChange = true;

        return QueryBuilder;
    });
