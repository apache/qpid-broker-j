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
        "dojo/Evented",
        "dojo/text!query/QueryWidget.html",
        "dojo/text!query/QueryCloneDialogForm.html",
        "qpid/management/preference/PreferenceSaveDialogContent",
        "dojo/store/Memory",
        "dijit/registry",
        "dojox/html/entities",
        "dojox/uuid/generateRandomUuid",
        "dgrid/extensions/ColumnReorder",
        "dgrid/extensions/ColumnHider",
        "qpid/management/query/QueryGrid",
        "qpid/common/MessageDialog",
        "dojox/uuid/generateRandomUuid",
        "qpid/management/query/DropDownSelect",
        "qpid/management/query/WhereExpression",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/form/ValidationTextBox",
        "dijit/form/SimpleTextarea",
        "dijit/Toolbar",
        "dijit/Dialog"],
    function (declare,
              lang,
              parser,
              domConstruct,
              json,
              Evented,
              template,
              queryCloneDialogFormTemplate,
              PreferenceSaveDialogContent,
              Memory,
              registry,
              entities,
              generateRandomUuid,
              ColumnReorder,
              ColumnHider,
              QueryGrid,
              MessageDialog,
              uuid)
    {

        var QueryCloneDialogForm = declare("qpid.management.query.QueryCloneDialogForm",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                /**
                 * dijit._TemplatedMixin enforced fields
                 */
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: queryCloneDialogFormTemplate.replace(/<!--[\s\S]*?-->/g, ""),

                /**
                 * template attach points
                 */
                scope: null,
                cloneQueryForm: null,
                okButton: null,
                cancelButton: null,

                /**
                 * constructor
                 */
                structure: null,

                // internal fields
                _scopeModelObjects: {},

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                _postCreate: function ()
                {
                    this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                    this.cloneQueryForm.on("submit", lang.hitch(this, this._onFormSubmit));
                    this.scope.on("change", lang.hitch(this, this._onChange));
                },
                _initScopeItems: function (defaultValue)
                {
                    var scopeItems = this.structure.getScopeItems();
                    this._scopeModelObjects = scopeItems.scopeModelObjects;

                    var scopeStore = new Memory({
                        data: scopeItems.items,
                        idProperty: 'id'
                    });
                    this.scope.set("store", scopeStore);

                    if (defaultValue)
                    {
                        for (var field in this._scopeModelObjects)
                        {
                            var item = this._scopeModelObjects[field];
                            if (item.id === defaultValue.id)
                            {
                                this.scope.set("value", item.id);
                                break;
                            }
                        }
                    }
                },
                _onCancel: function (data)
                {
                    this.emit("cancel");
                },
                _onChange: function (e)
                {
                    var invalid = !this._scopeModelObjects[this.scope.value];
                    this.okButton.set("disabled", invalid);
                },
                _onFormSubmit: function (e)
                {
                    try
                    {
                        if (this.cloneQueryForm.validate())
                        {
                            var parentObject = this._scopeModelObjects[this.scope.value];
                            this.emit("clone", {parentObject: parentObject});
                        }
                        else
                        {
                            alert('Form contains invalid data.  Please correct first');
                        }

                    }
                    finally
                    {
                        return false;
                    }
                }
            });

        var QueryWidget = declare("qpid.management.query.QueryWidget",
            [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                /**
                 * Fields from template
                 **/
                advancedSearch: null,
                advancedSelect: null,
                advancedWhere: null,
                advancedOrderBy: null,
                advancedSearchButton: null,
                standardSearch: null,
                standardSelectChooser: null,
                standardWhereChooser: null,
                standardSearchButton: null,
                standardWhereExpressionBuilder: null,
                modeButton: null,
                queryResultGrid: null,
                saveButton: null,
                cloneButton: null,
                deleteButton: null,
                saveButtonTooltip: null,
                cloneButtonTooltip: null,
                deleteButtonTooltip: null,
                searchForm: null,
                exportButton: null,
                exportButtonTooltip: null,

                /**
                 * constructor parameter
                 */
                management: null,
                controller: null,
                parentObject: null,
                preference: null,

                /**
                 * Inner fields
                 */
                _querySaveDialog: null,
                _standardMode: true,
                _lastStandardModeSelect: null,
                _lastHeaders: null,
                _queryCloneDialogForm: null,
                _ownQuery: false,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                startup: function ()
                {
                    this.inherited(arguments);
                    this.standardSelectChooser.startup();
                    this.standardWhereChooser.startup();
                    this.standardWhereExpressionBuilder.startup();
                    this._resultsGrid.startup();
                    this._querySaveDialog.startup();
                    this._queryCloneDialog.startup();
                },
                _postCreate: function ()
                {
                    var valuePresent = this.preference && this.preference.value;
                    var selectPresent = valuePresent && this.preference.value.select;
                    this.categoryName = valuePresent && this.preference.value.category ? this.preference.value.category : "Queue";
                    this._lastStandardModeSelect = [];
                    this._lastHeaders = [];

                    // lifecycle UI
                    this._queryCloneDialogForm = new QueryCloneDialogForm({structure : this.controller.structure});
                    this._queryCloneDialog =
                        new dijit.Dialog({title: "Clone query", content: this._queryCloneDialogForm});
                    this._queryCloneDialogForm.on("clone", lang.hitch(this, this._onQueryClone));
                    this._queryCloneDialogForm.on("cancel", lang.hitch(this, this._onQueryCloneCancel));

                    this._querySaveDialogForm = new PreferenceSaveDialogContent({management: this.management});
                    this._querySaveDialog = new dijit.Dialog({title: "Save query", content: this._querySaveDialogForm});
                    this._querySaveDialogForm.on("save", lang.hitch(this, this._onQuerySave));
                    this._querySaveDialogForm.on("cancel", lang.hitch(this._querySaveDialog, this._querySaveDialog.hide));

                    // lifecycle controls
                    this.saveButton.on("click", lang.hitch(this, this._saveQuery));
                    this.cloneButton.on("click", lang.hitch(this, this._cloneQuery));
                    this.deleteButton.on("click", lang.hitch(this, this._deleteQuery));
                    this.exportButton.on("click", lang.hitch(this, this._exportQueryResults));

                    this._ownQuery = !this.preference
                                     || !this.preference.owner
                                     || this.preference.owner === this.management.getAuthenticatedUser();
                    var newQuery = !this.preference || !this.preference.createdDate;
                    this.saveButton.set("disabled", !this._ownQuery);
                    this.deleteButton.set("disabled", !this._ownQuery || newQuery);
                    this.exportButton.set("disabled", true);

                    if (!this._ownQuery)
                    {
                        this.saveButtonTooltip.set("label", "The query belongs to another user.<br/>Use clone if you wish to make your own copy.");
                        this.deleteButtonTooltip.set("label", "This query belongs to another user.");
                    }

                    // advanced mode widgets
                    this.advancedSelect.on("change", lang.hitch(this, this._advancedModeSelectChanged));
                    this.advancedWhere.on("change", lang.hitch(this, this._advancedModeWhereChanged));
                    this.advancedOrderBy.on("change", lang.hitch(this, this._advancedModeOrderByChanged));
                    this.advancedSelect.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));
                    this.advancedWhere.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));
                    this.advancedOrderBy.on("keyDown", lang.hitch(this, this._advancedModeKeyPressed));

                    // standard mode widgets
                    this.standardSelectChooser.on("change", lang.hitch(this, this._standardModeSelectChanged));
                    this.standardWhereExpressionBuilder.set("whereFieldsSelector", this.standardWhereChooser);
                    this.standardWhereExpressionBuilder.set("userPreferences", this.management.userPreferences);
                    this.standardWhereExpressionBuilder.on("change", lang.hitch(this, this._standardModeWhereChanged));

                    // search & mode buttons
                    this.searchForm.on("submit", lang.hitch(this, function(){this.search(); return false;}));
                    this.modeButton.on("click", lang.hitch(this, this._showModeSwitchWarningIfRequired));

                    var rowsPerPage = valuePresent && this.preference.value.limit ? this.preference.value.limit  : 100;
                    var currentPage = valuePresent && this.preference.value.offset ?  this.preference.value.offset / rowsPerPage + 1: 1;
                    this._buildGrid(currentPage, rowsPerPage);
                    this._initStandardModeWidgets(this.categoryName, !selectPresent);

                    if (selectPresent)
                    {
                        this._standardMode = false;
                        this._configureModalWidgets(false);

                        this.advancedSelect.set("value", this.preference.value.select);
                        this._setSelectClause(this.advancedSelect.value);
                        if (this.preference.value.where )
                        {
                            this.advancedWhere.set("value", this.preference.value.where);
                            this._setWhereClause(this.advancedWhere.value);
                        }
                        if (this.preference.value.orderBy)
                        {
                            this.advancedOrderBy.set("value", this.preference.value.orderBy);
                            this._setOrderByClause(this.advancedOrderBy.value);
                        }

                        this._toggleSearchButton(this.preference.value.select);
                    }
                    else
                    {
                        this._toggleSearchButton(true);
                    }
                    this._lastQuery = this._getQuery();
                },
                search: function ()
                {
                    this._resultsGrid.refresh();
                },
                getDefaultColumns: function (category)
                {
                    return ["id", "name"];
                },
                destroyRecursive: function (arg)
                {
                    this.inherited(arguments);
                    if (this._queryCloneDialog)
                    {
                        this._queryCloneDialog.destroyRecursive();
                        this._queryCloneDialog = null;
                    }
                    if (this._querySaveDialog)
                    {
                        this._querySaveDialog.destroyRecursive();
                        this._querySaveDialog = null;
                    }
                },
                _showModeSwitchWarningIfRequired: function ()
                {
                    if (this._standardMode)
                    {
                        var warningMessage = "<div>Copying of query settings is only supported on switching from Standard view into Advanced view!<br/>"
                                             + "Switching back from Advanced view into Standard view will completely reset the query.<br/><br/>"
                                             + "Are you sure you want to switch from Standard view into Advanced view?";
                                             "</div>";
                        MessageDialog.confirm({
                            title: "Warning!",
                            message: warningMessage,
                            confirmationId: "query.confirmation.mode.change"
                        }, domConstruct.create("div"))
                            .then(lang.hitch(this, function ()
                            {
                                this._modeChanged();
                            }));
                    }
                    else
                    {
                        this._modeChanged();
                    }
                },
                _setSelectClause: function (select)
                {
                    this._selectClause = select;
                    this._resultsGrid.setSelect(select ? select + ",id" : "");
                },
                _setWhereClause: function (where)
                {
                    this._whereClause = where;
                    this._resultsGrid.setWhere(where);
                },
                _setOrderByClause: function (orderBy)
                {
                    this._orderByClause = orderBy;
                    this._resultsGrid.setOrderBy(orderBy);
                },
                _advancedModeSelectChanged: function ()
                {
                    this._setSelectClause(this.advancedSelect.value);
                    this._queryChanged();
                    this._submitIfEnterPressed();
                },
                _advancedModeWhereChanged: function ()
                {
                    this._setWhereClause(this.advancedWhere.value);
                    this._queryChanged();
                    this._submitIfEnterPressed();
                },
                _advancedModeOrderByChanged: function ()
                {
                    this._setOrderByClause(this.advancedOrderBy.value);
                    this._queryChanged();
                    this._submitIfEnterPressed();
                },
                _submitIfEnterPressed: function ()
                {
                    if (this._enterPressed)
                    {
                        this._enterPressed = false;
                        this.searchForm.submit();
                    }
                },
                _toggleSearchButton: function (select)
                {
                    var criteriaNotSet = !select;
                    this.advancedSearchButton.set("disabled", criteriaNotSet);
                    this.advancedSearchButton.set("title",
                        criteriaNotSet ? "Please, choose fields to display in order to enable search" : "Search");
                    this.standardSearchButton.set("disabled", criteriaNotSet);
                    this.standardSearchButton.set("title",
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
                    this._setOrderByClause(this._resultsGrid.setSort(newSort));
                },
                _processStandardModeSelectChange: function (selectedColumns)
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
                    this._queryChanged();
                },
                _standardModeColumnOrderChanged: function (event)
                {
                    if (this._standardMode)
                    {
                        var columnRow = event.subRow;
                        var selectedItems = this.standardSelectChooser.get("selectedItems");
                        var newSelectedItems = [];
                        for (var i = 0; i < columnRow.length; i++)
                        {
                            var field = parseInt(columnRow[i].field) - 1;
                            newSelectedItems.push(selectedItems[field]);
                        }
                        this._processStandardModeSelectChange(newSelectedItems);
                        this.standardSelectChooser.set("data", {"selected": newSelectedItems});
                        this._queryChanged();
                    }
                    else
                    {
                        event.preventDefault();
                        event.stopPropagation();
                    }
                },
                _standardModeColumnStateChanged: function (event)
                {
                    if (event.hidden)
                    {
                        var checkNode = null;
                        if (this._resultsGrid._columnHiderCheckboxes
                            && this._resultsGrid._columnHiderCheckboxes[event.column.id])
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
                            this._queryChanged();
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
                    this._setWhereClause(result.expression);
                    this.search();
                    this._queryChanged();
                },
                _buildGrid: function (currentPage, rowsPerPage)
                {
                    var Grid = declare([QueryGrid, ColumnReorder, ColumnHider],
                                      {
                                            _restoreCurrentPage : currentPage > 1,
                                            gotoPage:function (page)
                                            {
                                                if (this._restoreCurrentPage)
                                                {
                                                    return this.inherited(arguments, [currentPage]);
                                                }
                                                else
                                                {
                                                    return this.inherited(arguments);
                                                }
                                            }
                                      });
                    var grid = new Grid({
                        management: this.management,
                        category: this.categoryName.toLowerCase(),
                        parentObject: this.parentObject,
                        zeroBased: false,
                        rowsPerPage: rowsPerPage,
                        _currentPage: currentPage,
                        allowTextSelection: true,
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
                    grid.on('queryCompleted', lang.hitch(this, this._queryCompleted));
                    grid.on('orderByChanged', lang.hitch(this, function (event)
                    {
                        if (this._standardMode)
                        {
                            this._setOrderByClause(event.orderBy);
                        }
                        else
                        {
                            this.advancedOrderBy.set("value", event.orderBy);
                        }
                        this._queryChanged();
                    }));
                    grid.on('dgrid-columnreorder', lang.hitch(this, this._standardModeColumnOrderChanged));
                    grid.on('dgrid-columnstatechange', lang.hitch(this, this._standardModeColumnStateChanged));
                    grid.hiderToggleNode.title = "Remove columns";
                    grid.on('rowBrowsed', lang.hitch(this, function(event){this.controller.showById(event.id);}));
                    this._resultsGrid = grid;
                },
                _queryCompleted: function (e)
                {
                    this._buildColumnsIfHeadersChanged(e.data);
                    this.exportButton.set("disabled", !(e.data.total && e.data.total > 0));
                },
                _buildColumnsIfHeadersChanged: function (data)
                {
                    var headers = lang.clone(data.headers);
                    if (headers.length > 0)
                    {
                        headers.pop();
                    }
                    if (!this._equalStringArrays(headers, this._lastHeaders))
                    {
                        this._lastHeaders = headers;
                        this._resultsGrid.setUseCachedResults(true);
                        this._resultsGrid.hiderToggleNode.style.display =
                            this._standardMode && headers.length > 0 ? '' : 'none';
                        this._resultsGrid.set("columns", this._getColumns(headers));
                        this._resultsGrid.resize();
                        this._resultsGrid._restoreCurrentPage = false;
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
                _initStandardModeWidgets: function (category, isNew)
                {
                    var metadata = this._getCategoryMetadata(category);
                    var columns, items, selectedItems;
                    if (metadata)
                    {
                        var data = this._combineTypeAttributesAndStatistics(metadata);
                        columns = data.asObject;
                        items = data.asArray;
                        selectedItems = isNew ? this.getDefaultColumns(category) : [];

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
                        this._columns = columns;
                        this._lastStandardModeSelect = this.standardSelectChooser.get("selectedItems");
                        if (isNew)
                        {
                            var select = this._buildSelectExpression(this._lastStandardModeSelect);
                            this._setSelectClause(select);
                        }
                    }
                },
                _advancedModeKeyPressed: function (evt)
                {
                    var key = evt.keyCode;
                    if (key == dojo.keys.ENTER)
                    {
                        evt.preventDefault();
                        evt.stopPropagation();

                        // set flag for Enter being pressed
                        this._enterPressed = true;

                        // move focus out and back into widget to provoke triggering of on change event
                        this.advancedSearchButton.focus();
                        registry.getEnclosingWidget(evt.target).focus();
                    }
                },
                _modeChanged: function ()
                {
                    this._standardMode = !this._standardMode;
                    this._configureModalWidgets(this._standardMode);
                    if (!this._standardMode)
                    {
                        this.advancedSelect.set("value", this._selectClause);
                        this.advancedOrderBy.set("value", this._orderByClause);
                        this.advancedWhere.set("value", this._whereClause);

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
                        var category = this.categoryName;
                        var selectedItems = this.getDefaultColumns(category);
                        this.standardSelectChooser.set("data", {selected: selectedItems});
                        this.standardWhereChooser.set("data", {selected: []});
                        this.standardWhereExpressionBuilder.clearWhereCriteria();
                        this._lastStandardModeSelect = this.standardSelectChooser.get("selectedItems");
                        this._lastHeaders = [];
                        var select = this._buildSelectExpression(this._lastStandardModeSelect);
                        this._setSelectClause(select);
                        this._setWhereClause("");
                        this._setOrderByClause("");
                        this._toggleSearchButton(select);
                        this._resultsGrid.hiderToggleNode.style.display = '';
                        this.search();
                        this._queryChanged();
                    }
                },
                _configureModalWidgets: function(standardMode)
                {
                  if (standardMode)
                  {
                    this.modeButton.set("label", "Advanced View");
                    this.modeButton.set("title", "Switch to 'Advanced View' search using SQL-like expressions");
                    this.modeButton.set("iconClass", "advancedViewIcon ui-icon");
                    this.advancedSelect.set("disabled", true);
                    this.advancedWhere.set("disabled", true);
                    this.standardSearch.style.display = "";
                    this.standardWhereExpressionBuilder.domNode.style.display = "";
                    this.advancedSearch.style.display = "none";
                  }
                  else
                  {
                    this.modeButton.set("label", "Standard View");
                    this.modeButton.set("title", "Switch to 'Standard View' search");
                    this.modeButton.set("iconClass", "dijitIconApplication");
                    this.advancedSelect.set("disabled", false);
                    this.advancedWhere.set("disabled", false);
                    this.standardSearch.style.display = "none";
                    this.standardWhereExpressionBuilder.domNode.style.display = "none";
                    this.advancedSearch.style.display = "";
                    this._resultsGrid.hiderToggleNode.style.display = 'none';
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
                },
                _getQuery: function ()
                {
                    return {
                        select: this._selectClause,
                        where: this._whereClause,
                        orderBy: this._orderByClause,
                        category: this.categoryName
                    };
                },
                _saveQuery: function ()
                {
                    var queryParameters = this._getQuery();
                    var preference = lang.clone(this.preference);
                    preference.type = "query";
                    preference.value = queryParameters;
                    this._querySaveDialogForm.set("preference", preference);
                    this._querySaveDialog.show();
                },
                _onQuerySave: function (e)
                {
                    var saveResponse = management.savePreference(this.parentObject, e.preference);
                    saveResponse.then(lang.hitch(this, function ()
                    {
                        var responsePromise = this._loadPreference(e.preference.name);
                        responsePromise.then(lang.hitch(this, function (preference)
                        {
                            this.preference = preference;
                            this._querySaveDialog.hide();
                            this.emit("save", {preference: preference});
                            this.deleteButton.set("disabled", false);
                        }));
                    }));
                },
                _cloneQuery: function ()
                {
                    this._queryCloneDialogForm._initScopeItems(this.parentObject);
                    this._queryCloneDialog.show();
                },
                _onQueryClone: function (e)
                {
                    var preference = {id : generateRandomUuid(), type: "query", value: this._getQuery()};
                    this._queryCloneDialog.hide();
                    this.emit("clone", {preference: preference, parentObject: e.parentObject});
                },
                _onQueryCloneCancel: function ()
                {
                    this._queryCloneDialog.hide();
                },
                _deleteQuery: function ()
                {
                    MessageDialog.confirm({
                        title: "Discard query?",
                        message: "Are you sure you want to delete the query?",
                        confirmationId: "query.confirmation.delete"
                    }).then(lang.hitch(this, function ()
                    {
                        if (this.preference.id)
                        {
                            var deletePromise = this.management.deletePreference(this.parentObject,
                                this.preference.type,
                                this.preference.name);
                            deletePromise.then(lang.hitch(this, function (preference)
                            {
                                this.emit("delete");
                            }));
                        }
                        else
                        {
                            this.emit("delete");
                        }
                    }));
                },

                _loadPreference: function (name)
                {
                    return this.management.getPreference(this.parentObject, "query", name)
                },
                _queryChanged: function()
                {
                    if (this._ownQuery)
                    {
                        var query = this._getQuery();

                        if (this._lastQuery.orderBy !== query.orderBy
                            || this._lastQuery.where !== query.where
                            || this._lastQuery.select !== query.select)
                        {
                            var pref = lang.clone(this.preference);
                            pref.value = query;
                            this._lastQuery = lang.clone(query);
                            this.emit("change", {preference: pref});
                        }
                    }
                },
                _exportQueryResults: function () {
                    var query = this._getQuery();
                    delete query.category;
                    var params = {};
                    for(var fieldName in query)
                    {
                        if (query.hasOwnProperty(fieldName) && query[fieldName])
                        {
                            params[fieldName] = query[fieldName];
                        }
                    }
                    params.format = "csv";
                    var id = uuid();
                    params.contentDispositionAttachmentFilename ="query-results-" + id + ".csv";
                    var url = this.management.getQueryUrl({category: this.categoryName, parent: this.parentObject}, params);
                    this.management.downloadUrlIntoFrame(url, "query_downloader_" + id);
                }
            });

        return QueryWidget;
    });
