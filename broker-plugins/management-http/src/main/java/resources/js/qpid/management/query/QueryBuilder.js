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
        "dgrid/extensions/ColumnResizer",
        "dstore/Memory",
        'dstore/legacy/DstoreAdapter',
        "qpid/management/query/QueryStore",
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
        "qpid/management/query/MessageDialog"
        ],
        function(declare,
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
                 ColumnResizer,
                 Memory,
                 DstoreAdapter,
                 QueryStore
                 )
        {
            var predefinedCategories =      [ {id: "queue", name: "Queue"},  {id: "connection", name: "Connection"} ];

            var QueryBuilder = declare( "qpid.management.query.QueryBuilder",
                            [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
                            {
                                 //Strip out the apache comment header from the template html as comments unsupported.
                                templateString:    template.replace(/<!--[\s\S]*?-->/g, ""),

                                /**
                                 * Fields from template
                                 **/
                                scope:null,
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
                                _management: null,

                                /**
                                 * Inner fields
                                 */
                                _standardMode: true,
                                _scopeModelObjects: {},
                                _categorySelector: null,
                                _searchScopeSelector: null,
                                _lastStandardModeSelect: [],
                                _sort: [],
                                _lastHeaders: [],

                                constructor: function(args)
                                             {
                                               this._management = args.management;
                                               this.inherited(arguments);
                                             },
                                postCreate:  function()
                                             {
                                               this.inherited(arguments);
                                               this._postCreate();
                                             },
                                _postCreate: function()
                                             {
                                               var promise = this._createScopeList();
                                               promise.then(lang.hitch(this, this. _postCreateScope));
                                             },
                                _postCreateScope: function()
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
                                               this.standardWhereExpressionBuilder.set("whereFieldsSelector", this.standardWhereChooser );
                                               this.standardWhereExpressionBuilder.set("userPreferences", this._management.userPreferences );
                                               this.standardWhereExpressionBuilder.startup();
                                               this.standardWhereExpressionBuilder.on("change", lang.hitch(this, this._standardModeWhereChanged));

                                               // search & mode buttons
                                               this.searchButton.on("click", lang.hitch(this, this.search));
                                               this.modeButton.on("click", lang.hitch(this, this._showModeSwitchWarningIfRequired));

                                               this._buildGrid();
                                               this._categoryChanged();
                                               this._toggleSearchButton();
                                             },
                                search:      function()
                                             {
                                                 var category = this._categorySelector.value.toLowerCase();
                                                 var scope = this._searchScopeSelector.value;
                                                 var modelObj = this._scopeModelObjects[scope];
                                                 this._store.selectClause = this._store.selectClause;
                                                 this._store.where = this._store.where;
                                                 this._store.category = category;
                                                 this._store.parent = modelObj;
                                                 this._store.orderBy = this._store.orderBy;
                                                 this._resultsGrid.refresh();
                                             },
                                _showModeSwitchWarningIfRequired: function()
                                            {
                                                var userPreferences = this._management.userPreferences;
                                                var displayWarning = (!userPreferences || !userPreferences.query ||
                                                                      (userPreferences.query.displaySwitchModeWarning == undefined ||
                                                                       userPreferences.query.displaySwitchModeWarning));
                                                if (this._standardMode && displayWarning && QueryBuilder.showWarningOnModeChange)
                                                {
                                                    if (!this._switchModeWarningDialog)
                                                    {
                                                        var formattedMessage = "<div>Switching to advanced mode is a one-way street,<br/>"
                                                            + "switching back from advanced mode to standard mode will<br/>"
                                                            + "completely reset the query.</div>";
                                                        this._switchModeWarningDialog = new qpid.management.query.MessageDialog({
                                                            title: "Warning!",
                                                            message: formattedMessage},
                                                            domConstruct.create("div"));
                                                        this._switchModeWarningDialog.on("execute",
                                                            lang.hitch(this, function(stopDisplaying)
                                                            {
                                                                if (stopDisplaying)
                                                                {
                                                                    if (!userPreferences.query)
                                                                    {
                                                                        userPreferences.query = {};
                                                                    }
                                                                    userPreferences.query.displaySwitchModeWarning = false;
                                                                    userPreferences.save({query: userPreferences.query},
                                                                        null,
                                                                        function(error){console.log("Saving user preferences failed: " + error);}
                                                                    );
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
                                _advancedModeSelectChanged: function()
                                             {
                                               this._store.selectClause = this.advancedSelect.value;
                                             },
                                _advancedModeWhereChanged: function()
                                             {
                                                 this._store.where = this.advancedWhere.value;
                                             },
                                _advancedModeOrderByChanged: function()
                                            {
                                                this._store.orderBy = this.advancedOrderBy.value;
                                            },
                                _toggleSearchButton: function(select)
                                             {
                                               var criteriaNotSet = !select;
                                               this.searchButton.set("disabled",criteriaNotSet);
                                               this.searchButton.set("title", criteriaNotSet?"Please, choose fields to display in order to enable search":"Search");
                                             },
                                _buildOrderByExpression: function()
                                             {
                                               var orderByExpression = "";
                                               if (this._sort && this._sort.length)
                                               {
                                                 var orders = []
                                                 for (var i = 0; i < this._sort.length; ++i)
                                                 {
                                                   orders.push(parseInt(this._sort[i].property) + (this._sort[i].descending? " desc" : ""));
                                                 }
                                                 orderByExpression = orders.join(",");
                                               }
                                               this.advancedOrderBy.set("value", orderByExpression);
                                               return orderByExpression;
                                             },
                                _buildSelectExpression: function(value)
                                             {
                                                var expression = "";
                                                if (lang.isArray(value))
                                                {
                                                    for(var i=0; i<value.length ;i++)
                                                    {
                                                        var selection = value[i] && value[i].hasOwnProperty("attributeName") ?
                                                            value[i].attributeName : value[i];
                                                        expression = expression + (i > 0 ? "," : "") + selection;
                                                    }
                                                }
                                                return expression;
                                             },
                                _normalizeSorting: function(selectedColumns)
                                             {
                                                 var newSort = [];
                                                 for (var i = 0; i < this._sort.length; ++i) {
                                                     var sortColumnIndex = parseInt(this._sort[i].property) - 1;
                                                     var sortDescending = this._sort[i].descending;
                                                     if (sortColumnIndex < this._lastStandardModeSelect.length) {
                                                         var oldSortedColumnName = this._lastStandardModeSelect[sortColumnIndex].attributeName;
                                                         for (var j = 0; j < selectedColumns.length; ++j) {
                                                             if (selectedColumns[j].attributeName === oldSortedColumnName) {
                                                                 newSort.push({
                                                                     property: "" + (j + 1),
                                                                     descending: sortDescending
                                                                 });
                                                                 break;
                                                             }
                                                         }
                                                     }
                                                 }
                                                 this._sort = newSort;
                                             },
                                _standardModeSelectChanged: function(selectedColumns)
                                             {
                                               this._normalizeSorting(selectedColumns);
                                               this._store.orderBy = this._buildOrderByExpression();
                                               this._store.selectClause = this._buildSelectExpression(selectedColumns);
                                               this._lastStandardModeSelect = lang.clone(selectedColumns);
                                               this._toggleSearchButton(this._store.selectClause);
                                               this.search();
                                             },
                                _standardModeWhereChanged: function(result)
                                             {
                                                this._store.where = result;
                                                this.search();
                                             },
                                _buildGrid:  function()
                                             {
                                                this._store = new QueryStore({
                                                    management: this.management,
                                                    category: this._categorySelector.value.toLowerCase(),
                                                    parent: this._scopeModelObjects[this._searchScopeSelector.value],
                                                    zeroBased: false});

                                                var CustomGrid = declare([ Grid, Keyboard, Selection, Pagination, ColumnResizer ]);

                                                var grid = new CustomGrid({   collection: this._store,
                                                                              rowsPerPage: 100,
                                                                              selectionMode: 'single',
                                                                              cellNavigation: false,
                                                                              className: 'dgrid-autoheight',
                                                                              pageSizeOptions: [10,20,30,40,50,100,1000,10000,100000],
                                                                              adjustLastColumn: true
                                                                          },
                                                                          this.queryResultGrid);
                                                this._store.on("changeHeaders", lang.hitch(this, function(event) {
                                                    this._store.useCachedResults = true;
                                                    grid.set("columns", this._getColumns(event.headers));
                                                    this._resultsGrid.resize();
                                                }));
                                                this._resultsGrid = grid;
                                                this._resultsGrid.startup();
                                                this._resultsGrid.on('.dgrid-row:dblclick', lang.hitch(this, this._onRowClick));
                                                this._resultsGrid.on("dgrid-sort", lang.hitch(this, function(event) {
                                                    for (var i = 0; i < this._sort.length; ++i) {
                                                        if (this._sort[i].property == event.sort[0].property) {
                                                            this._sort.splice(i, 1);
                                                            break;
                                                        }
                                                    }
                                                    this._sort.splice(0, 0, event.sort[0]);
                                                    this._store.orderBy = this._buildOrderByExpression();
                                                    event.preventDefault();
                                                    event.stopPropagation();
                                                    this.search();
                                                }));
                                                this._resultsGrid.on("dgrid-refresh-complete",
                                                                     lang.hitch(this,
                                                                                function()
                                                                                {
                                                                                  this._store.useCachedResults = false;
                                                                                  this._resultsGrid.updateSortArrow(this._sort, true);
                                                                                }));
                                             },
                                _onRowClick: function (event)
                                             {
                                               var row = this._resultsGrid.row(event);
                                               var promise = this._management.get({url:"service/structure"});
                                               var that = this;
                                               promise.then(function (data)
                                                            {
                                                              var findObject = function findObject(structure, parent, type)
                                                              {
                                                                  var item = {id:structure.id,
                                                                             name: structure.name,
                                                                             type: type,
                                                                             parent: parent};
                                                                  if (item.id == row.id)
                                                                  {
                                                                    return item;
                                                                  }
                                                                  else
                                                                  {
                                                                      for(var fieldName in structure)
                                                                      {
                                                                          var fieldValue = structure[fieldName];
                                                                          if (lang.isArray(fieldValue))
                                                                          {
                                                                             var fieldType = fieldName.substring(0, fieldName.length - 1);
                                                                             for (var i = 0; i < fieldValue.length; i++)
                                                                             {
                                                                                var object = fieldValue[i];
                                                                                var result = findObject(object, item, fieldType);
                                                                                if (result != null)
                                                                                {
                                                                                    return result;
                                                                                }
                                                                             }
                                                                          }
                                                                      }
                                                                      return null;
                                                                  }
                                                              };

                                                              var item = findObject(data, null, "broker");
                                                              if (item != null)
                                                              {
                                                               that.controller.show(item.type, item.name, item.parent, item.id);
                                                              }
                                                            });
                                             },
                                _getColumns: function(headers)
                                             {
                                               this._lastHeaders = headers;
                                               var columns = [];
                                               if (headers)
                                               {
                                                  for (var i = 0; i < headers.length; ++i)
                                                  {
                                                     var attribute = headers[i];
                                                     var column = {label: attribute, field: "" + (i + 1), sortable: true};
                                                     columns.push(column);
                                                     if (this._columns)
                                                     {
                                                       var columnData = this._columns[attribute];
                                                       if (columnData)
                                                       {
                                                         if (columnData.type == "Date")
                                                         {
                                                           var that = this;
                                                           column.formatter = function(value, object)
                                                                                  {
                                                                                    if (!isNaN(value) &&  parseInt(Number(value)) == value &&  !isNaN(parseInt(value, 10)))
                                                                                    {
                                                                                      return that._management.userPreferences.formatDateTime(value, {addOffset: true, appendTimeZone: true});
                                                                                    }
                                                                                    return value ? entities.encode(String(value)) : "";
                                                                                  };
                                                         }
                                                         else if (columnData.type == "Map")
                                                         {
                                                           column.renderCell = function(object, value, node)
                                                                                  {
                                                                                    if (value)
                                                                                    {
                                                                                      var list = domConstruct.create("div", {}, node);
                                                                                      for(var i in value)
                                                                                      {
                                                                                         domConstruct.create("div",
                                                                                                              {innerHTML: entities.encode(String(i))
                                                                                                                          + ": "
                                                                                                                          + entities.encode(json.stringify(value[i]))},
                                                                                                              list);
                                                                                      }
                                                                                      return list;
                                                                                    }
                                                                                    return "";
                                                                                  };
                                                         }
                                                         else if (columnData.type == "List" || columnData.type == "Set")
                                                         {
                                                           column.renderCell = function(object,value, node)
                                                                                  {
                                                                                    if (value)
                                                                                    {
                                                                                      var list = domConstruct.create("div", {}, node);
                                                                                      for(var i in value)
                                                                                      {
                                                                                         domConstruct.create("div", {innerHTML:entities.encode(json.stringify(value[i]))}, list)
                                                                                      }
                                                                                      return list;
                                                                                    }
                                                                                    return  "";
                                                                                  };
                                                         }
                                                       }
                                                     }
                                                  }
                                               }
                                               return columns;
                                             },
                                _createScopeList: function()
                                             {
                                               var that = this;
                                               var result = this._management.query({select: "id, $parent.name as parentName, name",
                                                                                   category : "virtualhost"});
                                               var deferred = new dojo.Deferred();
                                               result.then(function(data)
                                                           {
                                                             try
                                                             {
                                                               that._scopeDataReceived(data);
                                                             }
                                                             finally
                                                             {
                                                               deferred.resolve(that._searchScopeSelector);
                                                             }
                                                           },
                                                           function(error)
                                                           {
                                                             deferred.reject(null);
                                                             console.error(error.message ? error.message : error);
                                                           });
                                               return deferred.promise;
                                             },
                                _scopeDataReceived: function(result)
                                             {
                                               this._scopeModelObjects = {};
                                               var defaultValue = undefined;
                                               var items = [{id:undefined, name: "Broker"}];
                                               var data = result.results;
                                               for(var i =0 ; i<data.length;i++)
                                               {
                                                 var name = data[i][2];
                                                 var parentName = data[i][1];
                                                 items.push({id: data[i][0],  name: "VH:" + parentName + "/" + name});
                                                 this._scopeModelObjects[data[i].id] = {name: name,
                                                                                        type: "virtualhost",
                                                                                        parent: {name: parentName,
                                                                                                 type: "virtualhostnode",
                                                                                                 parent: {type: "broker"}
                                                                                                }
                                                                                       };
                                                 if (this.parentModelObj &&
                                                     this.parentModelObj.type == "virtualhost" &&
                                                     this.parentModelObj.name == name &&
                                                     this.parentModelObj.parent &&
                                                     this.parentModelObj.parent.name == parentName)
                                                 {
                                                   defaultValue = data[i].id;
                                                 }
                                               }

                                               var scopeStore = new DstoreAdapter (new Memory({data: items,
                                                                                               idProperty: 'id'}));
                                               this._searchScopeSelector = new dijit.form.FilteringSelect({ name: "scope",
                                                                                                            placeHolder: "Select search scope",
                                                                                                            store: scopeStore,
                                                                                                            value: defaultValue,
                                                                                                            required: false
                                                                                                          },
                                                                                                          this.scope);
                                               this._searchScopeSelector.startup();
                                            },
                                _createCategoryList: function()
                                            {
                                              var categoryStore = new DstoreAdapter(new Memory({idProperty: "id",
                                                                                                data: predefinedCategories}));
                                              var categoryList = new dijit.form.ComboBox({name: "category",
                                                                                          placeHolder: "Select Category",
                                                                                          store: categoryStore,
                                                                                          value: this._category || "Queue",
                                                                                          required: true,
                                                                                          invalidMessage: "Invalid category specified"
                                                                                         },
                                                                                         this.categoryName);
                                              categoryList.startup();
                                              categoryList.on("change", lang.hitch(this, this._categoryChanged));
                                              this._categorySelector = categoryList;
                                            },
                                _categoryChanged: function()
                                            {
                                                this._resetSearch();
                                                var metadata = this._getCategoryMetadata(this._categorySelector.value);
                                                var disableMetadataDependant = !metadata;
                                                this.standardWhereChooser.set("disabled", disableMetadataDependant);
                                                this.standardSelectChooser.set("disabled", disableMetadataDependant);
                                                this.searchButton.set("disabled", disableMetadataDependant || !this._store.selectClause);
                                                this.modeButton.set("disabled", disableMetadataDependant);
                                                this.advancedSelect.set("disabled", disableMetadataDependant);
                                                this.advancedWhere.set("disabled", disableMetadataDependant);
                                                this.advancedOrderBy.set("disabled", disableMetadataDependant);

                                                if (disableMetadataDependant)
                                                {
                                                    dijit.showTooltip(
                                                        this._categorySelector.get("invalidMessage"),
                                                        this._categorySelector.domNode,
                                                        this._categorySelector.get("tooltipPosition"),
                                                        !this._categorySelector.isLeftToRight()
                                                    );
                                                }
                                                else
                                                {
                                                    var data = this._combineTypeAttributesAndStatistics(metadata);
                                                    this._columns = data.asObject;
                                                    this.standardSelectChooser.set("data", {items: data.asArray,
                                                        idProperty: "id",
                                                        selected:[],
                                                        nameProperty: "attributeName"});
                                                    this.standardWhereChooser.set("data", {items: data.asArray,
                                                        selected:[],
                                                        idProperty: "id",
                                                        nameProperty: "attributeName"});
                                                }
                                            },
                                _advancedModeKeyPressed:function(evt)
                                            {
                                              var key = evt.keyCode;
                                              if (key == dojo.keys.ENTER)
                                              {
                                                  evt.preventDefault();
                                                  evt.stopPropagation();
                                                  this._store.selectClause = this.advancedSelect.value;
                                                  this._store.where = this.advancedWhere.value;
                                                  this._store.orderBy = this.advancedOrderBy.value;
                                                  this.search();
                                              }
                                            },
                                _modeChanged: function()
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
                                                this.advancedSelect.set("value", this._store.selectClause);
                                                this.advancedWhere.set("value", this._store.where);
                                                this.advancedOrderBy.set("value", this._store.orderBy);
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
                                                this._resetSearch();
                                              }
                                            },
                                _resetSearch: function()
                                            {
                                                this._store.where = "";
                                                this._store.selectClause = "";
                                                this._store.orderBy = "";
                                                this.standardSelectChooser.set("data", {selected: []});
                                                this.standardWhereExpressionBuilder.clearWhereCriteria();
                                                this._sort = [];
                                                this.advancedSelect.set("value", this._store.selectClause);
                                                this.advancedWhere.set("value", this._store.where);
                                                this.advancedOrderBy.set("value", this._store.orderBy);
                                                this.search();
                                            },
                                _getCategoryMetadata: function(value)
                                            {
                                              if (value)
                                              {
                                                var category = value.charAt(0).toUpperCase() + value.substring(1);
                                                return this._management.metadata.metadata[category];
                                              }
                                              else
                                              {
                                                return undefined;
                                              }
                                            },
                                _combineTypeAttributesAndStatistics: function(metadata)
                                            {
                                              var columnsArray = [];
                                              var columnsObject = {};
                                              var validTypes = [];
                                              var typeAttribute = null;
                                              for(var i in metadata)
                                              {
                                                validTypes.push(i);
                                                var categoryType = metadata[i];
                                                var attributes = categoryType.attributes;
                                                for(var name in attributes)
                                                {
                                                  var attribute = attributes[name];
                                                  if (!(name in columnsObject))
                                                  {
                                                    var attributeData = {id: name,
                                                                         attributeName: name,
                                                                         type: attribute.type,
                                                                         validValues: attribute.validValues,
                                                                         description: attribute.description,
                                                                         columnType: "attribute"};
                                                    if (name === "type")
                                                    {
                                                      typeAttribute = attributeData;
                                                    }
                                                    columnsObject[name] = attributeData;
                                                    columnsArray.push(attributeData );
                                                  }
                                                }

                                                var statistics = categoryType.statistics;
                                                for(var name in statistics)
                                                {
                                                  var statistic = statistics[name];
                                                  if (!(name in columnsObject))
                                                  {
                                                    var statisticData = {id: name,
                                                                         attributeName: name,
                                                                         type: statistic.type,
                                                                         description: statistic.description,
                                                                         columnType: "statistics"};
                                                    columnsArray.push(statisticData);
                                                    columnsObject[name] = statisticData;
                                                  }
                                                }
                                              }
                                              if (typeAttribute != null && !typeAttribute.validValues)
                                              {
                                                typeAttribute.validValues = validTypes;
                                              }
                                              return {asArray: columnsArray, asObject: columnsObject};
                                            }
                            });

            QueryBuilder.showWarningOnModeChange = true;

            return QueryBuilder;
        });
