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
        "dojo/text!query/QueryBuilder.html",
        "dojox/html/entities",
        "dgrid/Grid",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/Selector",
        "dstore/Memory",
        'dstore/legacy/DstoreAdapter',
        "qpid/management/query/ColumnsSelector",
        "qpid/management/query/WhereCriteria",
        "dijit/popup",
        "qpid/common/FormWidgetMixin",
        "dijit/_Widget",
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
        "dijit/Menu",
        "dijit/MenuItem",
        "dijit/Toolbar",
        "dijit/TooltipDialog",
        "dijit/Dialog",
        "dojo/Deferred",
        "dojo/json"
        ],
        function(declare,
                 lang,
                 parser,
                 domConstruct,
                 template,
                 entities,
                 Grid,
                 Keyboard,
                 Selection,
                 Pagination,
                 Selector,
                 Memory,
                 DstoreAdapter,
                 ColumnsSelector,
                 WhereCriteria,
                 popup
                 )
        {
            return declare( "qpid.management.query.QueryBuilder",
                            [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
                            {
                                 //Strip out the apache comment header from the template html as comments unsupported.
                                templateString:    template.replace(/<!--[\s\S]*?-->/g, ""),
                                _select: "",
                                _where: [],
                                _whereItems: {},
                                _standardMode: true,
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
                                                this._createCategoryList();
                                                var that = this;
                                                this.searchButton.on("click", function(e){ that.search()});
                                                this.selectCriteria.set("value", this._select);
                                                this.selectCriteria.on("change", function(){that._advancedModeSelectCriteriaChanged();});
                                                this.selectCriteria.on("keyUp", function(e){that._keyPressed(e);});
                                                this.whereCriteria.on("change", function(){that._advancedModeWhereCriteriaChanged();});
                                                this.whereCriteria.on("keyUp", function(e){that._keyPressed(e);});
                                                this.modeButton.on("click", function(e){that._modeChanged();});
                                                this._virtualHostWidget = this._createScopeList();
                                                this._selectChooser = new ColumnsSelector({onDone:  function(result)
                                                                                                    {
                                                                                                      popup.close(that.columnsDialog);
                                                                                                      that._setSelectedAttr(result);
                                                                                                      that.search();
                                                                                                    },
                                                                                           onCancel:function(result)
                                                                                                    {
                                                                                                      popup.close(that.columnsDialog);
                                                                                                    }
                                                                                          }, this.selectPanel);
                                                this.columnsDialog.set("content", this._selectChooser);

                                                this._whereChooser = new ColumnsSelector({onDone:   function(result)
                                                                                                    {
                                                                                                      popup.close(that.conditionsDialog);
                                                                                                      that._setWhereAttr(result);
                                                                                                      that.search();
                                                                                                    },
                                                                                           onCancel:function(result)
                                                                                                    {
                                                                                                        popup.close(that.conditionsDialog);
                                                                                                    }
                                                                                          }, this.wherePanel);
                                                this.conditionsDialog.set("content", this._whereChooser);

                                                // startup buttons to create dropDown fields
                                                this.conditionsButton.startup();
                                                this.columnsButton.startup();
                                                this.modeButton.set("checked", false)
                                                this._categoryChanged();
                                                this._criteriaChanged();
                                             },
                                search:      function()
                                             {
                                               if (this.selectCriteria.value)
                                               {
                                                  if (!this._standardMode)
                                                  {
                                                    this._selectChooser.set("selected", this._getSelectItems());
                                                    if (this._lastStandardModeWhereExpression && this._lastStandardModeWhereExpression != this.whereCriteria.value)
                                                    {
                                                        this._lastStandardModeWhereExpression = "";
                                                        this._clearWhereCriteria();
                                                        this._where = [];
                                                        this._whereItems ={}
                                                        this._whereChooser.set("selected", []);
                                                    }
                                                  }
                                                  var modelObj = this._scopeModelObjects[this._searchScopeList.value];
                                                  this._doSearch( modelObj,
                                                                  this._categoryList.value.toLowerCase(),
                                                                  this.selectCriteria.value,
                                                                  this._standardMode ? this._lastStandardModeWhereExpression : this.whereCriteria.value);
                                               }
                                             },
                                _doSearch:   function(modelObj, category, select, where)
                                             {
                                               var that = this;
                                               var result = this._management.query({select: select,
                                                                                      where: where,
                                                                                      parent: modelObj,
                                                                                      category: category,
                                                                                      transformIntoObjects: true});
                                               result.then(function(data)
                                                           {
                                                             that._showResults(data, select);
                                                           },
                                                           function(error)
                                                           {
                                                             if (error && error.response && error.response.status == 404)
                                                             {
                                                               that._showResults([], select);
                                                             }
                                                             else
                                                             {
                                                               alert(error.message ? error.message: error);
                                                             }
                                                           });
                                             },
                                _advancedModeWhereCriteriaChanged:  function()
                                             {
                                               this._criteriaChanged();
                                               if (this._lastStandardModeWhereExpression)
                                               {
                                                dijit.showTooltip("On switching into Standard Mode where expression"
                                                                  + " will be erased. Copying of where expression from "
                                                                  + " Advanced Mode in Standard Mode is unsupported!",
                                                                  this.whereCriteria.domNode,
                                                                  this.whereCriteria.get("tooltipPosition"),
                                                                  !this.whereCriteria.isLeftToRight());
                                               }
                                             },
                                _advancedModeSelectCriteriaChanged: function()
                                             {
                                               this._criteriaChanged();
                                               this._setSelectedAttr(this.selectCriteria.value);
                                             },
                                _showResults:function(data, select)
                                             {
                                               var store = new Memory({data: data, idProperty: 'id'});
                                               if (!this._resultsGrid)
                                               {
                                                 this._buildGrid(store, select);
                                               }
                                               else
                                               {
                                                 this._resultsGrid.set("collection", store);
                                                 this._resultsGrid.set("columns", this._getColumns(select));
                                                 this._resultsGrid.refresh();
                                               }
                                             },
                                _buildGrid:  function(store, select)
                                             {
                                                var CustomGrid = declare([ Grid, Keyboard, Selection, Pagination ]);
                                                var grid = new CustomGrid({
                                                                              columns: this._getColumns(select),
                                                                              collection: store,
                                                                              rowsPerPage: 100,
                                                                              selectionMode: 'single',
                                                                              cellNavigation: false,
                                                                              className: 'dgrid-autoheight'
                                                                          },
                                                                          this.queryResultGrid);
                                                this._resultsGrid = grid;
                                                this._resultsGrid.startup();
                                                this._resultsGrid.on('.dgrid-row:dblclick', lang.hitch(this, this._onRowClick));
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
                                _getColumns: function(select)
                                             {
                                                select =  select || this._select;
                                                var columns = {};
                                                var attributes = select.split(",");
                                                for (var i in attributes)
                                                {
                                                   var attribute = attributes[i].replace(/^\s+|\s+$/gm,'');
                                                   columns[attribute] = attribute;
                                                }
                                                return columns;
                                             },
                                _createScopeList: function()
                                            {
                                                var that = this;
                                                var result = this._management.query({select: "$parent.name, name, id",
                                                                                     category : "virtualhost",
                                                                                     transformIntoObjects: true});
                                                var deferred = new dojo.Deferred();
                                                result.then(function(data)
                                                            {
                                                               that._scopeModelObjects = {};
                                                               var defaultValue = undefined;
                                                               var items = [{id:undefined, name: "Broker"}];
                                                               for(var i =0 ; i<data.length;i++)
                                                               {
                                                                 var name = data[i].name;
                                                                 var parentName = data[i]["$parent.name"];
                                                                 items.push({id: data[i].id,  name: "VH:" + parentName + "/" + name});
                                                                 that._scopeModelObjects[data[i].id] = {name: name,
                                                                                                             type: "virtualhost",
                                                                                                             parent: {name: parentName,
                                                                                                                      type: "virtualhostnode",
                                                                                                                      parent: {type: "broker"}
                                                                                                                     }
                                                                                                            };
                                                                 if (that.parentModelObj &&
                                                                     that.parentModelObj.type == "virtualhost" &&
                                                                     that.parentModelObj.name == name &&
                                                                     that.parentModelObj.parent &&
                                                                     that.parentModelObj.parent.name == parentName)
                                                                 {
                                                                    defaultValue = data[i].id;
                                                                 }
                                                               }
                                                               try
                                                               {
                                                                  var scopeStore = new DstoreAdapter (new Memory({data: items, idProperty: 'id'}));
                                                                  that._searchScopeList = new dijit.form.FilteringSelect({
                                                                                                                        name: "scope",
                                                                                                                        placeHolder: "Select search scope",
                                                                                                                        store: scopeStore,
                                                                                                                        value: defaultValue,
                                                                                                                        required: false,
                                                                                                                        "class": "queryDefaultField"
                                                                                                                        },
                                                                                                                        that.scope);
                                                                  that._searchScopeList.startup();
                                                               }
                                                               finally
                                                               {
                                                                 deferred.resolve(that._searchScopeList);
                                                               }
                                                            },
                                                            function(error)
                                                            {
                                                              deferred.reject(null);
                                                              console.error(error.message ? error.message : error);
                                                            });
                                                return deferred.promise;
                                            },
                                _createCategoryList: function()
                                            {
                                                var predefinedCategories = [
                                                                             {id: "queue", name: "queue"},
                                                                             {id: "connection", name: "connection"}
                                                                           ];
                                                var categoryStore = new DstoreAdapter (new Memory({ idProperty: "id",
                                                                                       data: predefinedCategories}));
                                                this._categoryList = new dijit.form.ComboBox({
                                                                                                name: "category",
                                                                                                placeHolder: "Select Category",
                                                                                                store: categoryStore,
                                                                                                value: this._category || "queue",
                                                                                                required: true,
                                                                                                invalidMessage: "Invalid category specified",
                                                                                                "class": "queryDefaultField"
                                                                                             },
                                                                                             this.categoryName);
                                                this._categoryList.startup();
                                                this._categoryList.on("change", lang.hitch(this, this._categoryChanged));
                                            },
                                _categoryChanged: function()
                                            {
                                                var metadata = this._getCategoryMetadata();
                                                var disableMetadataDependant = !metadata;
                                                this.conditionsButton.set("disabled", disableMetadataDependant);
                                                this.columnsButton.set("disabled", disableMetadataDependant);
                                                this.searchButton.set("disabled", disableMetadataDependant);
                                                if (disableMetadataDependant)
                                                {
                                                    dijit.showTooltip(
                                                        this._categoryList.get("invalidMessage"),
                                                        this._categoryList.domNode,
                                                        this._categoryList.get("tooltipPosition"),
                                                        !this._categoryList.isLeftToRight()
                                                    );
                                                }
                                                else
                                                {
                                                    if (this._lastCategory != this._categoryList.value)
                                                    {
                                                        this._lastCategory = this._categoryList.value;
                                                        this.selectCriteria.set("value", "");
                                                        this._selected = "";
                                                        this._showResults([],  this._selected);
                                                        var columns = this._extractColumnsFromMetadata(metadata);
                                                        this.whereCriteria.set("value", "");
                                                        this._clearWhereCriteria();
                                                        this._where = [];
                                                        this._whereItems ={}
                                                        this._whereChooser.updateData(columns, []);
                                                        this._selectChooser.updateData(columns, []);
                                                    }
                                                }
                                            },
                                _criteriaChanged: function()
                                            {
                                                var criteriaNotSet = !this.selectCriteria.value;
                                                this.searchButton.set("disabled",criteriaNotSet);
                                                this.searchButton.set("title",criteriaNotSet?"Please, choose fields to display in order to enable search":"Search");
                                            },
                                _keyPressed:function(evt)
                                            {
                                                var key = evt.keyCode;
                                                if (key == dojo.keys.ENTER && this.selectCriteria.value)
                                                {
                                                   this.search();
                                                }
                                            },
                                _modeChanged: function()
                                            {
                                              this._standardMode = !this._standardMode
                                              if (!this._standardMode)
                                              {
                                                this.modeButton.set("label", "Standard");
                                                this.modeButton.set("title", "Switch to 'Standard' search");
                                                this.selectCriteria.set("disabled", false);
                                                this.whereCriteria.set("disabled", false);
                                                this.standardSearchTools.style.display = "none";
                                                this.standardModeConditionsToolbar.domNode.style.display = "none";
                                                this.advancedSearch.style.display = "";
                                              }
                                              else
                                              {
                                                this.modeButton.set("label", "Advanced");
                                                this.modeButton.set("title", "Switch to 'Advanced' search using SQL-like expressions");
                                                this.selectCriteria.set("disabled", true);
                                                this.whereCriteria.set("disabled", true);
                                                this.standardSearchTools.style.display = "";
                                                this.standardModeConditionsToolbar.domNode.style.display = "";
                                                this.advancedSearch.style.display = "none";
                                              }
                                            },
                                _getCategoryMetadata: function()
                                            {
                                                if (this._categoryList.value)
                                                {
                                                    var category = this._categoryList.value.charAt(0).toUpperCase() + this._categoryList.value.substring(1);
                                                    return this._management.metadata.metadata[category];
                                                }
                                                else
                                                {
                                                  return undefined;
                                                }
                                            },
                                _getSelectItems: function()
                                            {
                                                var columns = [];
                                                if (this.selectCriteria.value)
                                                {
                                                    var attributes = this.selectCriteria.value.split(",");
                                                    for (var i in attributes)
                                                    {
                                                       var attribute = attributes[i].replace(/^\s+|\s+$/gm,'');
                                                       columns.push(attribute);
                                                    }
                                                }
                                                return columns;
                                            },
                                _setSelectedAttr: function(value)
                                            {
                                                var val = value;
                                                if (lang.isArray( value ))
                                                {
                                                    val = "";
                                                    for(var i=0;i<value.length;i++)
                                                    {
                                                      val = val + (i> 0 ? "," : "") + (value[i].attributeName ? value[i].attributeName : value[i]);
                                                    }
                                                }
                                                this._selected = val;
                                                this.selectCriteria.set("value", val);
                                            },
                                _getWhereItems: function()
                                            {
                                                var selected = [];
                                                for(var i =0; i< this._where.length; i++)
                                                {
                                                    var id = this._where[i].id;
                                                    selected.push(id);
                                                }
                                                return selected;
                                            },
                                _setWhereAttr: function(items)
                                            {
                                                this._where = items;
                                                this._buildWhereCriteriaWidgets(items);
                                            },
                                _buildWhereCriteriaWidgets: function(items)
                                            {
                                                for(var i =0; i< items.length; i++)
                                                {
                                                    var name = items[i].attributeName;
                                                    if (!(name in this._whereItems))
                                                    {
                                                        this._whereItems[name] = this._createWhereCriteriaWidget(items[i]);
                                                    }
                                                }
                                            },
                                _createWhereCriteriaWidget: function(item)
                                            {
                                                var whereCriteria = null;
                                                var widgetNode = domConstruct.create("div");
                                                whereCriteria = new WhereCriteria({attributeDetails: item,
                                                                                   onChange:    lang.hitch(this,this._standardModeWhereChanged)},
                                                                                   widgetNode);
                                                this.standardModeConditionsToolbar.addChild(whereCriteria);
                                                whereCriteria.startup();
                                                return whereCriteria;

                                            },
                                _standardModeWhereChanged:function()
                                            {
                                              var columnsAfterChange = [];
                                              var whereExpression = "";
                                              var children = this.standardModeConditionsToolbar.getChildren();
                                              for(var i=0;i<children.length;i++)
                                              {
                                                var details = children[i].getAttributeDetails();
                                                columnsAfterChange.push(details);
                                                var expression =  children[i].getConditionExpression();
                                                if (expression)
                                                {
                                                    whereExpression = whereExpression + (whereExpression ? " and " : "") + expression;
                                                }
                                              }

                                              // find deleted widgets
                                              var deletedFound = false;
                                              var columnsBeforeChange = this._where.slice(0);
                                              for(var j=columnsBeforeChange.length - 1;j>=0;j--)
                                              {
                                                  var index = -1;
                                                  for(var i=0;i<columnsAfterChange.length;i++)
                                                  {
                                                    if (columnsBeforeChange[j].attributeName == columnsAfterChange[i].attributeName)
                                                    {
                                                      index = j;
                                                      break;
                                                    }
                                                  }
                                                  if (index == -1)
                                                  {
                                                    delete this._whereItems[columnsBeforeChange[j].attributeName];
                                                    deletedFound = true;
                                                  }
                                              }
                                              this._where = columnsAfterChange;
                                              this.whereCriteria.set("value", whereExpression);
                                              this._lastStandardModeWhereExpression = whereExpression;
                                              if (deletedFound)
                                              {
                                                this._whereChooser.set("selected", this._getWhereItems());
                                              }
                                              this.search();
                                            },
                                _clearWhereCriteria: function()
                                            {
                                               this._whereItems = {};
                                               this._where = [];
                                               var children = this.standardModeConditionsToolbar.getChildren();
                                               for(var i=children.length-1;i>=0;i--)
                                               {
                                                   children[i].destroyRecursive(false);
                                               }
                                            },
                                _extractColumnsFromMetadata: function(metadata)
                                            {
                                                var columns = [];
                                                var helper = {};
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
                                                    if (!(name in helper))
                                                    {
                                                      helper[name] = true;
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
                                                      columns.push(attributeData );
                                                    }
                                                  }

                                                  var statistics = categoryType.statistics;
                                                  for(var name in statistics)
                                                  {
                                                    var statistic = statistics[name];
                                                    if (!(name in helper))
                                                    {
                                                      helper[name] = true;
                                                      columns.push( {id: name,
                                                                     attributeName: name,
                                                                     type: statistic.type,
                                                                     description: statistic.description,
                                                                     columnType: "statistics"});
                                                    }
                                                  }
                                                }
                                                if (typeAttribute != null && !typeAttribute.validValues)
                                                {
                                                  typeAttribute.validValues = validTypes;
                                                }
                                                return columns;
                                            }
                            });
        });
