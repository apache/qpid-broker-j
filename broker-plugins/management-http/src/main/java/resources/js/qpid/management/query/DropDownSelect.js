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

define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",
  "dojo/json",
  "dojo/dom-construct",
  "dojo/text!query/OptionsPanel.html",
  "dgrid/OnDemandGrid",
  "dgrid/Keyboard",
  "dgrid/Selection",
  "dgrid/Selector",
  "dgrid/extensions/DijitRegistry",
  "dgrid/extensions/Pagination",
  "dstore/Memory",
  "dijit/popup",
  "dojo/Evented",
  "dijit/TooltipDialog",
  "dijit/layout/ContentPane",
  "dijit/form/Button",
  "dijit/form/ValidationTextBox",
  "dijit/form/CheckBox",
  "dijit/_WidgetBase",
  "dijit/_TemplatedMixin",
  "dijit/_WidgetsInTemplateMixin",
  "dojo/domReady!"
],
function(declare, lang, array, json, domConstruct, template, Grid, Keyboard, Selection,
         Selector, DijitRegistry, Pagination, Memory, popup, Evented)
{

   var Summary = declare(null,
                        {
                          showFooter: true,
                          buildRendering: function ()
                                          {
                                              this.inherited(arguments);
                                              this.summaryAreaNode = domConstruct.create('div',
                                                                                         {
                                                                                           className: 'dgrid-status',
                                                                                           role: 'row',
                                                                                           style: { overflow: 'hidden',
                                                                                                    display: 'none' }
                                                                                         },
                                                                                         this.footerNode);
                                              this.totalLabelNode = domConstruct.create('span',
                                                                                        {
                                                                                          innerHTML: "Total: "
                                                                                        },
                                                                                        this.summaryAreaNode);
                                              this.totalValueNode = domConstruct.create('span',
                                                                                        {
                                                                                           innerHTML: "0"
                                                                                        },
                                                                                        this.summaryAreaNode);
                                          },
                          setTotal:       function (total)
                                          {
                                            if (total>0)
                                            {
                                             this.totalValueNode.innerHTML = total;
                                             this.summaryAreaNode.style.display = "block";
                                            }
                                            else
                                            {
                                             this.summaryAreaNode.style.display = "none";
                                            }
                                          }
                        });

   var OptionsPanel = declare("qpid.management.query.OptionsPanel",
                   [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
                   {
                       /**
                        * dijit._TemplatedMixin enforced fields
                        */
                       //Strip out the apache comment header from the template html as comments unsupported.
                       templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                       /**
                        * template attach points
                        */
                       search: null,
                       clearButton: null,
                       selectOptions: null,
                       doneButton: null,
                       cancelButton: null,

                       /**
                        * widget fields which can be set via constructor arguments or #set("data",{...})
                        */
                       idProperty: "id",
                       nameProperty: "name",
                       store: null,
                       items: null,

                       /**
                        * widget inner fields
                        */
                       _optionsGrid: null,
                       _descending: false,

                       postCreate:  function()
                                    {
                                      this.inherited(arguments);
                                      this._postCreate();
                                    },
                       startup:     function()
                                    {
                                      this.inherited(arguments);
                                      this._optionsGrid.startup();
                                    },
                       _postCreate: function()
                                    {
                                      this.clearButton.on("click", lang.hitch(this, this._onClear));
                                      this.search.on("change", lang.hitch(this, this._searchChanged));
                                      this.search.on("keyUp", lang.hitch(this, function(evt)
                                                                               {
                                                                                 if (evt.keyCode == dojo.keys.ENTER && this.search.value)
                                                                                 {
                                                                                   this._applyFilter();
                                                                                 }
                                                                               }));
                                      this._toggleClearButtons();
                                      this._buildOptionsGrid();
                                      this._selectionChanged();
                                    },
                       _buildOptionsGrid: function()
                                    {
                                      var CustomGrid = declare([ Grid, Keyboard, Selector, Summary ]);
                                      if (!this.store)
                                      {
                                        this.store = new Memory({data: this.items || [], idProperty: this.idProperty});
                                      }
                                      var grid = new CustomGrid({
                                                                 columns: this._getOptionColumns(),
                                                                 collection: this.store,
                                                                 selectionMode: 'multiple',
                                                                 cellNavigation: true,
                                                                 allowSelectAll: true,
                                                                 minRowsPerPage : this.items ? this.items.length : 100,
                                                                 deselectOnRefresh: false
                                                               },
                                                               this.optionsGrid);
                                      grid.on('dgrid-select', lang.hitch(this, this._selectionChanged));
                                      grid.on('dgrid-deselect', lang.hitch(this, this._selectionChanged));
                                      grid.on('dgrid-sort', lang.hitch(this, function(event){this._descending = event.sort[0].descending}));
                                      grid.setTotal(this.items ? this.items.length : 0);
                                      this._optionsGrid =  grid;
                                   },
                       _setDataAttr:function(data)
                                    {
                                      if (data.idProperty)
                                      {
                                        this.idProperty = data.idProperty;
                                      }

                                      if (data.nameProperty)
                                      {
                                        this.nameProperty = data.nameProperty;
                                      }

                                      var store;
                                      if (data.store)
                                      {
                                        store = data.store;
                                      }
                                      else if (data.items)
                                      {
                                        store = new Memory({data: data.items, idProperty: this.idProperty});
                                        this.items = data.items;
                                      }

                                      if (store)
                                      {
                                        this.store = store;
                                        this._optionsGrid.set("columns", this._getOptionColumns());
                                        this._optionsGrid.set("minRowsPerPage", data.items?data.items.length:100);
                                        this._optionsGrid.set("sort", [{property:this.nameProperty,descending:this._descending}]);
                                        this._applyFilter();
                                        this._optionsGrid.setTotal(this.items ? this.items.length : 0);
                                      }

                                      if (data.selected)
                                      {
                                          this._selectGrid(data.selected);
                                      }
                                    },
                       _selectGrid: function(selected)
                                    {
                                      var selectedRowIds = lang.clone(this._optionsGrid.selection || {});
                                      if (selected && selected.length && this.store)
                                      {
                                        for(var i=0;i<selected.length;i++)
                                        {
                                          var id = selected[i] && selected[i].hasOwnProperty(this.idProperty) ? selected[i][this.idProperty] : selected[i];
                                          if (id in selectedRowIds)
                                          {
                                            delete selectedRowIds[id];
                                          }
                                          else
                                          {
                                            this._optionsGrid.select(id, null, true);
                                          }
                                        }
                                      }

                                      for (var id in selectedRowIds)
                                      {
                                        if (this._optionsGrid.selection[id])
                                        {
                                           this._optionsGrid.select(id, null, false);
                                        }
                                      }
                                    },
                       _onClear:    function()
                                    {
                                      this.search.set("value", "");
                                    },
                       _applyFilter:function()
                                    {
                                      if (this.search.value)
                                      {
                                        var searchRegExp = new RegExp(".*" + this.search.value + ".*", "i");
                                        var filter = {};
                                        filter[this.nameProperty] = searchRegExp;
                                        this._optionsGrid.set("collection", this.store.filter(filter));
                                      }
                                      else
                                      {
                                        this._optionsGrid.set("collection", this.store);
                                      }
                                    },
                       _toggleClearButtons:function()
                                    {
                                      this.clearButton.set("disabled", !this.search.value);
                                    },
                       _searchChanged: function()
                                    {
                                      this._toggleClearButtons();
                                      this.defer(this._applyFilter);
                                    },
                       _selectionChanged: function(event)
                                    {
                                      this.doneButton.set("disabled", !this._isSelected());
                                    },
                       _getOptionColumns: function()
                                    {
                                      var columns = {selected: { label: 'All', selector: 'checkbox' }};
                                      columns[this.nameProperty] = { label:"Name", sortable: true }
                                      return columns;
                                    },
                       _isSelected: function()
                                    {
                                      var selectionFound = false;
                                      for (var id in this._optionsGrid.selection)
                                      {
                                          if (this._optionsGrid.selection[id])
                                          {
                                             selectionFound = true;
                                             break;
                                          }
                                      }
                                      return selectionFound;
                                    },
                       _getSelectedItemsAttr: function()
                                    {
                                      var selected = [];
                                      if (this.store && this._optionsGrid.selection)
                                      {
                                        for (var id in this._optionsGrid.selection)
                                        {
                                          if (this._optionsGrid.selection[id])
                                          {
                                            selected.push(id);
                                          }
                                        }
                                      }
                                      var filter = new this.store.Filter().in(this.idProperty, selected);
                                      var promise = this.store.filter(filter).fetch();
                                      return promise;
                                    },
                       _reset:      function(items)
                                    {
                                      this._onClear();

                                      items = items || [];
                                      this._selectGrid(items);
                                    }
                   });

    return declare("qpid.management.query.DropDownSelect",
                   [dijit._WidgetBase, Evented],
                   {
                       _selectButton: null,
                       _optionsDialog: null,
                       _optionsPanel: null,
                       _selectedItems: null,

                       postCreate:  function()
                                    {
                                      this.inherited(arguments);
                                      this._postCreate();
                                    },
                       _postCreate: function()
                                    {
                                      this._optionsPanel = new OptionsPanel({},this._createDomNode());
                                      this._optionsDialog = new dijit.TooltipDialog({content:this._optionsPanel}, this._createDomNode());
                                      this._selectButton = new dijit.form.DropDownButton({label: this.label || "Select",
                                                                                          dropDown: this._optionsDialog},
                                                                                         this._createDomNode());
                                      this._optionsPanel.doneButton.on("click", lang.hitch(this, this._onSelectionDone));
                                      this._optionsPanel.cancelButton.on("click", lang.hitch(this, this._hideAndResetSearch));
                                      this._optionsDialog.on("hide", lang.hitch(this, this._resetSearch));
                                      this._optionsDialog.on("show", lang.hitch(this, this._onShow));
                                      this._selectButton.startup();
                                      this._optionsPanel.startup();
                                      this._optionsDialog.startup();
                                    },
                       _createDomNode: function()
                                    {
                                      return domConstruct.create("span", null, this.domNode);
                                    },
                       _setDataAttr:function(data)
                                    {
                                      this._optionsPanel.set("data", data);
                                      var promise = this._optionsPanel.get("selectedItems");
                                      dojo.when(promise,
                                                lang.hitch(this,
                                                           function(selectedItems)
                                                           {
                                                             this._selectedItems = selectedItems;
                                                           }));
                                    },
                       _getSelectedItemsAttr: function()
                                    {
                                      return this._optionsPanel.get("selectedItems");
                                    },
                       _onSelectionDone: function()
                                    {
                                      var promise = this._optionsPanel.get("selectedItems");
                                      dojo.when(promise, lang.hitch(this, function(selectedItems)
                                                                          {
                                                                            this._selectedItems = selectedItems;
                                                                            this._hideAndResetSearch(selectedItems);
                                                                            this.emit("change", selectedItems);
                                                                          }));
                                    },
                       _hideAndResetSearch: function()
                                    {
                                      popup.close(this._optionsDialog);
                                      this._resetSearch();
                                    },
                       _resetSearch:function()
                                    {
                                      this._optionsPanel._reset(this._selectedItems);
                                    },
                       _setDisabled:function(value)
                                    {
                                      this._selectButton.set("disabled", value);
                                    },
                       _getDisabled:function()
                                    {
                                      return  this._selectButton.get("disabled")
                                    },
                       _onShow:     function()
                                    {
                                      this._optionsPanel._optionsGrid.resize();
                                    }
                   });
});
