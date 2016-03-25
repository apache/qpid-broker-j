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
  "dojo/string",
  "dojo/text!query/ColumnsSelector.html",
  "dgrid/Grid",
  "dgrid/Keyboard",
  "dgrid/Selection",
  "dgrid/Selector",
  "dgrid/extensions/DijitRegistry",
  "dgrid/extensions/Pagination",
  "dstore/Memory",
  "dijit/popup",
  "dijit/TooltipDialog",
  "dijit/layout/ContentPane",
  "dijit/form/Button",
  "dijit/form/ValidationTextBox",
  "dijit/form/CheckBox",
  "dijit/_Widget",
  "dijit/_TemplatedMixin",
  "dijit/_WidgetsInTemplateMixin",
  "dojo/domReady!"
],
function(declare, lang, array, string, template, Grid, Keyboard, Selection,
         Selector, DijitRegistry, Pagination, Memory, popup)
{
    return declare("qpid.management.query.ColumnsSelector",
                   [dijit._Widget, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin],
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
                       columnSelectorContainer: null,
                       doneButton: null,
                       cancelButton: null,

                       /**
                        * widget fields
                        */
                       onDone: null,
                       onCancel: null,
                       _columnsGrid: null,

                       postCreate:  function()
                                    {
                                      this.inherited(arguments);
                                      this._initWidgets();
                                    },
                       _setDataAttr:function(data)
                                    {
                                      this._store = new Memory({data: data, idProperty: 'id'});
                                      this._columnsGrid.set("rowsPerPage", data.length);
                                      this._applyFilter();
                                    },
                       _setSelectedAttr:function(selected)
                                    {
                                      var grid = this._columnsGrid;
                                      var data = this._store.data;
                                      var selection = this._getSelection();
                                      for(var i = 0; i< selection.length; i++)
                                      {
                                        grid.select(selection[i], null, false);
                                      }
                                      if (selected && selected.length && selected.length>0)
                                      {
                                         array.forEach(data,
                                                    function (item)
                                                    {
                                                      for(var i=0;i<selected.length;i++)
                                                      {
                                                        if (item.id === selected[i])
                                                        {
                                                            grid.select(item, null, true);
                                                        }
                                                      }
                                                    });
                                      }
                                    },
                       updateData:  function(data, selected, onDone, onCancel)
                                    {
                                         if (onDone)
                                         {
                                            this.onDone = onDone;
                                         }
                                         if (onCancel)
                                         {
                                            this.onCancel = onCancel;
                                         }
                                         this._setDataAttr(data);
                                         this._setSelectedAttr(selected);
                                    },
                       _initWidgets:function()
                                    {
                                      var that = this;
                                      this.doneButton.on("click", lang.hitch(this, this._onSelectionDone));
                                      this.cancelButton.on("click", lang.hitch(this, this._onCancellation));
                                      this.clearButton.on("click", lang.hitch(this, function(){this.search.set("value", "");}));
                                      this.search.on("change", lang.hitch(this, this._searchChanged));
                                      this.search.on("keyUp", lang.hitch(this, function(evt)
                                                                               {
                                                                                 var key = evt.keyCode;
                                                                                 if (key == dojo.keys.ENTER && this.search.value)
                                                                                 {
                                                                                  this._applyFilter();
                                                                                 }
                                                                               }));
                                      this._buildColumnsGrid();
                                      this._toggleButtons();
                                      this._selectionChanged();
                                    },
                       _applyFilter:function()
                                    {
                                      if (this.search.value)
                                      {
                                        var searchRegExp = new RegExp(".*" + this.search.value + ".*", "i");
                                        this._columnsGrid.set("collection", this._store.filter({attributeName: searchRegExp }));
                                      }
                                      else
                                      {
                                        this._columnsGrid.set("collection", this._store);
                                      }
                                    },

                       _onSelectionDone: function()
                                    {
                                      var selection = this._getSelection();
                                      if (selection && selection.length >0)
                                      {
                                        this.search.set("value", "");
                                        var onDone = this.onDone;
                                        if (onDone)
                                        {
                                            window.setTimeout(function()
                                                              {
                                                                 onDone(selection);
                                                              },
                                                              50);
                                        }
                                      }
                                    },
                       _onCancellation: function()
                                    {
                                      var onCancel = this.onCancel;
                                      if (onCancel)
                                      {
                                          window.setTimeout(function()
                                                            {
                                                               onCancel();
                                                            },
                                                            50);
                                      }
                                    },
                       _toggleButtons:function()
                                    {
                                        this.clearButton.set("disabled", !this.search.value);
                                    },
                       _searchChanged: function()
                                    {
                                        this._toggleButtons();

                                        var that = this;
                                        if (this._searchTimeout)
                                        {
                                            clearTimeout(this._searchTimeout);
                                            this._searchTimeout = null;
                                        };
                                        this._searchTimeout = setTimeout(function()
                                                                    {
                                                                        that._applyFilter();
                                                                    }, 100);
                                    },
                       _buildColumnsGrid: function()
                                    {
                                      var that = this;
                                      var CustomGrid = declare([ Grid, Pagination, Keyboard, Selector ]);
                                      var grid = new CustomGrid({
                                                                    columns: this._getGridColumns(),
                                                                    collection: new Memory({data: [], idProperty: 'id'}),
                                                                    selectionMode: 'multiple',
                                                                    cellNavigation: false,
                                                                    sort: "attributeName",
                                                                    allowSelectAll: true,
                                                                    rowsPerPage : 100,
                                                                    deselectOnRefresh: false
                                                                },
                                                                this.columnSelectorContainer
                                                                );
                                      grid.on('dgrid-select', lang.hitch(this, this._selectionChanged));
                                      grid.on('dgrid-deselect', lang.hitch(this, this._selectionChanged));
                                      this._columnsGrid =  grid;
                                      grid.startup();

                                    },
                       _selectionChanged: function(event)
                                    {
                                      this.doneButton.set("disabled", !this._isSelected());
                                    },
                       _getGridColumns: function()
                                    {
                                        var columns = {
                                                        selected: { label: 'All', selector: 'checkbox' },
                                                        attributeName: { label:"Name", sortable: true }
                                                      };
                                        return columns;
                                    },
                       _refreshGrid:function() // <---- DEAD
                                    {
                                      var that = this;
                                      var selection = this._getSelection();
                                      this._columnsGrid.refresh();
                                      array.forEach(selection,
                                                      function (item)
                                                      {
                                                        that._columnsGrid.select(item, null, true);
                                                      });

                                    },
                       _isSelected: function()
                                    {
                                      var selectionFound = false;
                                      for (var id in this._columnsGrid.selection)
                                      {
                                          if (this._columnsGrid.selection[id])
                                          {
                                             selectionFound = true;
                                             break;
                                          }
                                      }
                                      return selectionFound;
                                    },
                       _getSelection: function()
                                    {
                                        var selected = [];
                                        if (this._store && this._columnsGrid.selection)
                                        {
                                            for (var id in this._columnsGrid.selection)
                                            {
                                              if (this._columnsGrid.selection[id])
                                              {
                                                this._store.forEach(function (object)
                                                {
                                                      if (object.id == id)
                                                      {
                                                        selected.push(object);
                                                      }
                                                });
                                              }
                                            }
                                        }
                                        return selected;
                                    }
                   });
});
