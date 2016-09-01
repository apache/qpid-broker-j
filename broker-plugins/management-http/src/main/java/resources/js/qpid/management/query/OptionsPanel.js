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
        "dojo/Evented",
        "dijit/layout/ContentPane",
        "dijit/form/Button",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "qpid/management/query/SearchTextBox",
        "dojo/domReady!"],
    function (declare,
              lang,
              array,
              json,
              domConstruct,
              template,
              Grid,
              Keyboard,
              Selection,
              Selector,
              DijitRegistry,
              Pagination,
              Memory,
              Evented)
    {

        var Summary = declare(null, {
            showFooter: true,
            buildRendering: function ()
            {
                this.inherited(arguments);
                this.summaryAreaNode = domConstruct.create('div', {
                    className: 'dgrid-status',
                    role: 'row',
                    style: {
                        overflow: 'hidden',
                        display: 'none'
                    }
                }, this.footerNode);
                this.totalLabelNode = domConstruct.create('span', {
                    innerHTML: "Total: "
                }, this.summaryAreaNode);
                this.totalValueNode = domConstruct.create('span', {
                    innerHTML: "0"
                }, this.summaryAreaNode);
            },
            setTotal: function (total)
            {
                if (total > 0)
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

        return declare("qpid.management.query.OptionsPanel",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
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
                renderItem: null, // function to render grid cells

                /**
                 * widget inner fields
                 */
                _optionsGrid: null,
                _descending: false,
                _selectedItems: null,
                _selectedIds: null,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                startup: function ()
                {
                    this.inherited(arguments);
                    this._optionsGrid.startup();
                },
                _postCreate: function ()
                {
                    this._selectedItems = [];
                    this._selectedIds = {};
                    this.search.on("change", lang.hitch(this, this._searchChanged));
                    this.search.on("keyUp", lang.hitch(this, function (evt)
                    {
                        if (evt.keyCode == dojo.keys.ENTER && this.search.value)
                        {
                            this._applyFilter();
                        }
                    }));
                    this._buildOptionsGrid();
                    this._selectionChanged();
                    this.buttons.style.display = this.showButtons ? '' : 'none';
                },
                _buildOptionsGrid: function ()
                {
                    var constructors = [Grid, Keyboard, Selector];
                    if (this.showSummary)
                    {
                        constructors.push(Summary);
                    }
                    var CustomGrid = declare(constructors);
                    if (!this.store)
                    {
                        this.store = new Memory({
                            data: this.items || [],
                            idProperty: this.idProperty
                        });
                    }
                    var grid = new CustomGrid({
                        columns: this._getOptionColumns(),
                        collection: this.store,
                        selectionMode: 'multiple',
                        cellNavigation: true,
                        allowSelectAll: true,
                        minRowsPerPage: this.items ? this.items.length : 100,
                        deselectOnRefresh: false
                    }, this.optionsGrid);
                    grid.on('dgrid-select', lang.hitch(this, this._gridSelected));
                    grid.on('dgrid-deselect', lang.hitch(this, this._gridDeselected));
                    grid.on('dgrid-sort', lang.hitch(this, function (event)
                    {
                        this._descending = event.sort[0].descending
                    }));
                    if (this.showSummary)
                    {
                        grid.setTotal(this.items ? this.items.length : 0);
                    }
                    this._optionsGrid = grid;
                },
                _gridSelected: function (event)
                {
                    for (var i = 0; i < event.rows.length; ++i)
                    {
                        var item = event.rows[i].data;
                        var id = item[this.idProperty];
                        if (!this._selectedIds[id])
                        {
                            this._selectedItems.push(item);
                            this._selectedIds[id] = true;
                        }
                    }
                    this._selectionChanged();
                },
                _gridDeselected: function (event)
                {
                    for (var i = 0; i < event.rows.length; ++i)
                    {
                        var id = event.rows[i].id;
                        for (var j = 0; j < this._selectedItems.length; ++j)
                        {
                            if (this._selectedItems[j][this.idProperty] === id)
                            {
                                this._selectedItems.splice(j, 1);
                                delete this._selectedIds[id];
                                break;
                            }
                        }
                    }
                    this._selectionChanged();
                },
                _setDataAttr: function (data)
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
                    if (data.items)
                    {
                        store = new Memory({
                            data: data.items,
                            idProperty: this.idProperty
                        });
                        this.items = data.items;
                    }

                    if (store)
                    {
                        this.store = store;
                        this._optionsGrid.set("columns", this._getOptionColumns());
                        this._optionsGrid.set("minRowsPerPage", data.items ? data.items.length : 100);
                        this._optionsGrid.set("sort", [{
                            property: this.nameProperty,
                            descending: this._descending
                        }]);
                        this._applyFilter();
                        if (this.showSummary)
                        {
                            this._optionsGrid.setTotal(this.items ? this.items.length : 0);
                        }
                    }

                    if (data.selected)
                    {
                        this._selectGrid(data.selected);
                    }
                },
                _findItemById: function (items, idValue)
                {
                    if (!items)
                    {
                        return null;
                    }
                    for (var i = 0; i < items.length; ++i)
                    {
                        if (items[i][this.idProperty] === idValue)
                        {
                            return items[i];
                        }
                    }
                    return null;
                },
                _selectGrid: function (selected)
                {
                    var items = [];
                    if (selected && selected.length && !selected[0].hasOwnProperty(this.idProperty))
                    {
                        for (var i = 0; i < selected.length; ++i)
                        {
                            var item = this._findItemById(this.items, selected[i]);
                            if (item)
                            {
                                items.push(item);
                            }
                        }
                    }
                    else
                    {
                        items = lang.clone(selected);
                    }

                    var selectedItems = lang.clone(this._selectedItems);
                    for (var i = 0; i < selectedItems.length; ++i)
                    {
                        var currentItem = selectedItems[i];
                        var item = this._findItemById(items, currentItem[this.idProperty]);
                        if (!item)
                        {
                            this._optionsGrid.deselect(currentItem);
                        }
                    }
                    for (var i = 0; i < items.length; ++i)
                    {
                        var currentItem = items[i];
                        var item = this._findItemById(this._selectedItems, currentItem[this.idProperty]);
                        if (!item)
                        {
                            this._optionsGrid.select(currentItem);
                        }
                    }
                    this._selectedItems = items;
                    this._selectionChanged();
                },
                _onClear: function ()
                {
                    this.search.set("value", "");
                },
                _applyFilter: function ()
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
                _searchChanged: function ()
                {
                    this.defer(this._applyFilter);
                },
                _selectionChanged: function (event)
                {
                    this.doneButton.set("disabled", this._selectedItems.length === 0);
                    this.emit("change", {value: lang.clone(this._selectedItems)});
                },
                _getOptionColumns: function ()
                {
                    var columns = {
                        selected: {
                            label: 'All',
                            selector: 'checkbox'
                        }
                    };
                    columns[this.nameProperty] = {
                        label: "Name",
                        sortable: true
                    };
                    if (this.renderItem)
                    {
                        columns[this.nameProperty].renderCell = this.renderItem;
                    }
                    return columns;
                },
                _getSelectedItemsAttr: function ()
                {
                    return lang.clone(this._selectedItems);
                },
                _getValueAttr: function ()
                {
                    return lang.clone(this._selectedItems);
                },
                resetItems: function (items)
                {
                    this._onClear();

                    if (items)
                    {
                        this._selectGrid(items);
                    }
                },
                resizeGrid: function ()
                {
                    this._optionsGrid.resize();
                },
                _setRenderItemAttr: function(renderItem)
                {
                    this.renderItem = renderItem;
                    if (this._optionsGrid != null)
                    {
                        this._optionsGrid.set("columns", this._getOptionColumns());
                    }
                }
            });

    });
