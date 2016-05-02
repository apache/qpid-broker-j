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
define(["dojo/dom",
        "dojo/query",
        "dojo/_base/array",
        "dojo/_base/connect",
        "dojo/_base/lang",
        "dojo/store/Memory",
        "dijit/registry",
        "qpid/common/util",
        "dojo/text!groupprovider/cloudfoundrydashboardmanagement/add.html",
        "dijit/form/FilteringSelect",
        "dojo/domReady!"], function (dom, query, array, connect, lang, Memory, registry, util, template)
{

    return {
        _nextGridItemId: 0,

        show: function (data)
        {
            var that = this;
            this.containerNode = data.containerNode;
            util.parse(data.containerNode, template, function ()
            {
                that._postParse(data);
            });
        },

        _postParse: function (data)
        {
            var that = this;
            util.makeInstanceStore(data.parent.management, "Broker", "TrustStore", function (trustStoresStore)
            {
                var trustStore = registry.byNode(query(".trustStore", data.containerNode)[0]);
                trustStore.set("store", trustStoresStore);
                if (data.data)
                {
                    util.initialiseFields(data.data,
                        data.containerNode,
                        data.parent.management.metadata,
                        "GroupProvider",
                        "CloudFoundryDashboardManagement");
                }
            });

            this._setupGrid();
            if (data.data && data.data.serviceToManagementGroupMapping)
            {
                this._populateGrid(data.data.serviceToManagementGroupMapping);
            }
        },

        _setupGrid: function ()
        {
            var that = this;
            var gridNode = query(".serviceToManagementGroupMapping", this.containerNode)[0];
            var addButtonNode = query(".addButton", this.containerNode)[0];
            var deleteButtonNode = query(".deleteButton", this.containerNode)[0];
            var addButton = registry.byNode(addButtonNode);
            var deleteButton = registry.byNode(deleteButtonNode);
            var layout = [[{
                name: "Service Instance Id",
                field: "serviceInstanceId",
                width: "50%",
                editable: true
            }, {
                name: 'Group Name',
                field: 'groupName',
                width: '50%',
                editable: true
            }]];
            var data = [];
            var objectStore = new dojo.data.ObjectStore({
                objectStore: new Memory({
                    data: data,
                    idProperty: "id"
                })
            });
            var grid = new dojox.grid.EnhancedGrid({
                selectionMode: "multiple",
                store: objectStore,
                singleClickEdit: true,
                structure: layout,
                autoHeight: true,
                sortFields: [{
                    attribute: 'serviceInstanceId',
                    descending: false
                }],
                plugins: {indirectSelection: true}
            }, gridNode);

            grid.name = "serviceToManagementGroupMapping";
            this._grid = grid;

            var toggleGridButtons = function (index)
            {
                var data = grid.selection.getSelected();
                deleteButton.set("disabled", !data || data.length == 0);
            };

            connect.connect(grid.selection, 'onSelected', toggleGridButtons);
            connect.connect(grid.selection, 'onDeselected', toggleGridButtons);

            deleteButton.set("disabled", true);
            addButton.on("click", function (event)
            {
                that._newItem();
            });
            deleteButton.on("click", function (event)
            {
                that._deleteSelected();
            });
            grid._getValueAttr = function ()
            {
                var value = {};
                that._grid.store.fetch({
                    onComplete: function (items, request)
                    {
                        array.forEach(items, function (item)
                        {
                            value[item.serviceInstanceId] = item.groupName;
                        });
                    }
                });
                return value;
            };

            grid.startup();
        },

        _populateGrid: function (serviceToManagementGroupMapping)
        {
            var that = this;
            // delete previous store data
            this._grid.store.fetch({
                onComplete: function (items, request)
                {
                    if (items.length)
                    {
                        array.forEach(items, function (item)
                        {
                            that._grid.store.deleteItem(item);
                        });
                    }
                }
            });

            // add new data into grid store
            this._nextGridItemId = 0;
            for (var serviceInstanceId in serviceToManagementGroupMapping)
            {
                var groupName = serviceToManagementGroupMapping[serviceInstanceId];
                var storeItem = {
                    id: this._nextId(),
                    serviceInstanceId: serviceInstanceId,
                    groupName: groupName
                };
                this._grid.store.newItem(storeItem);
            }
            this._grid.store.save();
        },

        _nextId: function ()
        {
            this._nextGridItemId = this._nextGridItemId + 1;
            return this._nextGridItemId;
        },

        _newItem: function ()
        {
            var newItem = {
                id: this._nextId(),
                serviceInstanceId: "",
                groupName: ""
            };
            var grid = this._grid;
            grid.store.newItem(newItem);
            grid.store.save();
            grid.store.fetch({
                onComplete: function (items, request)
                {
                    var rowIndex = items.length - 1;
                    window.setTimeout(function ()
                    {
                        grid.focus.setFocusIndex(rowIndex, 1);
                    }, 10);
                }
            });
        },

        _deleteSelected: function ()
        {
            var that = this;
            var grid = this._grid;
            var data = grid.selection.getSelected();
            if (data.length > 0)
            {
                array.forEach(data, function (selectedItem)
                {
                    if (selectedItem !== null)
                    {
                        grid.store.deleteItem(selectedItem);
                    }
                });
                grid.store.save();
                grid.selection.deselectAll();
            }
        }

    };
});
