/*
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
        "dojo/json",
        "dojo/Evented",
        "dojo/Deferred",
        "dojo/promise/all",
        'dstore/Store',
        'dstore/QueryResults',
        "dstore/Memory",
        "dstore/Trackable",
        "dstore/Tree",
        "dgrid/OnDemandGrid",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/Tree",
        "dgrid/extensions/DijitRegistry",
        "dgrid/extensions/ColumnResizer",
        "qpid/management/query/StoreUpdater",
        "dojo/text!query/QueryBrowserWidget.html",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/Toolbar",
        "dijit/Dialog"],
    function (declare,
              lang,
              json,
              Evented,
              Deferred,
              all,
              Store,
              QueryResults,
              Memory,
              Trackable,
              TreeStoreMixin,
              OnDemandGrid,
              Keyboard,
              Selection,
              Tree,
              DijitRegistry,
              ColumnResizer,
              StoreUpdater,
              template)
    {
        var empty = new Deferred();
        empty.resolve([]);

        var PreferencesStore = declare("qpid.management.query.PreferencesStore",
            [Store],
            {
                management: null,
                transformer: null,
                filter: "all",
                fetch: function ()
                {
                    var filter = this.filter;
                    var userPreferencesPromise = filter === "all" || filter === "myQueries"
                        ? this.management.getUserPreferences({type: "broker"}, "query")
                        : empty.promise;
                    var visiblePreferencesPromise = filter === "all" || filter === "sharedWithMe"
                        ? this.management.getVisiblePreferences({type: "broker"}, "query")
                        : empty.promise;

                    var deferred = new Deferred();

                    var successHandler = lang.hitch(this, function (allPreferences)
                    {
                        var items = [];
                        for (var i = 0; i < allPreferences.length; i++)
                        {
                            var prefs = allPreferences[i];
                            for (var j = 0; j < prefs.length; j++)
                            {
                                items.push(prefs[j]);
                                prefs[j].visible = (i === 1);
                            }
                        }
                        if (this.transformer)
                        {
                            items = this.transformer(items);
                        }
                        deferred.resolve(items);
                    });
                    var failureHandler = lang.hitch(this, function (error)
                    {
                        if (error && (!error.hasOwnProperty("response") || error.response.hasOwnProperty("status")))
                        {
                            this.management.errorHandler(error);
                        }
                        deferred.resolve([]);
                    });

                    all([userPreferencesPromise, visiblePreferencesPromise]).then(successHandler, failureHandler);

                    return new QueryResults(deferred.promise, {
                        totalLength: deferred.promise.then(function (data)
                        {
                            return data.length;
                        }, function ()
                        {
                            return 0;
                        })
                    });

                },
                fetchRange: function ()
                {
                    throw new Error("unsupported");
                }
            });

        return declare("qpid.management.query.QueryBrowserWidget",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),
                // attached automatically from template fields
                queryBrowserGridNode: null,
                all: null,
                myQueries: null,
                sharedWithMe: null,

                // passed automatically by constructor
                manangement: null,

                // internal
                _preferenceStore: null,
                _gridStore: null,
                queryBrowserGrid: null,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._gridStore = new (declare([Memory, TreeStoreMixin, Trackable]))();
                    this._preferenceStore  = this._createPreferencesStore(this._gridStore);
                    this._buildGrid();
                    this._initFilter();
                },
                startup: function ()
                {
                    this.inherited(arguments);
                    this.queryBrowserGrid.startup();
                    this.update();
                },
                _buildGrid: function ()
                {
                    var columns = {
                        name: {label: "Query", renderExpando: true, sortable: false},
                        description: {label: "Description", sortable: false},
                        owner: {label: "Owner", sortable: false},
                        visibilityList: {label: "Shared with", sortable: false}
                    };

                    var Grid = declare([OnDemandGrid, Keyboard, Selection, Tree, DijitRegistry, ColumnResizer]);
                    var store = this._gridStore.filter({parent: undefined});
                    this.queryBrowserGrid = new Grid({
                        minRowsPerPage: Math.pow(2, 53) - 1,
                        columns: columns,
                        collection: store
                    }, this.queryBrowserGridNode);

                    this.queryBrowserGrid.on('.dgrid-row:dblclick', lang.hitch(this, this._openQuery));
                    this.queryBrowserGrid.on('.dgrid-row:keypress', lang.hitch(this, function (event)
                    {
                        if (event.keyCode === keys.ENTER)
                        {
                            this._openQuery(event);
                        }
                    }));
                },
                resize: function ()
                {
                    this.inherited(arguments);
                    if (this.queryBrowserGrid)
                    {
                        this.queryBrowserGrid.resize();
                    }
                },
                update: function ()
                {
                    this._preferenceStore.update();
                },
                _createPreferencesStore: function (targetStore)
                {
                    var UpdatableStore = declare([PreferencesStore, StoreUpdater]);
                    var preferencesStore = new UpdatableStore({
                        targetStore: targetStore,
                        management: this.management,
                        transformer: function (preferences)
                        {
                            var items = [];
                            var rootItem = {id: "broker", name: "Broker"};
                            items.push(rootItem);
                            var rootItemCategories = {};
                            for (var i = 0; i < preferences.length; i++)
                            {
                                var preferenceItem = preferences[i];
                                var categoryName = preferenceItem.value.category;
                                var categoryItem = rootItemCategories[categoryName];
                                if (!categoryItem)
                                {
                                    categoryItem = {id: categoryName, name: categoryName, parent: rootItem.id};
                                    items.push(categoryItem);
                                    rootItemCategories[categoryName] = categoryItem;
                                }
                                preferenceItem.hasChildren = false;
                                preferenceItem.parent = categoryItem.id;
                                items.push(preferenceItem);
                            }
                            return items;
                        }
                    });
                    return preferencesStore;
                },
                _openQuery: function (event)
                {
                    var row = this.queryBrowserGrid.row(event);
                    //TODO: we need the associated object hierarchy path
                    if (row.data.value)
                    {
                        this.emit("openQuery", {preference: row.data, parentObject: {type: "broker"}});
                    }
                },
                _initFilter: function ()
                {
                    var that = this;
                    this.all.on("click", function (e)
                    {
                        that._modifyFilter(e, that.all);
                    });
                    this.myQueries.on("click", function (e)
                    {
                        that._modifyFilter(e, that.myQueries);
                    });
                    this.sharedWithMe.on("click", function (e)
                    {
                        that._modifyFilter(e, that.sharedWithMe);
                    });
                },
                _modifyFilter: function (event, targetWidget)
                {
                    this._preferenceStore.filter = targetWidget.get("value");
                    this.update();
                }
            });
    });
