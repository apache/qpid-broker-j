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
        "dojo/_base/event",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dijit/registry",
        "dojox/html/entities",
        "dgrid/Grid",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dstore/Memory",
        "dojo/text!connectionlimitprovider/connectionlimitfile/show.html",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (declare,
              event,
              parser,
              query,
              construct,
              registry,
              entities,
              Grid,
              Pagination,
              ColumnResizer,
              MemoryStore,
              template)
    {
        function ConnectionLimitFile(containerNode, modelObj, controller)
        {
            const that = this;
            this.modelObj = modelObj;
            this.management = controller.management;

            const node = construct.create("div", {innerHTML: template}, containerNode, "last");

            this._dataSore = new MemoryStore({data: []});

            parser.parse(containerNode)
                .then(function ()
                {
                    that._path = query(".path", node)[0];
                    that._frequencyPeriod = query(".defaultFrequencyPeriod", node)[0];

                    that._reloadButton = registry.byNode(query(".reload", node)[0]);
                    that._reloadButton.on("click", function (e)
                    {
                        that._reload(e)
                    });

                    that._resetButton = registry.byNode(query(".resetCounters", node)[0]);
                    that._resetButton.on("click", function (e)
                    {
                        that._resetCounters(e)
                    });

                    const columnsDefinition = [
                        {
                            label: 'Identity',
                            field: "identity",
                            width: "150",
                            id: "identity"
                        },
                        {
                            label: 'Port',
                            field: "port",
                            width: "100",
                            id: "port"
                        },
                        {
                            label: 'Blocked',
                            field: "blocked",
                            width: "50",
                            id: "blocked"
                        },
                        {
                            label: 'Count Limit',
                            field: "countLimit",
                            width: "100",
                            id: "count"
                        },
                        {
                            label: 'Frequency Limit',
                            field: "frequencyLimit",
                            width: "100",
                            id: "frequency"
                        },
                        {
                            label: 'Frequency period [ms]',
                            field: "frequencyPeriod",
                            width: "100",
                            id: "period"
                        }
                    ];

                    const gridFactory = declare([Grid, Pagination, ColumnResizer]);
                    that._ruleGrid = gridFactory(
                        {
                            collection: that._dataSore,
                            rowsPerPage: 10,
                            firstLastArrows: true,
                            selectionMode: 'none',
                            className: 'dgrid-autoheight',
                            pageSizeOptions: [10, 25, 50, 100],
                            columns: columnsDefinition,
                            noDataMessage: 'No connection limit rules.'
                        }, query(".rules", node)[0]);
                });
        }

        ConnectionLimitFile.prototype.update = function (data)
        {
            this._path.innerHTML = entities.encode(String(data.path));
            this._frequencyPeriod.innerHTML = entities.encode(String(data.defaultFrequencyPeriod));
            this._dataSore.setData(data.rules);
            if (Array.isArray(data.rules) && data.rules.length > 0)
            {
                this._ruleGrid.refresh(this._refreshOption || {});
                this._refreshOption = {keepCurrentPage: true, keepScrollPosition: true};
            }
            else
            {
                this._ruleGrid.refresh();
                this._refreshOption = {};
            }
        };

        ConnectionLimitFile.prototype._reload = function (e)
        {
            event.stop(e);
            this._reloadButton.set("disabled", true);
            this._resetButton.set("disabled", true);
            const modelObj = {
                type: this.modelObj.type,
                name: "reload",
                parent: this.modelObj
            };
            const url = this.management.buildObjectURL(modelObj);
            const that = this;
            this.management.post({url: url}, {})
                .then(null, management.xhrErrorHandler)
                .always(function ()
                {
                    that._reloadButton.set("disabled", false);
                    that._resetButton.set("disabled", false);
                });
        };

        ConnectionLimitFile.prototype._resetCounters = function (e)
        {
            event.stop(e);
            if (confirm("Are you sure you want to reset all connection counters?"))
            {
                this._resetButton.set("disabled", true);
                this._reloadButton.set("disabled", true);
                const modelObj = {
                    type: this.modelObj.type,
                    name: "resetCounters",
                    parent: this.modelObj
                };
                const url = this.management.buildObjectURL(modelObj);
                const that = this;
                this.management.post({url: url}, {})
                    .then(null, management.xhrErrorHandler)
                    .always(function ()
                    {
                        that._resetButton.set("disabled", false);
                        that._reloadButton.set("disabled", false);
                    });
            }
        };

        return ConnectionLimitFile;
    });
