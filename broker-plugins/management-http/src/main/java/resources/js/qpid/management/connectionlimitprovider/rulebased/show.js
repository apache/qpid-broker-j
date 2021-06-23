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
        "dojo/_base/connect",
        "dojo/_base/event",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dijit/registry",
        "dojox/html/entities",
        "dgrid/Grid",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/ColumnHider",
        "dstore/Memory",
        "qpid/management/connectionlimitprovider/rulebased/load",
        "qpid/management/connectionlimitprovider/rulebased/rule",
        "dojo/text!connectionlimitprovider/rulebased/show.html",
        "dijit/TitlePane",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (declare,
              connect,
              event,
              parser,
              query,
              construct,
              registry,
              entities,
              Grid,
              Selection,
              Pagination,
              ColumnResizer,
              ColumnHider,
              MemoryStore,
              loadForm,
              ruleForm,
              template)
    {
        function RuleBased(containerNode, conLimitModelObj, controller)
        {
            const that = this;
            this.modelObj = conLimitModelObj;
            this.management = controller.management;
            this.category =
                conLimitModelObj &&
                (conLimitModelObj.type === "virtualhost" ||
                    conLimitModelObj.type === "virtualhostconnectionlimitprovider")
                    ? "VirtualHostConnectionLimitProvider"
                    : "BrokerConnectionLimitProvider";
            this._loadForm = loadForm;
            this._ruleForm = ruleForm;

            this._dataSore = new MemoryStore({data: [], idProperty: "id"});

            const node = construct.create("div", {innerHTML: template}, containerNode, "last");
            parser.parse(containerNode)
                .then(function ()
                {
                    that._frequencyPeriod = query(".defaultFrequencyPeriod", node)[0];

                    const addButton = registry.byNode(query(".addRule", node)[0]);
                    addButton.on("click", function (e)
                    {
                        event.stop(e);
                        that._ruleForm.show({
                            management: that.management,
                            modelObj: that.modelObj,
                            gridData: that.gridData,
                            type: that.type,
                            category: that.category
                        });
                    });

                    that._resetButton = registry.byNode(query(".resetCounters", node)[0]);
                    that._resetButton.on("click", function (e)
                    {
                        that._resetCounters(e)
                    });

                    that._clearButton = registry.byNode(query(".clearRules", node)[0]);
                    that._clearButton.on("click", function (e)
                    {
                        that._clear(e)
                    });

                    const loadButton = registry.byNode(query(".loadRules", node)[0]);
                    loadButton.on("click", function (e)
                    {
                        event.stop(e);
                        that._loadForm.show(that.management, that.modelObj);
                    });

                    const extractButton = registry.byNode(query(".extractRules", node)[0]);
                    extractButton.on("click", function (e)
                    {
                        event.stop(e);
                        that.management.downloadIntoFrame({
                            type: that.modelObj.type,
                            name: "extractRules",
                            parent: that.modelObj
                        });
                    });

                    const columnsDefinition = [
                        {
                            label: 'ID',
                            field: "id",
                            width: "30",
                            id: "ID",
                            hidden: true,
                        },
                        {
                            label: 'Identity',
                            field: "identity",
                            width: "150",
                            id: "identity",
                            hidden: false,
                        },
                        {
                            label: 'Port',
                            field: "port",
                            width: "100",
                            id: "port",
                            hidden: false,
                        },
                        {
                            label: 'Blocked',
                            field: "blocked",
                            width: "50",
                            id: "blocked",
                            hidden: false,
                        },
                        {
                            label: 'Count Limit',
                            field: "countLimit",
                            width: "100",
                            id: "count",
                            hidden: false,
                        },
                        {
                            label: 'Frequency Limit',
                            field: "frequencyLimit",
                            width: "100",
                            id: "frequency",
                            hidden: false,
                        },
                        {
                            label: 'Frequency period [ms]',
                            field: "frequencyPeriod",
                            width: "100",
                            id: "period",
                            hidden: false,
                        }
                    ];

                    const gridFactory = declare([Grid, Selection, Pagination, ColumnResizer, ColumnHider]);
                    that._ruleGrid = gridFactory(
                        {
                            collection: that._dataSore,
                            rowsPerPage: 10,
                            pagingLinks: 1,
                            firstLastArrows: true,
                            selectionMode: 'single',
                            className: 'dgrid-autoheight',
                            pageSizeOptions: [10, 25, 50, 100],
                            columns: columnsDefinition,
                            noDataMessage: 'No connection limit rules.'
                        }, query(".rules", node)[0]);

                    that._ruleGrid.on('dgrid-select', function (evn) {
                        const theItem = evn.rows[0].data;
                        that._ruleForm.show({
                            management: that.management,
                            modelObj: that.modelObj,
                            gridData: that.gridData,
                            id: theItem.id,
                            type: that.type,
                            category: that.category
                        });
                    });
                });
        }

        RuleBased.prototype.update = function (data)
        {
            this.type = data.type;
            this._frequencyPeriod.innerHTML = entities.encode(String(data.defaultFrequencyPeriod));

            const gridData = [];
            let i = 0;
            for (const rule of data.rules)
            {
                const newRule = {id: i};
                Object.assign(newRule, rule);
                gridData.push(newRule);
                i++;
            }
            this.gridData = gridData;
            this._dataSore.setData(gridData);
            if (Array.isArray(gridData) && gridData.length > 0)
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

        RuleBased.prototype._clear = function (e)
        {
            event.stop(e);
            if (confirm("Are you sure you want to clear all connection limit rules?"))
            {
                this._clearButton.set("disabled", true);
                this._resetButton.set("disabled", true);
                const modelObj = {
                    type: this.modelObj.type,
                    name: "clearRules",
                    parent: this.modelObj
                };
                const url = {url: this.management.buildObjectURL(modelObj)};

                const that = this;
                this.management.post(url, {})
                    .always(function ()
                    {
                        that._clearButton.set("disabled", false);
                        that._resetButton.set("disabled", false);
                    });
            }
        };

        RuleBased.prototype._resetCounters = function (e)
        {
            event.stop(e);
            if (confirm("Are you sure you want to reset all connection counters?"))
            {
                this._resetButton.set("disabled", true);
                this._clearButton.set("disabled", true);
                const modelObj = {
                    type: this.modelObj.type,
                    name: "resetCounters",
                    parent: this.modelObj
                };
                const url = {url: this.management.buildObjectURL(modelObj)};

                const that = this;
                this.management.post(url, {})
                    .always(function ()
                    {
                        that._resetButton.set("disabled", false);
                        that._clearButton.set("disabled", false);
                    });
            }
        };

        return RuleBased;
    });
