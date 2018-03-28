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
        "dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dijit/registry",
        "dojox/html/entities",
        "dojo/text!accesscontrolprovider/showRuleBased.html",
        "dgrid/Grid",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/DijitRegistry",
        "dstore/Memory",
        "dstore/Trackable",
        "qpid/common/util",
        "dijit/TitlePane",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (declare,
              lang,
              dom,
              parser,
              query,
              construct,
              registry,
              entities,
              template,
              Grid,
              Keyboard,
              Selection,
              Pagination,
              ColumnResizer,
              DijitRegistry,
              MemoryStore,
              TrackableStore,
              util) {
        function RuleBased(containerNode, aclProviderObj, controller)
        {
            this.modelObj = aclProviderObj;
            this.management = controller.management;
            var node = construct.create("div", null, containerNode, "last");
            node.innerHTML = template;
            parser.parse(containerNode)
                .then(lang.hitch(this, function (instances) {

                    this.defaultResult = query(".defaultResult", node)[0];

                    var Store = MemoryStore.createSubclass(TrackableStore);
                    this._rulesStore = new Store({
                        data: [],
                        idProperty: "id"
                    });

                    var GridConstructor = declare([Grid,
                                                   Keyboard,
                                                   Selection,
                                                   Pagination,
                                                   ColumnResizer,
                                                   DijitRegistry]);
                    this._rulesGrid = new GridConstructor({
                        rowsPerPage: 20,
                        selectionMode: 'none',
                        deselectOnRefresh: false,
                        allowSelectAll: true,
                        cellNavigation: true,
                        className: 'dgrid-autoheight',
                        pageSizeOptions: [10, 20, 30, 40, 50, 100],
                        adjustLastColumn: true,
                        collection: this._rulesStore,
                        highlightRow: function () {
                        },
                        columns: [
                            {
                                label: 'Identity',
                                field: "identity"
                            }, {
                                label: "Object Type",
                                field: "objectType"
                            }, {
                                label: "Operation",
                                field: "operation"
                            }, {
                                label: "Outcome",
                                field: "outcome"
                            }, {
                                label: "Attributes",
                                field: "attributes",
                                sortable: false,
                                formatter: function (value, object) {
                                    var markup = "";
                                    if (value)
                                    {
                                        markup = "<div class='keyValuePair'>";
                                        for (var key in value)
                                        {
                                            if (value.hasOwnProperty(key))
                                            {
                                                markup += "<div>" + entities.encode(String(key)) + "="
                                                          + entities.encode(String(value[key])) + "</div>";
                                            }
                                        }
                                        markup += "</div>"
                                    }
                                    return markup;
                                }
                            }
                        ]
                    }, query(".rules", node)[0]);

                    this._rulesGrid.startup();

                    this.loadButton = registry.byNode(query(".load", node)[0]);
                    this.loadButton.on("click", lang.hitch(this, this.load));

                    this.extractButton = registry.byNode(query(".extract", node)[0]);
                    this.extractButton.on("click", lang.hitch(this, this.extractRules));
                }));
        }

        RuleBased.prototype.update = function (data) {
            this.defaultResult.innerHTML = entities.encode(String(data.defaultResult));

            var rules = [];
            if (data && data.rules)
            {
                for (var i = 0; i < data.rules.length; i++)
                {
                    var rule = {id: Number(i)};
                    lang.mixin(rule, data.rules[i]);
                    rules.push(rule);
                }
            }

            util.updateSyncDStore(this._rulesStore, rules, "id");
        };

        RuleBased.prototype.load = function () {
            this.loadButton.set("disabled", true);
            if (this.loadForm)
            {
                this.loadForm.show();
            }
            else
            {
                require(["qpid/management/accesscontrolprovider/rulebased/LoadForm"],
                    lang.hitch(this, function (LoadForm) {
                        this.loadForm = new LoadForm();
                        this.loadForm.on("load", lang.hitch(this, this.loadFromFile));
                        this.loadForm.on("hide", lang.hitch(this, function () {
                            this.loadButton.set("disabled", false);
                        }));
                        this.loadForm.show();
                    }));
            }
        };

        RuleBased.prototype.loadFromFile = function (event) {
            this.management.update({
                type: this.modelObj.type,
                name: "loadFromFile",
                parent: this.modelObj
            }, {path: event.path})
                .then(lang.hitch(this, function(){
                    this.loadForm.hide();
                    }),
                    lang.hitch(this, function(error){
                        util.xhrErrorHandler(error);
                        this.loadForm.reset();
                    }));
        };

        RuleBased.prototype.extractRules = function () {
            this.management.downloadIntoFrame({
                type: this.modelObj.type,
                name: "extractRules",
                parent: this.modelObj
            });
        };

        return RuleBased;
    });
