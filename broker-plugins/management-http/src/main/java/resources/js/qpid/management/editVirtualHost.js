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
define(["dojox/html/entities",
        "dojo/_base/array",
        "dojo/_base/declare",
        "dojo/_base/event",
        "dojo/_base/lang",
        "dojo/_base/window",
        "dojo/dom",
        "dojo/dom-construct",
        "dijit/registry",
        "dojo/parser",
        'dojo/json',
        "dojo/query",
        "dojo/store/Memory",
        "dojo/data/ObjectStore",
        "qpid/common/util",
        "dojo/text!editVirtualHost.html",
        "dgrid/OnDemandGrid",
        "dgrid/Selector",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/extensions/Pagination",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/DijitRegistry",
        "dstore/Memory",
        "dstore/Trackable",
        "dojo/keys",
        "qpid/common/ContextVariablesEditor",
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/FilteringSelect",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"],
    function (entities,
              array,
              declare,
              event,
              lang,
              win,
              dom,
              domConstruct,
              registry,
              parser,
              json,
              query,
              Memory,
              ObjectStore,
              util,
              template,
              Grid,
              Selector,
              Keyboard,
              Selection,
              Pagination,
              ColumnResizer,
              DijitRegistry,
              MemoryStore,
              TrackableStore,
              keys)
    {
        var numericFieldNames = ["storeTransactionIdleTimeoutWarn",
                                 "storeTransactionIdleTimeoutClose",
                                 "storeTransactionOpenTimeoutWarn",
                                 "storeTransactionOpenTimeoutClose",
                                 "housekeepingCheckPeriod",
                                 "housekeepingThreadCount",
                                 "connectionThreadPoolSize",
                                 "statisticsReportingPeriod"];

        var virtualHostEditor = {
            init: function ()
            {
                var that = this;
                this.containerNode = domConstruct.create("div", {innerHTML: template});
                parser.parse(this.containerNode)
                    .then(function (instances)
                    {
                        that._postParse();
                    });
            },
            _postParse: function ()
            {
                var that = this;
                this.allFieldsContainer = dom.byId("editVirtualHost.allFields");
                this.typeFieldsContainer = dom.byId("editVirtualHost.typeFields");
                this.dialog = registry.byId("editVirtualHostDialog");
                this.saveButton = registry.byId("editVirtualHost.saveButton");
                this.cancelButton = registry.byId("editVirtualHost.cancelButton");
                this.cancelButton.on("click", function (e)
                {
                    that._cancel(e);
                });
                this.saveButton.on("click", function (e)
                {
                    that._save(e);
                });
                // Add regexp to the numeric fields
                for (var i = 0; i < numericFieldNames.length; i++)
                {
                    registry.byId("editVirtualHost." + numericFieldNames[i]).set("regExpGen", util.numericOrContextVarRegexp);
                }
                this.form = registry.byId("editVirtualHostForm");
                this.form.on("submit", function ()
                {
                    return false;
                });
                this._createNodeAutoCreationPolicyUI();
            },
            show: function (management, modelObj)
            {
                this.management = management;
                this.modelObj = modelObj;
                if (!this.context)
                {
                    this.context = new qpid.common.ContextVariablesEditor({
                        name: 'context',
                        title: 'Context variables'
                    });
                    this.context.placeAt(dom.byId("editVirtualHost.context"));
                }
                this.dialog.set("title", "Edit Virtual Host - " + entities.encode(String(modelObj.name)));

                util.loadData(management, modelObj, lang.hitch(this, this._show));
            },
            destroy: function ()
            {
                if (this.dialog)
                {
                    this.dialog.destroyRecursive();
                    this.dialog = null;
                }

                if (this.containerNode)
                {
                    domConstruct.destroy(this.containerNode);
                    this.containerNode = null;
                }
            },
            _cancel: function (e)
            {
                this.dialog.hide();
            },
            _save: function (e)
            {
                event.stop(e);
                if (this.form.validate())
                {
                    var data = util.getFormWidgetValues(this.form, this.initialData);
                    var context = this.context.get("value");
                    if (context && !util.equals(context, this.initialData.context))
                    {
                        data["context"] = context;
                    }
                    var nodeAutoCreationPolicies = this._getNodeAutoCreationPolicies();

                    if (!util.equals(nodeAutoCreationPolicies, this.initialData.nodeAutoCreationPolicies))
                    {
                        data.nodeAutoCreationPolicies = nodeAutoCreationPolicies;
                    }

                    var that = this;
                    this.management.update(that.modelObj, data)
                        .then(function (x)
                        {
                            that.dialog.hide();
                        });
                }
                else
                {
                    alert('Form contains invalid data.  Please correct first');
                }
            },
            _show: function (data)
            {
                this.initialData = data.actual;
                this.form.reset();
                this.context.setData(data.actual.context, data.effective.context, data.inheritedActual.context);
                var that = this;

                var widgets = registry.findWidgets(this.typeFieldsContainer);
                array.forEach(widgets, function (item)
                {
                    item.destroyRecursive();
                });
                domConstruct.empty(this.typeFieldsContainer);

                require(["qpid/management/virtualhost/" + data.actual.type.toLowerCase() + "/edit"], function (TypeUI)
                {
                    try
                    {
                        var metadata = that.management.metadata;
                        TypeUI.show({
                            containerNode: that.typeFieldsContainer,
                            parent: that,
                            data: data.actual,
                            metadata: metadata
                        });
                    }
                    catch (e)
                    {
                        if (console && console.warn)
                        {
                            console.warn(e);
                        }
                    }
                });
                util.applyToWidgets(this.allFieldsContainer,
                    "VirtualHost",
                    data.actual.type,
                    data.actual,
                    this.management.metadata,
                    data.effective);
                this._initNodeAutoCreationPolicies(data.actual && data.actual.nodeAutoCreationPolicies ? data.actual.nodeAutoCreationPolicies : []);
                this.dialog.startup();
                this.dialog.show();
                if (!this.resizeEventRegistered)
                {
                    this.resizeEventRegistered = true;
                    util.resizeContentAreaAndRepositionDialog(dom.byId("editVirtualHost.contentPane"), this.dialog);
                }
                this._policyGrid.startup();
                this._policyGrid.refresh();
            },
            _createNodeAutoCreationPolicyUI: function () {
                this.addNodeAutoCreationPolicyButton =
                    registry.byId("editVirtualHost.addAutoCreationPolicy");
                this.addNodeAutoCreationPolicyButton.on("click",
                    lang.hitch(this, this._addNodeAutoCreationPolicy));

                this.deleteNodeAutoCreationPolicyButton =
                    registry.byId("editVirtualHost.deleteAutoCreationPolicy");
                this.deleteNodeAutoCreationPolicyButton.on("click",
                    lang.hitch(this, this._deleteNodeAutoCreationPolicy));
                this._policies = [];
                var Store = MemoryStore.createSubclass(TrackableStore);
                this._policyStore = new Store({
                    data: this._policies,
                    idProperty: "pattern"
                });
                var PolicyGrid = declare([Grid, Keyboard, Selector, Selection, ColumnResizer, DijitRegistry]);
                this._policyGrid = new PolicyGrid({
                    rowsPerPage: 10,
                    selectionMode: 'none',
                    deselectOnRefresh: false,
                    allowSelectAll: true,
                    cellNavigation: true,
                    className: 'dgrid-autoheight',
                    pageSizeOptions: [10, 20, 30, 40, 50, 100],
                    adjustLastColumn: true,
                    collection: this._policyStore,
                    highlightRow: function (){},
                    columns: {
                        selected: {
                            label: 'All',
                            selector: 'checkbox'
                        },
                        type: {
                            label: "Node Type"
                        },
                        pattern: {
                            label: "Pattern"
                        },
                        createdOnPublish: {
                            label: "Create On Publish"
                        },
                        createdOnConsume: {
                            label: "Create On Consume"
                        },
                        attributes: {
                            label: "Attributes",
                            sortable: false,
                            formatter: function (value, object) {
                                var markup = "";
                                if (value)
                                {
                                    markup = "<div class='keyValuePair'>";
                                    for (var key in value)
                                    {
                                        markup += "<div>" + entities.encode(String(key)) + "="
                                                  + entities.encode(String(value[key])) + "</div>";
                                    }
                                    markup += "</div>"
                                }
                                return markup;
                            }
                        }
                    }
                }, dom.byId("editVirtualHost.policies"));

                this._policyGrid.on('.dgrid-row:dblclick', lang.hitch(this, this._policySelected));
                this._policyGrid.on('.dgrid-row:keypress', lang.hitch(this, function (event) {
                    if (event.keyCode === keys.ENTER)
                    {
                        this._policySelected(event);
                    }
                }));
                this._policyGrid.on('dgrid-select', lang.hitch(this, this._policySelectionChanged));
                this._policyGrid.on('dgrid-deselect', lang.hitch(this, this._policySelectionChanged));

            },
            _toDgridFriendlyNodeAutoCreationPolicyObject: function (policy) {
                return { pattern: policy.pattern,
                         type: policy.nodeType,
                         attributes: policy.attributes,
                         createdOnPublish: policy.createdOnPublish,
                         createdOnConsume: policy.createdOnConsume};
            },
            _initNodeAutoCreationPolicies: function (policies) {

                var dgridFriendlyPolicies = [];
                for (var i = 0; i < policies.length; i++)
                {
                    dgridFriendlyPolicies.push(this._toDgridFriendlyNodeAutoCreationPolicyObject(policies[i]));
                }

                this._policies = dgridFriendlyPolicies;
                var Store = MemoryStore.createSubclass(TrackableStore);
                this._policyStore = new Store({
                    data: this._policies,
                    idProperty: "pattern"
                });
                this._policyGrid.set("collection", this._policyStore);
            },
            _addNodeAutoCreationPolicy: function () {
                this._showNodeAutoCreationPolicyForm({});
            },
            _showNodeAutoCreationPolicyForm: function (item) {
                if (this.nodeAutoCreationPolicyForm)
                {
                    this.nodeAutoCreationPolicyForm.show(item, this._getNodeAutoCreationPolicies());
                }
                else
                {
                    require(["qpid/management/virtualhost/NodeAutoCreationPolicyForm"],
                        lang.hitch(this, function (NodeAutoCreationPolicyForm) {
                            this.nodeAutoCreationPolicyForm =
                                new NodeAutoCreationPolicyForm({management: this.management});
                            this.nodeAutoCreationPolicyForm.on("create", lang.hitch(this, function (e) {
                                try
                                {
                                    this._policyStore.putSync(this._toDgridFriendlyNodeAutoCreationPolicyObject(e.data));
                                    if (e.oldData && e.oldData.pattern !== e.data.pattern)
                                    {
                                        this._policyStore.removeSync(e.oldData.pattern);
                                    }
                                }
                                catch (e)
                                {
                                    console.warn("Unexpected error" + e);
                                }
                                this._policyGrid.refresh({keepScrollPosition: true});
                            }));
                            this.nodeAutoCreationPolicyForm.show(item, this._getNodeAutoCreationPolicies());
                        }));
                }
            },
            _policySelected: function (event) {
                var row = this._policyGrid.row(event);
                this._showNodeAutoCreationPolicyForm(this._toNodeAutoCreationPolicyObject(row.data));
            },
            _deleteNodeAutoCreationPolicy: function () {
                var selected = this._getSelectedPolicies();
                if (selected.length > 0)
                {
                    for (var s in selected)
                    {
                        this._policyStore.removeSync(selected[s]);
                    }
                    this._policyGrid.clearSelection();
                }
            },
            _getSelectedPolicies: function () {
                var selected = [];
                var selection = this._policyGrid.selection;
                for (var item in selection)
                {
                    if (selection.hasOwnProperty(item) && selection[item])
                    {
                        selected.push(item);
                    }
                }
                return selected;
            },
            _policySelectionChanged: function () {
                var selected = this._getSelectedPolicies();
                this.deleteNodeAutoCreationPolicyButton.set("disabled", selected.length === 0);
            },
            _getNodeAutoCreationPolicies: function () {
                var policies = [];
                this._policyStore.fetchSync().forEach(lang.hitch(this, function (policy) {
                    policies.push(this._toNodeAutoCreationPolicyObject(policy));
                }));
                return policies;
            },
            _toNodeAutoCreationPolicyObject: function (policy) {
                var obj =  {
                    pattern: policy.pattern,
                    nodeType: policy.type,
                    attributes: policy.attributes
                };
                if (policy.createdOnPublish === true)
                {
                    obj.createdOnPublish = policy.createdOnPublish;
                }
                if (policy.createdOnConsume === true)
                {
                    obj.createdOnConsume = policy.createdOnConsume;
                }
                return obj;
            }
        };

        virtualHostEditor.init();

        return virtualHostEditor;
    });
