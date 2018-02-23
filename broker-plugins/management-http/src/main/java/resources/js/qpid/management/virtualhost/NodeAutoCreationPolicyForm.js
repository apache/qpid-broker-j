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
        "dojo/Evented",
        "dojo/keys",
        "dojo/text!virtualhost/NodeAutoCreationPolicyForm.html",
        "dgrid/OnDemandGrid",
        "dgrid/Selector",
        "dgrid/Keyboard",
        "dgrid/Selection",
        "dgrid/Editor",
        "dgrid/extensions/ColumnResizer",
        "dgrid/extensions/DijitRegistry",
        "dstore/Memory",
        "dstore/Trackable",
        "dstore/legacy/DstoreAdapter",
        "dijit/Dialog",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/CheckBox",
        "dijit/form/ComboBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dojo/domReady!"],
    function (declare,
              lang,
              Evented,
              keys,
              template,
              Grid,
              Selector,
              Keyboard,
              Selection,
              Editor,
              ColumnResizer,
              DijitRegistry,
              Memory,
              Trackable) {

        var Store = Memory.createSubclass(Trackable);
        return declare("qpid.management.virtualhost.NodeAutoCreationPolicyForm",
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
                pattern: null,
                attributes: null,
                createdOnPublish: null,
                createdOnConsume: null,
                okButton: null,
                cancelButton: null,
                nodeAutoCreationPolicyForm: null,
                addAttributeButton: null,
                deleteAttributeButton: null,
                nodeAutoCreationPolicyDialog: null,
                type: null,

                /**
                 * constructor arguments
                 */
                management: null,
                /**
                 * private fields
                 */
                _store: null,
                _policy: null,
                _attributesGrid: null,
                _id: 0,
                _nodeAutoCreationPolicyDialog: null,

                postCreate: function () {
                    this.inherited(arguments);
                    this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                    this.okButton.on("click", lang.hitch(this, this._onFormSubmit));
                    this.pattern.on("change", lang.hitch(this, this._onChange));
                    this.createdOnPublish.on("change", lang.hitch(this, this._onChange));
                    this.createdOnConsume.on("change", lang.hitch(this, this._onChange));
                    this.addAttributeButton.on("click", lang.hitch(this, this._addAttribute));
                    this.deleteAttributeButton.on("click", lang.hitch(this, this._deleteAttribute));
                    this.okButton.set("disabled", true);
                    this.deleteAttributeButton.set("disabled", true);
                    this.type.on("change", lang.hitch(this, this._onChange));
                    this._store = new Store({data: [], idProperty: "id"});
                },
                show: function(policyData, policies)
                {
                    this._policy = policyData ? policyData: {};
                    this._policies = policies || [];
                    var attributes = [];
                    this._id = 0;
                    if (this._policy.attributes)
                    {
                        for(var a in this._policy.attributes)
                        {
                            if (this._policy.attributes.hasOwnProperty(a))
                            {
                                var id = (++this._id);
                                attributes.push({
                                    id: id,
                                    name: a,
                                    value: this._policy.attributes[a]
                                });
                            }
                        }
                    }
                    this._store = new Store({data: attributes, idProperty: "id"});
                    this.type.set("value", this._policy.nodeType ? this._policy.nodeType : "Queue");
                    this.pattern.set("value", this._policy.pattern ? this._policy.pattern : "");
                    this.createdOnPublish.set("checked", this._policy.createdOnPublish);
                    this.createdOnConsume.set("checked", this._policy.createdOnConsume);
                    this.addAttributeButton.set("disabled", false);
                    this.nodeAutoCreationPolicyDialog.show();
                    this._initAttributesGrid();
                },
                _onCancel: function () {
                    this.nodeAutoCreationPolicyDialog.hide();
                    this.emit("cancel");
                },
                _onChange: function () {
                    var invalid = !this.pattern.value ||
                                  !(this.type.value) ||
                                  !(this.createdOnPublish.checked || this.createdOnConsume.checked) ||
                                  this.addAttributeButton.get("disabled");
                    this.okButton.set("disabled", invalid);
                },
                _onFormSubmit: function () {
                    try
                    {
                        if (this.nodeAutoCreationPolicyForm.validate())
                        {
                            var nodeType = this.type.value;
                            var category = nodeType.charAt(0).toUpperCase() + nodeType.substring(1);
                            if (this.management.metadata.metadata[category])
                            {
                                if (this._isUniquePattern(this.pattern.value))
                                {
                                    var data = {
                                        pattern: this.pattern.value,
                                        nodeType: nodeType,
                                        createdOnPublish: this.createdOnPublish.checked,
                                        createdOnConsume: this.createdOnConsume.checked,
                                        attributes: this._getObjectAttributes()
                                    };
                                    this.emit("create", {data: data, oldData: this._policy});
                                    this.nodeAutoCreationPolicyDialog.hide();
                                }
                                else
                                {
                                    alert('The auto-creation policy with the same pattern already exists');
                                }
                            }
                            else
                            {
                                alert('Specified node type does not exist. Please enter valid node type');
                            }
                        }
                        else
                        {
                            alert('Form contains invalid data.  Please correct first');
                        }
                    }
                    catch (e)
                    {
                        console.warn(e);
                    }
                    return false;
                },
                _addAttribute: function () {
                    var id = (++this._id);
                    var item = {
                        name: "",
                        value: "",
                        id: id
                    };
                    this.addAttributeButton.set("disabled", true);
                    try
                    {
                        this._store.addSync(item);
                        this._attributesGrid.edit(this._attributesGrid.cell(item, "name"));
                    }
                    catch(e)
                    {
                        console.error("failure to add new attribute:" + e);
                    }
                    this._onChange();
                },
                _deleteAttribute: function () {
                    var selected = this._getSelectedAttributes();
                    if (selected.length > 0)
                    {
                        for (var s in selected)
                        {
                            if (selected.hasOwnProperty(s))
                            {
                                var id = selected[s];
                                var item =  this._store.getSync(id);
                                if (item)
                                {
                                    this._store.removeSync(selected[s]);
                                }
                            }
                        }
                        this._attributesGrid.clearSelection();
                    }
                    this.addAttributeButton.set("disabled", this._emptyPatternFound());
                    this._onChange();
                },
                _getSelectedAttributes: function () {
                    var selected = [];
                    var selection = this._attributesGrid.selection;
                    for(var item in selection)
                    {
                        if (selection.hasOwnProperty(item) && selection[item])
                        {
                            selected.push(item);
                        }
                    }
                    return selected;
                },
                _onGridEdit: function (e) {
                    this.addAttributeButton.set("disabled", this._emptyPatternFound());
                    this._onChange();
                },
                _gridSelectionChanged: function () {
                    var selected = this._getSelectedAttributes();
                    this.deleteAttributeButton.set("disabled", selected.length === 0);
                },
                _getObjectAttributes: function () {
                    var attributes = {};
                    this._store.fetchSync().forEach(function (entry) {
                        attributes[entry.name] = entry.value;
                    });
                    return attributes;
                },
                _isUniquePattern: function(pattern)
                {
                    if (this._policy && this._policy.pattern === pattern)
                    {
                        // no change to the pattern
                        return true;
                    }
                    for (var i=0;i<this._policies.length;i++)
                    {
                        if (this._policies[i].pattern === pattern)
                        {
                            return false;
                        }
                    }
                    return true;
                },
                _initAttributesGrid: function()
                {
                    if (this._attributesGrid )
                    {
                        this._attributesGrid.set("collection", this._store);
                    }
                    else
                    {
                        var CustomGrid = declare([Grid, Selector, Editor, Keyboard, DijitRegistry]);
                        this._attributesGrid = new CustomGrid({
                            selectionMode: 'none',
                            deselectOnRefresh: false,
                            allowSelectAll: true,
                            cellNavigation: true,
                            adjustLastColumn: true,
                            collection: this._store,
                            sort: "id",
                            showHeader: true,
                            highlightRow: function (){},
                            columns: {
                                selected: {
                                    label: 'All',
                                    selector: 'checkbox'
                                },
                                name: {
                                    label: 'Name',
                                    editor: 'text',
                                    autoSelect: true,
                                    autoSave: true,
                                    editOn  : "click,dgrid-cellfocusin"
                                },
                                value: {
                                    label: "Value",
                                    editor: 'text',
                                    autoSelect: true,
                                    autoSave: true,
                                    editOn: "click,dgrid-cellfocusin"
                                }
                            }
                        }, this.attributes);
                        this._attributesGrid.on("dgrid-datachange", lang.hitch(this, this._onGridEdit));
                        this._attributesGrid.on('dgrid-select', lang.hitch(this, this._gridSelectionChanged));
                        this._attributesGrid.on('dgrid-deselect', lang.hitch(this, this._gridSelectionChanged));

                        // Header TAB navigation does not work reliably with Editor and Selector
                        // This is a work around to always focus first cell in first row if grid is not empty
                        this._attributesGrid.addKeyHandler(keys.TAB, lang.hitch(this, function (event) {
                            var range = this._store.fetchRangeSync({start: 0, end: 1});
                            if (range && range[0])
                            {
                                var cell = this._attributesGrid.cell(range[0], "selected");
                                if (cell)
                                {
                                    this._attributesGrid.focus(cell);
                                    event.preventDefault();
                                    event.stopPropagation();
                                }
                            }
                        }), true);
                        this._attributesGrid.startup();
                    }
                },
                _emptyPatternFound: function () {
                    var emptyPatternDetected = false;
                    this._store.fetchSync()
                        .forEach(function (value) {
                            if (value && value.pattern === "")
                            {
                                emptyPatternDetected = true;
                            }
                        });
                    return emptyPatternDetected;
                }
            });
    });
