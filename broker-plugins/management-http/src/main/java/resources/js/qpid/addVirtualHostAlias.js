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
define(["dojo/_base/lang",
        "dojo/dom",
        "dojo/dom-construct",
        "dijit/registry",
        "dojo/parser",
        "dojo/store/Memory",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/json',
        "qpid/common/util",
        "dojo/text!addLogInclusionRule.html",
        "dojo/store/Memory",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/Textarea",
        "dijit/form/ComboBox",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dijit/layout/ContentPane",
        "dojox/layout/TableContainer",
        "dojo/domReady!"], function (lang, dom, construct, registry, parser, memory, array, event, json, util, template)
{
    var addLogInclusionRule = {
        init: function ()
        {
            var that = this;
            this.containerNode = construct.create("div", {innerHTML: template});
            parser.parse(this.containerNode)
                .then(function (instances)
                {
                    that._postParse();
                });
        },
        _postParse: function ()
        {
            var that = this;
            this.name = registry.byId("addLogInclusionRule.name");
            this.name.set("regExpGen", util.nameOrContextVarRegexp);

            this.dialog = registry.byId("addLogInclusionRule");
            this.addButton = registry.byId("addLogInclusionRule.addButton");
            this.cancelButton = registry.byId("addLogInclusionRule.cancelButton");
            this.cancelButton.on("click", function (e)
            {
                that._cancel(e);
            });
            this.addButton.on("click", function (e)
            {
                that._add(e);
            });

            this.typeFieldsContainer = dom.byId("addLogInclusionRule.typeFields");
            this.form = registry.byId("addLogInclusionRule.form");
            this.form.on("submit", function ()
            {
                return false;
            });

            this.logInclusionRuleType = registry.byId("addLogInclusionRule.type");
            this.logInclusionRuleType.on("change", function (type)
            {
                that._typeChanged(type);
            });

            this.durable = registry.byId("addLogInclusionRule.durable");
            this.allFieldsContainer = dom.byId("addLogInclusionRule.contentPane");
        },
        show: function (management, modelObj, category, actualData)
        {
            this.management = management;
            this.modelObj = modelObj;
            var metadata = management.metadata;
            this.category = category;
            this.configured = false;
            this._destroyTypeFields(this.typeFieldsContainer);
            this.logInclusionRuleType.set("store",
                util.makeTypeStoreFromMetadataByCategory(management.metadata, category));
            this.form.reset();

            this.initialData = actualData;
            this.isNew = !actualData;

            this.name.set("disabled", !this.isNew);
            this.logInclusionRuleType.set("disabled", !this.isNew);
            this.durable.set("disabled", !this.isNew);
            this.dialog.set("title",
                this.isNew ? "Add Log Inclusion Rule" : "Edit Log Inclusion Rule - " + actualData.name);

            if (actualData)
            {
                this._configure(actualData.type);
            }

            this.dialog.show();
        },
        _cancel: function (e)
        {
            event.stop(e);
            this._destroyTypeFields(this.typeFieldsContainer);
            this.dialog.hide();
        },
        _add: function (e)
        {
            event.stop(e);
            this._submit();
        },
        _submit: function ()
        {
            if (this.form.validate())
            {
                var that = this;
                var formData = util.getFormWidgetValues(this.form, this.initialData);

                if (this.isNew)
                {
                    this.management.create(this.category, this.modelObj, formData)
                        .then(function (x)
                        {
                            that.dialog.hide();
                        });
                }
                else
                {
                    this.management.update(this.modelObj, formData)
                        .then(function (x)
                        {
                            that.dialog.hide();
                        });
                }
            }
            else
            {
                alert('Form contains invalid data. Please correct first');
            }
        },
        _destroyTypeFields: function (typeFieldsContainer)
        {
            var widgets = registry.findWidgets(typeFieldsContainer);
            array.forEach(widgets, function (item)
            {
                item.destroyRecursive();
            });
            construct.empty(typeFieldsContainer);
        },
        _typeChanged: function (type)
        {
            this._destroyTypeFields(this.typeFieldsContainer);

            if (type)
            {
                this._configure(type);
                var that = this;
                require(["qpid/management/loginclusionrule/" + this.category.toLowerCase() + "/" + type.toLowerCase()
                         + "/add"], function (typeUI)
                {
                    try
                    {
                        var metadata = that.management.metadata;
                        var promise = typeUI.show({
                            containerNode: that.typeFieldsContainer,
                            data: that.initialData,
                            metadata: metadata,
                            category: that.category,
                            type: type
                        });
                        if (promise)
                        {
                            promise.then(function (instances)
                            {
                                util.applyToWidgets(that.typeFieldsContainer,
                                    that.category,
                                    type,
                                    that.initialData,
                                    metadata);
                                if (!that.isNew)
                                {
                                    util.disableWidgetsForImmutableFields(that.allFieldsContainer,
                                        that.category,
                                        type,
                                        metadata);
                                }
                            });
                        }
                    }
                    catch (e)
                    {
                        console.warn(e);
                    }
                });
            }
        },
        _configure: function (type)
        {
            if (!this.configured)
            {
                var metadata = this.management.metadata;
                util.applyToWidgets(this.allFieldsContainer, this.category, type, this.initialData, metadata);
                this.configured = true;
            }
        }
    };

    try
    {
        addLogInclusionRule.init();
    }
    catch (e)
    {
        console.warn(e);
    }
    return addLogInclusionRule;
});
