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
        "dojo/dom-style",
        "dijit/registry",
        "dojo/parser",
        "dojo/store/Memory",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/json',
        "qpid/common/util",
        "dojo/text!addLogger.html",
        "qpid/common/ContextVariablesEditor",
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
        "dojo/domReady!"],
    function (lang, dom, construct, domStyle, registry, parser, memory, array, event, json, util, template)
    {
        var addLogger = {
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
                this.name = registry.byId("addLogger.name");
                this.name.set("regExpGen", util.nameOrContextVarRegexp);

                this.dialog = registry.byId("addLogger");
                this.addButton = registry.byId("addLogger.addButton");
                this.cancelButton = registry.byId("addLogger.cancelButton");
                this.cancelButton.on("click", function (e)
                {
                    that._cancel(e);
                });
                this.addButton.on("click", function (e)
                {
                    that._add(e);
                });

                this.typeFieldsContainer = dom.byId("addLogger.typeFields");
                this.form = registry.byId("addLogger.form");
                this.form.on("submit", function ()
                {
                    return false;
                });

                this.loggerType = registry.byId("addLogger.type");
                this.loggerType.on("change", function (type)
                {
                    that._typeChanged(type);
                });

                this.durable = registry.byId("addLogger.durable");

                this.categoryFieldsContainer = dom.byId("addLogger.categoryFields");
                this.allFieldsContainer = dom.byId("addLogger.contentPane");
                this.context = registry.byId("addLogger.context");
            },
            show: function (management, modelObj, category, actualData)
            {
                this.management = management;
                this.modelObj = modelObj;
                this.category = category;
                this.configured = false;
                this._destroyTypeFields(this.typeFieldsContainer);
                this._destroyTypeFields(this.categoryFieldsContainer);
                this.form.reset();
                this.loggerType.set("store", util.makeTypeStoreFromMetadataByCategory(management.metadata, category));
                this.initialData = actualData;
                this.isNew = !actualData;
                this.name.set("disabled", !this.isNew);
                this.loggerType.set("disabled", !this.isNew);
                this.durable.set("disabled", !this.isNew);
                this.dialog.set("title", this.isNew ? "Add Logger" : "Edit Logger - " + actualData.name);

                if (actualData)
                {
                    this._configure(actualData.type);
                }

                var brokerLoggerEditWarningNode = dom.byId("brokerLoggerEditWarning");
                var virtualHostlLoggerEditWarningNode = dom.byId("virtualHostlLoggerEditWarning");
                domStyle.set(brokerLoggerEditWarningNode,
                    "display",
                    !this.isNew && this.category === "BrokerLogger" ? "block" : "none");
                domStyle.set(virtualHostlLoggerEditWarningNode,
                    "display",
                    !this.isNew && this.category === "VirtualHostLogger" ? "block" : "none");

                util.loadEffectiveAndInheritedActualData(this.management, this.modelObj, lang.hitch(this, function(data)
                {
                    this.context.setData(this.isNew ? {} : this.initialData.context ,
                        data.effective.context,
                        data.inheritedActual.context);
                    this._loadCategoryUserInterfacesAndShowDialog(actualData);
                }));
            },
            hide: function ()
            {
                this._destroyTypeFields(this.categoryFieldsContainer);
                this._destroyTypeFields(this.typeFieldsContainer);
                this.dialog.hide();
            },
            _cancel: function (e)
            {
                event.stop(e);
                this.hide();
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
                    var excludedData = this.initialData
                                       || this.management.metadata.getDefaultValueForType(this.category,
                            this.loggerType.get("value"));
                    var formData = util.getFormWidgetValues(this.form, excludedData);
                    var context = this.context.get("value");
                    var oldContext = null;
                    if (this.initialData !== null && this.initialData !== undefined)
                    {
                        oldContext = this.initialData.context;
                    }
                    if (context && !util.equals(context, oldContext))
                    {
                        formData["context"] = context;
                    }
                    var that = this;
                    if (this.isNew)
                    {
                        this.management.create(this.category, this.modelObj, formData)
                            .then(function ()
                            {
                                that.hide();
                            });
                    }
                    else
                    {
                        this.management.update(this.modelObj, formData)
                            .then(function ()
                            {
                                that.hide();
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
                    require(["qpid/management/logger/" + this.category.toLowerCase() + "/" + type.toLowerCase()
                             + "/add"], function (typeUI)
                    {
                        try
                        {
                            var promise = typeUI.show({
                                containerNode: that.typeFieldsContainer,
                                data: that.initialData,
                                metadata: that.management.metadata,
                                category: that.category,
                                type: type,
                                context: that.context
                            });
                            if (promise)
                            {
                                promise.then(function (instances)
                                {
                                    util.applyToWidgets(that.typeFieldsContainer,
                                        that.category,
                                        type,
                                        that.initialData,
                                        that.management.metadata);
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
            },
            _loadCategoryUserInterfacesAndShowDialog: function (actualData)
            {
                var that = this;
                var node = construct.create("div", {}, this.categoryFieldsContainer);
                require(["qpid/management/logger/" + this.category.toLowerCase() + "/add"], function (categoryUI)
                {
                    try
                    {
                        var promise = categoryUI.show({
                            containerNode: node,
                            data: actualData
                        });
                        if (actualData)
                        {
                            promise.then(function (instances)
                            {
                                util.applyToWidgets(node,
                                    that.category,
                                    actualData.type,
                                    actualData,
                                    that.management.metadata);
                                that.dialog.show();
                            });
                        }
                        else
                        {
                            that.dialog.show();
                        }
                    }
                    catch (e)
                    {
                        console.error(e);
                    }
                });
            }
        };

        try
        {
            addLogger.init();
        }
        catch (e)
        {
            console.warn(e);
        }
        return addLogger;
    });
