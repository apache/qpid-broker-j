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
        "dojo/dom-construct",
        "dojo/_base/window",
        "dijit/registry",
        "dojo/parser",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/json',
        "dojo/store/Memory",
        "dijit/form/FilteringSelect",
        "dojo/_base/connect",
        "dojo/dom-style",
        "qpid/common/util",
        "dojo/text!addAuthenticationProvider.html",
        /* dojox/ validate resources */
        "dojox/validate/us",
        "dojox/validate/web",
        /* basic dijit classes */
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/Textarea",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        /* basic dojox classes */
        "dojox/form/BusyButton",
        "dojox/form/CheckedMultiSelect",
        "dojox/layout/TableContainer",
        "qpid/common/ContextVariablesEditor",
        "dojo/domReady!"],
    function (dom,
              construct,
              win,
              registry,
              parser,
              array,
              event,
              json,
              Memory,
              FilteringSelect,
              connect,
              domStyle,
              util,
              template)
    {
        var addAuthenticationProvider = {
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
                this.authenticationProviderName = registry.byId("addAuthenticationProvider.name");
                this.authenticationProviderName.set("regExpGen", util.nameOrContextVarRegexp);

                this.dialog = registry.byId("addAuthenticationProvider");
                this.addButton = registry.byId("addAuthenticationProvider.addButton");
                this.cancelButton = registry.byId("addAuthenticationProvider.cancelButton");
                this.cancelButton.on("click", function (e)
                {
                    that._cancel(e);
                });
                this.addButton.on("click", function (e)
                {
                    that._add(e);
                });

                this.authenticationProviderTypeFieldsContainer = dom.byId("addAuthenticationProvider.typeFields");
                this.authenticationProviderForm = registry.byId("addAuthenticationProvider.form");
                this.authenticationProviderType = registry.byId("addAuthenticationProvider.type");
                this.authenticationProviderType.on("change", function (type)
                {
                    that._authenticationProviderTypeChanged(type);
                });

                this.context = registry.byId("addAuthenticationProvider.context");
            },
            show: function (management, modelObj, effectiveData)
            {
                this.management = management;
                this.modelObj = modelObj;
                this.authenticationProviderForm.reset();

                this.supportedAuthenticationProviderTypes =
                    management.metadata.getTypesForCategory("AuthenticationProvider");
                this.supportedAuthenticationProviderTypes.sort();
                var authenticationProviderTypeStore = util.makeTypeStore(this.supportedAuthenticationProviderTypes);
                this.authenticationProviderType.set("store", authenticationProviderTypeStore);

                var that = this;
                if (effectiveData)
                {
                    var contextCallback = function (data)
                    {
                        var actualData = data.actual;
                        var effectiveData = data.effective;
                        that.initialData = actualData;
                        that.effectiveData = effectiveData;
                        that.authenticationProviderType.set("value", actualData.type);

                        that.authenticationProviderType.set("disabled", true);
                        that.authenticationProviderName.set("disabled", true);
                        that.authenticationProviderName.set("value", actualData.name);
                        that.context.setData(actualData.context, effectiveData.context, data.inheritedActual.context);
                        that._show();
                    };
                    util.loadData(management, modelObj, contextCallback, {depth: 1});
                }
                else
                {
                    this.authenticationProviderType.set("disabled", false);
                    this.authenticationProviderName.set("disabled", false);
                    this.initialData = {};
                    this.effectiveData = {};
                    util.loadEffectiveAndInheritedActualData(management, modelObj, function (data)
                    {
                        that.context.setData(data.actual.context, data.effective.context, data.inheritedActual.context);
                        that._show();
                    }, {depth: 1});
                }
            },
            _show: function ()
            {
                this.dialog.show();
                if (!this.resizeEventRegistered)
                {
                    this.resizeEventRegistered = true;
                    util.resizeContentAreaAndRepositionDialog(dom.byId("addAuthenticationProvider.contentPane"),
                        this.dialog);
                }
            },
            _cancel: function (e)
            {
                event.stop(e);
                this.dialog.hide();
            },
            _add: function (e)
            {
                event.stop(e);
                this._submit();
            },
            _submit: function ()
            {
                if (this.authenticationProviderForm.validate())
                {
                    var authenticationProviderData = util.getFormWidgetValues(this.authenticationProviderForm,
                        this.initialData);
                    if (this.typeUI && this.typeUI._preSubmit)
                    {
                        this.typeUI._preSubmit(authenticationProviderData);
                    }
                    var context = this.context.get("value");
                    if (context && (!this.initialData || !util.equals(context, this.initialData.context)))
                    {
                        authenticationProviderData["context"] = context;
                    }
                    var that = this;

                    var hideDialog = function (x)
                    {
                        that.dialog.hide();
                    };

                    if (this.initialData && this.initialData.id)
                    {
                        // update request
                        this.management.update(that.modelObj, authenticationProviderData)
                            .then(hideDialog);
                    }
                    else
                    {
                        this.management.create("authenticationprovider", that.modelObj, authenticationProviderData)
                            .then(hideDialog);
                    }
                }
                else
                {
                    alert('Form contains invalid data. Please correct first');
                }
            },
            _authenticationProviderTypeChanged: function (type)
            {
                this._typeChanged(type,
                    this.authenticationProviderTypeFieldsContainer,
                    "qpid/management/authenticationprovider/",
                    "AuthenticationProvider");
            },
            _typeChanged: function (type, typeFieldsContainer, baseUrl, category)
            {
                var widgets = registry.findWidgets(typeFieldsContainer);
                array.forEach(widgets, function (item)
                {
                    item.destroyRecursive();
                });
                construct.empty(typeFieldsContainer);
                if (type)
                {
                    var that = this;
                    require([baseUrl + type.toLowerCase() + "/add"], function (typeUI)
                    {
                        try
                        {
                            typeUI.show({
                                containerNode: typeFieldsContainer,
                                parent: that,
                                category: category,
                                type: type,
                                data: that.initialData,
                                effectiveData: that.effectiveData,
                                metadata: that.management.metadata
                            });
                            that.typeUI = typeUI;
                        }
                        catch (e)
                        {
                            console.warn(e);
                        }
                    });
                }
            }
        };

        try
        {
            addAuthenticationProvider.init();
        }
        catch (e)
        {
            console.warn(e);
        }
        return addAuthenticationProvider;
    });
