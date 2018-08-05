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
        "dojo/_base/lang",
        "dojo/_base/array",
        "dojo/_base/event",
        "dojo/_base/json",
        "qpid/common/util",
        "dojo/text!addExchange.html",
        "qpid/common/DestinationChooser",
        "qpid/common/ContextVariablesEditor",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/TitlePane",
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/FilteringSelect",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dojo/domReady!"], function (dom, construct, win, registry, parser, lang, array, event, json, util, template)
{
    var hideDialog = function ()
    {
        registry.byId("addExchange").hide();
    };

    var addExchange = {
        _init: function ()
        {
            var node = construct.create("div", {innerHTML: template});
            parser.parse(node)
                .then(lang.hitch(this, function (instances)
                {
                    this._postParse();
                }));
        },
        _postParse: function ()
        {
            this.alternateBinding = registry.byId("formAddExchange.alternateBinding");
            this.form = registry.byId("formAddExchange");
            this.exchangeName = registry.byId("formAddExchange.name");
            this.exchangeDurable = registry.byId("formAddExchange.durable");
            this.exchangeName.set("regExpGen", util.nameOrContextVarRegexp);
            this.exchangeType = registry.byId("formAddExchange.type");
            this.context = registry.byId("formAddExchange.context");
            this.unroutableMessageBehaviour = registry.byId("formAddExchange.unroutableMessageBehaviour");

            registry.byId("formAddExchange.cancelButton")
                .on("click", function (e)
                {
                    event.stop(e);
                    hideDialog();
                });

            registry.byId("formAddExchange.saveButton")
                .on("click", function (e)
                {
                    addExchange._submit(e);
                });

            array.forEach(this.form.getDescendants(), function (widget)
            {
                if (widget.name === "type")
                {
                    widget.on("change", function (isChecked)
                    {

                        var obj = registry.byId(widget.id + ":fields");
                        if (obj)
                        {
                            if (isChecked)
                            {
                                obj.domNode.style.display = "block";
                                obj.resize();
                            }
                            else
                            {
                                obj.domNode.style.display = "none";
                                obj.resize();
                            }
                        }
                    })
                }

            });
        },

        show: function (management, modelObj, effectiveData)
        {
            this.management = management;
            this.modelObj = modelObj;

            this.alternateBindingLoadPromise =
                this.alternateBinding.loadData(management, effectiveData ? modelObj.parent : modelObj);
            this.form.reset();

            var validUnroutableMessageBehaviourValues = this.management.metadata.getMetaData("Exchange","direct").attributes.unroutableMessageBehaviour.validValues;
            var validUnroutableMessageBehaviourStore = util.makeTypeStore(validUnroutableMessageBehaviourValues);
            this.unroutableMessageBehaviour.set("store", validUnroutableMessageBehaviourStore);
            if (effectiveData)
            {
                this.effectiveData = effectiveData;
                var afterLoad = lang.hitch(this, function (data)
                {
                    var actualData = data.actual;
                    var effectiveData = data.effective;
                    this.initialData = actualData;
                    this.effectiveData = effectiveData;
                    this.exchangeType.set("value", actualData.type);
                    this.exchangeType.set("disabled", true);
                    this.exchangeName.set("disabled", true);
                    this.exchangeDurable.set("disabled", true);
                    this.exchangeName.set("value", actualData.name);
                    this.context.setData(actualData.context, effectiveData.context, data.inheritedActual.context);
                    this._show();
                });
                util.loadData(management, modelObj, afterLoad, {depth: 1});
            }
            else
            {
                this.exchangeType.set("disabled", false);
                this.exchangeName.set("disabled", false);
                this.exchangeDurable.set("disabled", false);
                this.initialData = {};
                this.effectiveData = {};
                util.loadEffectiveAndInheritedActualData(management, modelObj, lang.hitch(this, function (data)
                {
                    this.context.setData(data.actual.context, data.effective.context, data.inheritedActual.context);
                    this._show();
                }), {depth: 1});
            }
        },

        _show: function ()
        {
            this.alternateBindingLoadPromise.then(lang.hitch(this, function ()
            {
                util.applyToWidgets(this.form.domNode,
                    "Exchange",
                    this.initialData.type || "direct",
                    this.initialData,
                    this.management.metadata,
                    this.effectiveData);

                var alternate = this.initialData.alternateBinding;
                if (alternate && alternate.destination)
                {
                    this.alternateBinding.set("value", alternate.destination);
                }

                if (this.initialData && this.initialData.unroutableMessageBehaviour)
                {
                    this.unroutableMessageBehaviour.set("value", this.initialData.unroutableMessageBehaviour);
                }
                else
                {
                    this.unroutableMessageBehaviour.set("value", null);
                }
                this.unroutableMessageBehaviour.set("required", false);
                registry.byId("addExchange").show();
            }));
        },

         _submit : function (e)
        {
            event.stop(e);
            if (this.form.validate())
            {
                var exchangeData = util.getFormWidgetValues(this.form, this.initialData);
                var context = this.context.get("value");
                if (context)
                {
                    exchangeData["context"] = context;
                }
                exchangeData.alternateBinding = this.alternateBinding.valueAsJson();

                if (this.initialData && this.initialData.id)
                {
                    this.management.update(this.modelObj, exchangeData) .then(hideDialog);
                }
                else
                {
                    this.management.create("exchange", this.modelObj, exchangeData) .then(hideDialog);
                }
            }
            else
            {
                alert('Form contains invalid data.  Please correct first');
            }
        }
    };
    addExchange._init();
    return addExchange;
});