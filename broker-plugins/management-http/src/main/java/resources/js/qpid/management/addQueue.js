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
        'dojo/_base/json',
        "dojo/query",
        'qpid/common/util',
        "dojo/text!addQueue.html",
        "qpid/common/DestinationChooser",
        "qpid/common/ContextVariablesEditor",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/FilteringSelect",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dojo/domReady!"],
    function (dom, construct, win, registry, parser, lang, array, event, json, query, util, template)
    {
        var hideDialog = function ()
        {
            registry.byId("addQueue")
                .hide();
        };

        var requiredFields = {sorted: "sortKey"};

        var numericFieldNames = ["maximumMessageTtl",
                                 "minimumMessageTtl",
                                 "alertThresholdQueueDepthMessages",
                                 "alertThresholdQueueDepthBytes",
                                 "alertThresholdMessageAge",
                                 "alertThresholdMessageSize",
                                 "alertRepeatGap",
                                 "maximumDeliveryAttempts"];

        var addQueue = {
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
                this.alternateBinding = registry.byId("formAddQueue.alternateBinding");
                this.form = registry.byId("formAddQueue");

                for (var i = 0; i < numericFieldNames.length; i++)
                {
                    registry.byId("formAddQueue." + numericFieldNames[i])
                        .set("regExpGen", util.numericOrContextVarRegexp);
                }

                registry.byId("formAddQueue.maximumQueueDepthBytes")
                    .set("regExpGen", util.signedOrContextVarRegexp);
                registry.byId("formAddQueue.maximumQueueDepthMessages")
                    .set("regExpGen", util.signedOrContextVarRegexp);

                this.queueName = registry.byId("formAddQueue.name");
                this.queueName.set("regExpGen", util.nameOrContextVarRegexp);
                this.queueDurable = registry.byId("formAddQueue.durable");
                this.queueType = registry.byId("formAddQueue.type");
                this.context = registry.byId("formAddQueue.context");
                this.overflowPolicyWidget = registry.byId("formAddQueue.overflowPolicy");
                this.messageGroupTypeWidget = registry.byId("formAddQueue.messageGroupType");
                this.exclusivityWidget = registry.byId("formAddQueue.exclusive");
                this.editNodeBanner = dom.byId("addQueue.editNoteBanner");


                registry.byId("formAddQueue.cancelButton")
                    .on("click", function (e)
                    {
                        event.stop(e);
                        hideDialog();
                    });

                registry.byId("formAddQueue.saveButton")
                    .on("click", lang.hitch(this, function (e)
                    {
                        this._submit(e);
                    }));

                registry.byId("formAddQueue.type")
                    .on("change", function (value)
                    {
                        query(".typeSpecificDiv")
                            .forEach(function (node, index, arr)
                            {
                                if (node.id === "formAddQueueType:" + value)
                                {
                                    node.style.display = "block";
                                    if (addQueue.management)
                                    {
                                        util.applyToWidgets(node,
                                            "Queue",
                                            value,
                                            addQueue.initialData,
                                            addQueue.management.metadata,
                                            addQueue.effectiveData
                                        );
                                    }
                                }
                                else
                                {
                                    node.style.display = "none";
                                }
                            });
                        for (var requiredField in requiredFields)
                        {
                            dijit.byId('formAddQueue.' + requiredFields[requiredField]).required =
                                (requiredField == value);
                        }
                    });
            },

            _submit: function (e)
            {
                event.stop(e);
                if (this.form.validate())
                {
                    var queueData = util.getFormWidgetValues(this.form, this.initialData);
                    var context = this.context.get("value");
                    if (context)
                    {
                        queueData["context"] = context;
                    }
                    queueData.alternateBinding = this.alternateBinding.valueAsJson();

                    if (this.initialData && this.initialData.id)
                    {
                        this.management.update(this.modelObj, queueData)
                            .then(hideDialog);
                    }
                    else
                    {
                        this.management.create("queue", this.modelObj, queueData)
                            .then(hideDialog);
                    }
                    return false;
                }
                else
                {
                    alert('Form contains invalid data.  Please correct first');
                    return false;
                }
            },

            show: function (management, modelObj, effectiveData)
            {
                this.management = management;
                this.modelObj = modelObj;

                this.alternateBindingLoadPromise =
                    this.alternateBinding.loadData(management, effectiveData ? modelObj.parent : modelObj);
                this.form.reset();

                if (effectiveData)
                {
                    var afterLoad = lang.hitch(this, function (data)
                    {
                        var actualData = data.actual;
                        var effectiveData = data.effective;
                        this.initialData = actualData;
                        this.effectiveData = effectiveData;
                        this.queueType.set("value", actualData.type);
                        this.queueType.set("disabled", true);
                        this.queueName.set("disabled", true);
                        this.queueDurable.set("disabled", true);
                        this.queueName.set("value", actualData.name);
                        this.context.setData(actualData.context, effectiveData.context, data.inheritedActual.context);
                        this.editNodeBanner.style.display = "block";
                        this._show();
                    });
                    util.loadData(management, modelObj, afterLoad, {depth: 1});
                }
                else
                {
                    this.editNodeBanner.style.display = "none";
                    this.queueType.set("disabled", false);
                    this.queueName.set("disabled", false);
                    this.queueDurable.set("disabled", false);
                    this.initialData = {"type": "standard"};
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
                    var validOverflowValues = this.management.metadata.getMetaData("Queue",
                        this.initialData.type).attributes.overflowPolicy.validValues;
                    var validOverflowValueStore = util.makeTypeStore(validOverflowValues);
                    this.overflowPolicyWidget.set("store", validOverflowValueStore);

                    var validGroupingValues = this.management.metadata.getMetaData("Queue",
                        this.initialData.type).attributes.messageGroupType.validValues;
                    var validGroupingValueStore = util.makeTypeStore(validGroupingValues);
                    this.messageGroupTypeWidget.set("store", validGroupingValueStore);

                    var exclusivityOptions = this.management.metadata.getMetaData("Queue",
                        this.initialData.type).attributes.exclusive.validValues;
                    var exclusivityOptionStore = util.makeTypeStore(exclusivityOptions);
                    this.exclusivityWidget.set("store", exclusivityOptionStore);
                    util.applyToWidgets(this.form.domNode,
                        "Queue",
                        this.initialData.type,
                        this.initialData,
                        this.management.metadata,
                        this.effectiveData
                    );

                    var alternate = this.initialData.alternateBinding;
                    if (alternate && alternate.destination)
                    {
                        this.alternateBinding.set("value", alternate.destination);
                    }

                    registry.byId("addQueue").show();
                }));
            }
        };

        addQueue._init();
        return addQueue;
    });
