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
        'dojo/_base/json',
        "dojo/query",
        'qpid/common/util',
        "dojo/text!addQueue.html",
        "qpid/common/ContextVariablesEditor",
        "dijit/form/NumberSpinner", // required by the form
    /* dojox/ validate resources */
        "dojox/validate/us",
        "dojox/validate/web",
    /* basic dijit classes */
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/Textarea",
        "dijit/form/FilteringSelect",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/DateTextBox",
        "dijit/form/TimeTextBox",
        "dijit/form/Button",
        "dijit/form/RadioButton",
        "dijit/form/Form",
        "dijit/form/DateTextBox",
    /* basic dojox classes */
        "dojox/form/BusyButton",
        "dojox/form/CheckedMultiSelect",
        "dojo/domReady!"], function (dom, construct, win, registry, parser, array, event, json, query, util, template)
{

    var addQueue = {};

    var node = construct.create("div", null, win.body(), "last");

    var requiredFields = {sorted: "sortKey"};

    var numericFieldNames = ["maximumMessageTtl",
                             "minimumMessageTtl",
                             "alertThresholdQueueDepthMessages",
                             "alertThresholdQueueDepthBytes",
                             "alertThresholdMessageAge",
                             "alertThresholdMessageSize",
                             "alertRepeatGap",
                             "maximumDeliveryAttempts"];

    var theForm;
    node.innerHTML = template;
    addQueue.dialogNode = dom.byId("addQueue");
    parser.instantiate([addQueue.dialogNode]);

    // for children which have name type, add a function to make all the associated atrributes
    // visible / invisible as the select is changed
    theForm = registry.byId("formAddQueue");
    var typeSelector = registry.byId("formAddQueue.type");
    typeSelector.on("change", function (value)
    {
        query(".typeSpecificDiv")
            .forEach(function (node, index, arr)
            {
                if (node.id === "formAddQueueType:" + value)
                {
                    node.style.display = "block";
                    if (addQueue.management)
                    {
                        util.applyMetadataToWidgets(node, "Queue", value, addQueue.management.metadata);
                    }
                }
                else
                {
                    node.style.display = "none";
                }
            });
        for (var requiredField in requiredFields)
        {
            dijit.byId('formAddQueue.' + requiredFields[requiredField]).required = (requiredField == value);
        }
    });

    theForm.on("submit", function (e)
    {

        event.stop(e);
        if (theForm.validate())
        {

            var newQueue = util.getFormWidgetValues(theForm);
            var context = addQueue.context.get("value");
            if (context)
            {
                newQueue["context"] = context;
            }

            addQueue.management.create("queue", addQueue.modelObj, newQueue)
                .then(function (x)
                {
                    registry.byId("addQueue")
                        .hide();
                });
            return false;

        }
        else
        {
            alert('Form contains invalid data.  Please correct first');
            return false;
        }

    });

    addQueue.show = function (management, modelObj)
    {
        addQueue.management = management;
        addQueue.modelObj = modelObj;

        var form = registry.byId("formAddQueue");
        form.reset();
        registry.byId("addQueue")
            .show();
        util.applyMetadataToWidgets(form.domNode, "Queue", "standard", addQueue.management.metadata);

        var overflowPolicyWidget = registry.byId("formAddQueue.overflowPolicy");
        var validValues = addQueue.management.metadata.getMetaData("Queue", "standard").attributes.overflowPolicy.validValues;
        var validValueStore = util.makeTypeStore(validValues);
        overflowPolicyWidget.set("store", validValueStore);

        // Add regexp to the numeric fields
        for (var i = 0; i < numericFieldNames.length; i++)
        {
            registry.byId("formAddQueue." + numericFieldNames[i])
                .set("regExpGen", util.numericOrContextVarRegexp);
        }

        registry.byId("formAddQueue.maximumQueueDepthBytes").set("regExpGen", util.signedOrContextVarRegexp);
        registry.byId("formAddQueue.maximumQueueDepthMessages").set("regExpGen", util.signedOrContextVarRegexp);

        if (!this.context)
        {
            this.context = new qpid.common.ContextVariablesEditor({
                name: 'context',
                title: 'Context variables'
            });
            this.context.placeAt(dom.byId("formAddQueue.context"));
        }

        util.loadEffectiveAndInheritedActualData(management, modelObj, function (data)
        {
            addQueue.context.setData(data.actual.context, data.effective.context, data.inheritedActual.context);
        });
    };

    return addQueue;
});
