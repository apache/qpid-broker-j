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
define(["dojo/_base/event",
        "dojo/dom-construct",
        "dojo/parser",
        "dijit/registry",
        "qpid/management/preferencesprovider/PreferencesProviderForm",
        "qpid/common/util",
        "dojo/text!addPreferencesProvider.html",
        "dojox/html/entities",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (event, construct, parser, registry, PreferencesProviderForm, util, template, entities)
    {

        var addPreferencesProvider = {
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
                this.preferencesProviderForm = registry.byId("addPreferencesProvider.preferencesProvider");
                this.dialog = registry.byId("addPreferencesProvider");

                var cancelButton = registry.byId("addPreferencesProvider.cancelButton");
                cancelButton.on("click", function ()
                {
                    that.dialog.hide();
                });

                var saveButton = registry.byId("addPreferencesProvider.saveButton");
                saveButton.on("click", function ()
                {
                    var result = that.preferencesProviderForm.submit(function (preferencesProviderData)
                    {
                        if (that.preferencesProviderForm.data)
                        {
                            // update request
                            that.management.update(that.modelObj, preferencesProviderData)
                                .then(function ()
                                {
                                    that.dialog.hide();
                                });
                        }
                        else
                        {
                            that.management.create("preferencesprovider", that.modelObj, preferencesProviderData)
                                .then(function ()
                                {
                                    that.dialog.hide();
                                });
                        }
                    });
                });
            },
            show: function (management, modelObj)
            {
                this.management = management;
                this.modelObj = modelObj;
                this.preferencesProviderForm.setMetadata(management.metadata)
                this.authenticationProviderName =
                    modelObj.type == "authenticationprovider" ? modelObj.name : modelObj.parent.name;
                this.dialog.set("title",
                    modelObj.type == "preferencesprovider"
                        ? "Edit preferences provider '" + entities.encode(String(modelObj.name)) + "' for '"
                          + entities.encode(String(modelObj.parent.name)) + "'"
                        : "Add preferences provider " + " for '" + entities.encode(String(modelObj.name)));
                if (modelObj.type == "preferencesprovider")
                {
                    var that = this;
                    management.load(modelObj, {actuals: true})
                        .then(function (data)
                        {
                            that.preferencesProviderForm.setData(data[0]);
                            that.dialog.show();
                        }, util.xhrErrorHandler);
                }
                else
                {
                    this.preferencesProviderForm.reset();
                    this.dialog.show();
                }
            }
        };

        try
        {
            addPreferencesProvider.init();
        }
        catch (e)
        {
            console.warn("Initialisation of add preferences dialog failed", e);
        }

        return addPreferencesProvider;
    });
