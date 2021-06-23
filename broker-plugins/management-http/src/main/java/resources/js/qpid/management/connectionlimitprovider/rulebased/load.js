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
define(["dojo/parser",
        "dojo/dom-construct",
        "dijit/registry",
        "dojo/_base/event",
        "qpid/common/util",
        "dojo/text!connectionlimitprovider/rulebased/load.html",
        "qpid/common/ResourceWidget",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/Form",
        "dojo/domReady!"],
    function (parser,
              construct,
              registry,
              event,
              util,
              template)
    {
        function Load()
        {
            const that = this;
            this.containerNode = construct.create("div", {innerHTML: template});

            parser.parse(this.containerNode)
                .then(function ()
                {
                    that._dialog = registry.byId("loadConnectionLimitProvider.ruleBased");
                    that._form = registry.byId("loadConnectionLimitProvider.form");

                    const submitButton = registry.byId("loadConnectionLimitProvider.submitButton");
                    submitButton.on("click", function (e)
                    {
                        that._submit(submitButton, e);
                    });

                    const cancelButton = registry.byId("loadConnectionLimitProvider.cancelButton");
                    cancelButton.on("click", function (e)
                    {
                        event.stop(e);
                        that._dialog.hide();
                    });

                    if (!window.FileReader)
                    {
                        const oldBrowserWarning = registry.byId("loadConnectionLimitProvider.oldBrowserWarning");
                        oldBrowserWarning.innerHTML = "File upload requires a more recent browser with HTML5 support";
                        oldBrowserWarning.className = oldBrowserWarning.className.replace("hidden", "");
                    }
                });
        }

        Load.prototype.show = function (management, modelObj)
        {
            this.management = management;
            this.modelObj = modelObj;
            this._form.reset();
            this._dialog.show();
        };

        Load.prototype._submit = function (submitButton, e)
        {
            event.stop(e);
            if (this._form.validate())
            {
                submitButton.set("disabled", true);
                const that = this;
                const modelObj = {
                    type: this.modelObj.type,
                    name: "loadFromFile",
                    parent: this.modelObj
                };
                const url = {url: this.management.buildObjectURL(modelObj)};
                const data = util.getFormWidgetValues(this._form, {});
                this.management.post(url, data)
                    .then(that._dialog.hide.bind(that._dialog), this.management.xhrErrorHandler)
                    .always(function ()
                    {
                        submitButton.set("disabled", false);
                    });
            }
            else
            {
                alert('Form contains invalid data. Please correct first');
            }
        };

        return new Load();
    });
