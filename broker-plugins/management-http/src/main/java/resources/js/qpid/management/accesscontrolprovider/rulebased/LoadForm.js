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
        "dojo/text!accesscontrolprovider/rulebased/LoadForm.html",
        "qpid/common/ResourceWidget",
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
              template) {

        return declare("qpid.management.accesscontrolprovider.rulebased.LoadForm",
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
                path: null,
                loadDialog: null,
                loadForm: null,
                okButton: null,
                cancelButton: null,
                warning: null,

                postCreate: function () {
                    this.inherited(arguments);
                    this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                    this.okButton.on("click", lang.hitch(this, this._onFormSubmit));

                    var reader = window.FileReader ? new FileReader() : undefined;
                    if (!reader)
                    {
                        this.warning.innerHTML = "File upload requires a more recent browser with HTML5 support";
                        this.warning.className = this.warning.className.replace("hidden", "");
                    }
                    this.loadDialog.onHide = lang.hitch(this, function () {
                        this.emit("hide");
                    });
                },
                show: function()
                {
                    this.path.reset();
                    this.loadDialog.show();
                },
                hide: function()
                {
                    this.loadDialog.hide();
                },
                reset: function()
                {
                    this.path.reset();
                },
                _onCancel: function () {
                    this.emit("cancel");
                    this.hide();
                },
                _onFormSubmit: function () {
                    if (this.loadForm.validate())
                    {
                        var path = this.path.get("value");
                        this.emit("load", {path: path});
                    }
                    else
                    {
                        alert('Form contains invalid data.  Please correct first');
                    }

                    return false;
                }
            });
    });
