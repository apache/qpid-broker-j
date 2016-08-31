/*
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
        "dojo/text!dashboard/DashboardCreateDialogForm.html",
        "dojo/Evented",
        "dojo/store/Memory",
        "dojox/uuid/generateRandomUuid",
        "dijit/form/Form",
        "dijit/form/Button",
        "dijit/form/FilteringSelect",
        "dijit/form/ComboBox",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/Tooltip",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"], function (declare, lang, template, Evented, Memory, generateRandomUuid)
{

    return declare("qpid.management.dashboard.DashboardCreateDialogForm",
        [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
        {
            /**
             * dijit._TemplatedMixin enforced fields
             */
            //Strip out the apache comment header from the template html as comments unsupported.
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            structure: null,
            management: null,

            /**
             * template attach points
             */
            scope: null,
            okButton: null,
            cancelButton: null,
            createDashboardForm: null,

            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            initScope: function ()
            {
                var scopeItems = this.structure.getScopeItems();
                this._scopeModelObjects = scopeItems.scopeModelObjects;

                var scopeStore = new Memory({
                    data: scopeItems.items,
                    idProperty: 'id'
                });
                this.scope.set("store", scopeStore);
                this.scope.set("value", scopeItems.items[0].id);
                this._onChange();
            },
            _postCreate: function ()
            {
                this.initScope();
                this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                this.okButton.on("click", lang.hitch(this, this._onFormSubmit));
                this.scope.on("change", lang.hitch(this, this._onChange));
            },
            _onCancel: function (data)
            {
                this.emit("cancel");
            },
            _onChange: function (e)
            {
                var invalid = !this._scopeModelObjects[this.scope.value];
                this.okButton.set("disabled", invalid);
            },
            _onFormSubmit: function (e)
            {
                if (this.createDashboardForm.validate())
                {
                    var data = {
                        preference: {
                            id: generateRandomUuid(),
                            type: "X-Dashboard",
                            value: {widgets: {}, layout: {type: "singleColumn", column: []}}
                        },
                        parentObject: this._scopeModelObjects[this.scope.value]
                    };
                    this.emit("create", data);
                }
                else
                {
                    alert('Form contains invalid data.  Please correct first');
                }
                return false;
            }
        });

});
