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
        "dojo/_base/array",
        "dojo/json",
        "dojo/dom-construct",
        "dojo/text!query/MessageDialogForm.html",
        "dojo/Evented",
        "dojox/html/entities",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/CheckBox",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dojo/domReady!"], function (declare, lang, array, json, domConstruct, template, Evented, entities)
{

    var MessageDialogForm = declare("qpid.management.query.MessageDialogForm",
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
            messageNode: null,
            messagePanel: null,
            stopDisplaying: null,
            okButton: null,
            cancelButton: null,

            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            _postCreate: function ()
            {
                if (this.message)
                {
                    this.messageNode.innerHTML = this.message;
                }
                this.okButton.on("click", lang.hitch(this, this._onOk));
                this.cancelButton.on("click", lang.hitch(this, this._onCancel));
            },
            _onOk: function ()
            {
                this.emit("execute", this.stopDisplaying.checked);
            },
            _onCancel: function (data)
            {
                this.emit("cancel");
            }
        });

    return declare("qpid.management.query.MessageDialog", [dijit.Dialog, Evented], {
        postCreate: function ()
        {
            this.inherited(arguments);
            this._postCreate();
        },
        _postCreate: function ()
        {
            var options = {};
            if (this.message)
            {
                options.message = this.message;
            }
            var contentForm = new MessageDialogForm(options);
            this.set("content", contentForm);
            contentForm.on("execute", lang.hitch(this, this._onExecute));
            contentForm.on("cancel", lang.hitch(this, this._onCancel))
        },
        _onExecute: function (stopDisplaying)
        {
            this.hide();
            this.emit("execute", stopDisplaying);
        },
        _onCancel: function (data)
        {
            this.hide();
            this.emit("cancel");
        }
    });
});
