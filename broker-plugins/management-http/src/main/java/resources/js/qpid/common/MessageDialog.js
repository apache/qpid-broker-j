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
        "dojo/_base/array",
        "dojo/json",
        "dojo/dom-construct",
        "dojo/text!common/MessageDialogForm.html",
        "dojo/Evented",
        "dojo/Deferred",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/CheckBox",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dojo/domReady!"], function (declare, lang, array, json, domConstruct, template, Evented, Deferred)
{

    var MessageDialogForm = declare("qpid.common.MessageDialogForm",
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

    var MessageDialog = declare("qpid.common.MessageDialog", [dijit.Dialog, Evented], {
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
            contentForm.on("cancel", lang.hitch(this, this._onCancel));
            this.contentForm = contentForm;
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

    var confirmationState = {
        stopDisplaying: {},
        confirmationDialog: null,
        confirmations: []
    };

    var requestConfirmationAsPromise = function (kwargs)
    {
        if (!kwargs.title && !kwargs.message)
        {
            throw new Error("Confirmation title or/and message are not specified!");
        }

        if (confirmationState.confirmationDialog === null)
        {
            confirmationState.confirmationDialog = new MessageDialog({
                title: kwargs.title,
                message: kwargs.message,
                onHide: function ()
                {
                    var deferred = confirmationState.confirmations.shift();
                    if (deferred && !deferred.isFulfilled())
                    {
                        deferred.cancel();
                    }
                }
            });
        }
        else
        {
            confirmationState.confirmationDialog.set("title", kwargs.title);
            confirmationState.confirmationDialog.contentForm.messageNode.innerHTML = kwargs.message;
            confirmationState.confirmationDialog.contentForm.stopDisplaying.set("checked", false);
        }

        var deferred = new Deferred();
        var confirmationHandler = null;
        if (!kwargs.confirmationId || !confirmationState.stopDisplaying[kwargs.confirmationId])
        {
            var displayForFlag = kwargs.confirmationId ? "block" : "none";
            confirmationState.confirmationDialog.contentForm.stopDisplayingNode.style.display = displayForFlag;
            confirmationHandler = confirmationState.confirmationDialog.on("execute",
                function (stopDisplaying)
                {
                    if (kwargs.confirmationId)
                    {
                        confirmationState.stopDisplaying[kwargs.confirmationId] = stopDisplaying;
                    }
                    confirmationHandler.remove();
                    deferred.resolve();
                });

            deferred.promise.otherwise(function ()
            {
                confirmationHandler.remove();
            });

            confirmationState.confirmations.push(deferred);
            confirmationState.confirmationDialog.show();
        }
        else
        {
            deferred.resolve()
        }

        return deferred.promise;
    };

    /**
     * Displays a confirmation dialog if the following conditions are met:
     *   confirmationId is not provided as part of kwargs
     *   confirmationId is provided as part of kwargs and user previously did not check 'stop displaying'
     *                  as part of confirmation with the same confirmationId
     * Promise is returned by the method.
     * It is resolved if the following conditions are met:
     *   Ok is pressed by the user
     *   User selected 'stop displaying' as part of previous confirmation with the same confirmationId
     * Otherwise promise is cancelled.
     * @param kwargs
     * @returns promise which is resolved only when user confirms or selected 'stop displaying'
     *                  for previously confirmation having the same id.
     */
    MessageDialog.confirm = function (kwargs)
    {
        return requestConfirmationAsPromise(kwargs);
    };

    return MessageDialog;
});
