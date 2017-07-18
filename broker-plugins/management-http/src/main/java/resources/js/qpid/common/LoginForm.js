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
        "dojo/dom-class",
        "dojo/text!common/LoginForm.html",
        "dojo/Evented",
        "dojox/html/entities",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/TitlePane",
        "dijit/form/Form",
        "dijit/form/ValidationTextBox",
        "dijit/form/Button",
        "dojox/layout/TableContainer",
        "dojo/domReady!"],
    function (declare,
              lang,
              domClass,
              template,
              Evented,
              entities)
    {

        return declare("qpid.common.LoginForm",
                       [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {

            //Strip out the apache comment header from the template html as comments unsupported.
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            // template fields
            formWidget: null,
            usernameWidget: null,
            passwordWidget: null,
            loginButtonWidget: null,
            statusCodeNode: null,
            errorMessageNode: null,

            //

            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            _postCreate: function ()
            {
                this.formWidget.on("submit", lang.hitch(this, this._onFormSubmit));
                this.usernameWidget.on("change", lang.hitch(this, this._onCredentialChange));
                this.passwordWidget.on("change", lang.hitch(this, this._onCredentialChange));
            },
            show: function()
            {
                domClass.remove(this.domNode, "dijitHidden");
            },
            hide: function()
            {
                domClass.add(this.domNode, "dijitHidden");
            },
            onError: function (error)
            {
                if (error.response)
                {
                    this.statusCodeNode.innerHTML =  entities.encode(String(error.response.status));
                    if (error.response.status == 401)
                    {
                        this.errorMessageNode.innerHTML = "Authentication Failed";
                    }
                    else if (error.response.status == 403)
                    {
                        this.errorMessageNode.innerHTML ="Authorization Failed";
                    }
                    else
                    {
                        this.errorMessageNode.innerHTML =  entities.encode(String(error.message));
                    }
                }
                else
                {
                    var message = error.message ?  entities.encode(String(error.message)) : "Authentication failed";
                    this.statusCodeNode.innerHTML = "";
                    this.errorMessageNode.innerHTML = message;
                }
                this.usernameWidget.set("disabled", false);
                this.passwordWidget.set("disabled", false);
                this.formWidget.reset();
                this.usernameWidget.focus();
            },
            _onFormSubmit: function (event)
            {
                event.preventDefault();
                if (this.formWidget.validate())
                {
                    var data = {username: this.usernameWidget.value, password: this.passwordWidget.value};
                    this.emit("submit", data);

                    this.usernameWidget.set("disabled", true);
                    this.passwordWidget.set("disabled", true);
                    this.loginButtonWidget.set("disabled", true);
                }
                return false;
            },
            _onCredentialChange: function ()
            {
                this.loginButtonWidget.set("disabled", !this.usernameWidget.value || !this.passwordWidget.value);
            }

        });
    });
