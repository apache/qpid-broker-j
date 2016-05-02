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
        "dojo/_base/array",
        "dojo/_base/lang",
        "dojo/_base/event",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dojo/text!common/warning.html",
        "dojox/html/entities",
        "dijit/form/Button",
        "dojo/domReady!"],
    function (declare, array, lang, event, _WidgetBase, _TemplatedMixin, _WidgetsInTemplateMixin, template, entities)
    {

        return declare("qpid.common.WarningPane", [_WidgetBase, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            message: "Not Found",

            buildRendering: function ()
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                this.templateString = this.templateString.replace(/<!--[\s\S]*?-->/g, "");
                this.inherited(arguments);
            },
            postCreate: function ()
            {
                this.inherited(arguments);
                this._renderMessage();
                var that = this;
                this.closeButton.on("click", function (e)
                {
                    that._onButtonClick(e)
                });
            },
            _onButtonClick: function (/*Event*/ e)
            {
                this.onClick(e);
            },
            onClick: function (/*Event*/ e)
            {
                // extention point
            },
            _setMessageAttr: function (message)
            {
                this.message = message;
                this._renderMessage();
            },
            _renderMessage: function ()
            {
                this.warningMessage.innerHTML = entities.encode(String(this.message));
            }
        });
    });
