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
        "dojo/dom-construct",
        "dojo/dom-class",
        "dojo/on",
        "dijit/form/ValidationTextBox",
        "dojo/domReady!"],
    function (declare, lang, domConstruct, domClass, on, ValidationTextBox)
    {
        return declare("qpid.management.query.SearchTextBox",
            [ValidationTextBox],
            {
                clearText: "Clear",
                postCreate: function ()
                {
                    this.inherited(arguments);
                    domClass.add(this.domNode, "searchBox");
                    this.searchNode = domConstruct.create("span", {
                        className: "search ui-icon"
                    }, this.domNode);

                    this.clearNode = domConstruct.create("a", {
                        className: "clear ui-icon",
                        innerHTML: this.clearText,
                        title: this.clearText
                    }, this.domNode);

                    on(this.clearNode, "click", lang.hitch(this, function ()
                    {
                        this.set("value", "");
                        this.focus();
                    }));

                }
            });
    });
