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
        "dojo/json",
        "dojo/Evented",
        "dojo/text!preference/PreferenceSaveDialogContent.html",
        "qpid/common/util",
        "dojox/html/entities",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/form/ValidationTextBox",
        "dijit/form/SimpleTextarea",
        "qpid/management/query/OptionsPanel"
],
    function (declare,
              lang,
              json,
              Evented,
              template,
              util,
              entities)
    {
        return declare([dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                /**
                 * dijit._TemplatedMixin enforced fields
                 */
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                /**
                 * template attach points
                 */
                name: null,
                description: null,
                groupChooser: null,
                saveButton: null,
                cancelButton: null,
                form: null,

                /**
                 * constructor mixin
                 */
                management : null,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this._postCreate();
                },
                startup: function ()
                {
                    this.inherited(arguments);
                    this.groupChooser.startup();
                },
                _setPreferenceAttr: function (preference)
                {
                    this.preference = lang.clone(preference);
                    this.name.set("value", this.preference.name);
                    this.description.set("value", this.preference.description);

                    var userGroups = this.management.getGroups();
                    var selected = this.preference.visibilityList || [];
                    for (var i = selected.length - 1; i >= 0; i--)
                    {
                        var present = false;
                        for (var j = 0; j < userGroups.length; j++)
                        {
                            if (selected[i] === userGroups[j])
                            {
                                present = true;
                                break;
                            }
                        }
                        if (!present)
                        {
                            selected.splice(i, 1);
                        }
                    }
                    var items = [];
                    for (var j = 0; j < userGroups.length; j++)
                    {
                        items[j] = {
                            id: userGroups[j],
                            name: util.toFriendlyUserName(userGroups[j])
                        };
                    }
                    this.groupChooser.set("data",
                        {
                            items: items,
                            selected: selected
                        });
                    this._onChange();
                },
                _postCreate: function ()
                {
                    this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                    this.name.on("change", lang.hitch(this, this._onChange));
                    this.form.on("submit", lang.hitch(this, this._onFormSubmit));
                    this.groupChooser.set("renderItem",  this._renderGroup);
                },
                _onCancel: function (data)
                {
                    this.emit("cancel");
                },
                _onChange: function (e)
                {
                    var invalid = !this.form.validate();
                    this.saveButton.set("disabled", invalid);
                },
                _onFormSubmit: function (e)
                {
                    try
                    {
                        if (this.form.validate())
                        {
                            var preference = this.preference;
                            preference.name = this.name.get("value");
                            preference.description = this.description.get("value");
                            var groups = [];
                            var selected = this.groupChooser.get("selectedItems");
                            for (var i = 0; i < selected.length; i++)
                            {
                                groups.push(selected[i].id);
                            }
                            preference.visibilityList = groups;
                            this.emit("save", {preference: preference});
                        }
                        else
                        {
                            alert('Form contains invalid data.  Please correct first');
                        }
                    }
                    finally
                    {
                        return false;
                    }
                },
                _renderGroup: function (object, value, node)
                {
                    node.appendChild(document.createTextNode(value));
                    node.title = entities.encode(object.id);
                }
            });
    });


