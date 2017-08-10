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
        "dojo/Evented",
        "dojo/_base/lang",
        "dojo/_base/array",
        "dojo/dom-construct",
        "dijit/registry",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/FilteringSelect",
        "qpid/common/util",
        "dojo/text!addVirtualHost.html",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "qpid/common/ContextVariablesEditor",
        "dijit/TitlePane",
        "dijit/layout/ContentPane",
        "dijit/form/Form",
        "dijit/form/CheckBox",
        "dijit/form/RadioButton",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"],
    function (declare,
              Evented,
              lang,
              array,
              domConstruct,
              registry,
              Dialog,
              Button,
              FilteringSelect,
              util,
              template)
    {

        return declare("qpid.management.addVirtualHost",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                // template fields
                addVirtualHost: null,
                addButton: null,
                cancelButton: null,
                virtualHostType: null,
                typeFields: null,
                context: null,
                contextEditorPane: null,

                // constructor parameters
                management: null,
                virtualhostNodeModelObject: null,
                virtualhostNodeType : null,

                postCreate: function ()
                {
                    this.inherited(arguments);

                    this.addButton.on("click", lang.hitch(this, this._onFormSubmit));
                    this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                    this.virtualHostType.on("change", lang.hitch(this, this._onVhTypeChanged));

                    var validChildTypes = this.management ? this.management.metadata.validChildTypes("VirtualHostNode",
                        this.virtualhostNodeType,
                        "VirtualHost") : [];
                    validChildTypes.sort();

                    var virtualHostTypeStore = util.makeTypeStore(validChildTypes);

                    this.virtualHostType.set("store", virtualHostTypeStore);
                    this.virtualHostType.set("disabled", validChildTypes.length <= 1);
                    if (validChildTypes.length === 1)
                    {
                        this.virtualHostType.set("value", validChildTypes[0]);
                    }
                    else
                    {
                        this.virtualHostType.reset();
                    }
                },
                show: function ()
                {
                    util.loadEffectiveAndInheritedActualData(this.management, this.virtualhostNodeModelObject, lang.hitch(this, function(data)
                    {
                        this.virtualHostContext.setData({},
                            data.effective.context,
                            data.inheritedActual.context);
                        this.addVirtualHost.show();
                    }));
                },
                hideAndDestroy: function ()
                {
                    this.destroy();
                },
                destroy: function ()
                {
                    this._destroyTypeFields();
                    this.addVirtualHost.destroyRecursive();
                    this.inherited(arguments);
                },
                _onCancel: function (data)
                {
                    this.emit("done", false);
                },
                _onFormSubmit: function (e)
                {
                    if (this.virtualHostForm.validate())
                    {
                        var virtualHostData = util.getFormWidgetValues(this.virtualHostForm);
                        var virtualHostContext = this.virtualHostContext.get("value");
                        if (virtualHostContext)
                        {
                            virtualHostData["context"] = virtualHostContext;
                        }

                        //Default the VH name to be the same as the VHN name.
                        virtualHostData["name"] = this.virtualhostNodeModelObject.name;

                        this.management.create("virtualhost", this.virtualhostNodeModelObject, virtualHostData)
                            .then(lang.hitch(this, function (x)
                            {
                                this.emit("done", true);
                            }));
                    }
                    else
                    {
                        alert('Form contains invalid data. Please correct first');
                    }
                    return false;

                },
                _onVhTypeChanged: function (type)
                {
                    this._destroyTypeFields();
                    this.virtualHostContext.removeDynamicallyAddedInheritedContext();
                    if (type)
                    {
                        require(["qpid/management/virtualhost/" + type.toLowerCase() + "/add"], lang.hitch(this, function (typeUI)
                        {
                            try
                            {
                                var metadata = this.management.metadata;
                                typeUI.show({
                                    containerNode: this.typeFields,
                                    parent: this,
                                    metadata: metadata,
                                    type: type
                                });
                            }
                            catch (e)
                            {
                                console.warn(e);
                            }
                        }));
                    }
                },
                _destroyTypeFields: function()
                {
                    var widgets = registry.findWidgets(this.typeFields);
                    array.forEach(widgets, function (item)
                    {
                        item.destroyRecursive();
                    });
                    domConstruct.empty(this.typeFields);
                },
                on: function (type, listener)
                {
                    this.inherited(arguments);
                    if (type === "done")
                    {
                        this.addVirtualHost.on("hide", function(){listener(false);});
                    }
                }

            });
    });
