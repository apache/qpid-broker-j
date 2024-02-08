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
define(["dojo/_base/event",
        "dojo/_base/lang",
        "dojo/_base/array",
        "dojo/dom",
        "dojo/dom-construct",
        "dojo/json",
        "dojo/parser",
        "dojo/store/Memory",
        "dojo/window",
        "dojo/on",
        "dojox/lang/functional/object",
        "dijit/registry",
        "dijit/Dialog",
        "dijit/form/Button",
        "dijit/form/FilteringSelect",
        "qpid/common/properties",
        "qpid/common/util",
        "dojo/text!addVirtualHostNodeAndVirtualHost.html",
        "qpid/common/ContextVariablesEditor",
        "dijit/TitlePane",
        "dijit/layout/ContentPane",
        "dijit/form/Form",
        "dijit/form/CheckBox",
        "dijit/form/RadioButton",
        "dojox/form/Uploader",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"],
    function (event,
              lang,
              array,
              dom,
              domConstruct,
              json,
              parser,
              Memory,
              win,
              on,
              fobject,
              registry,
              Dialog,
              Button,
              FilteringSelect,
              properties,
              util,
              template)
    {
        const addVirtualHostNodeAndVirtualHost = {
            init: function ()
            {
                const that = this;
                this.containerNode = domConstruct.create("div", {innerHTML: template});
                parser.parse(this.containerNode)
                    .then(function (instances)
                    {
                        that._postParse();
                    });
            },
            _postParse: function ()
            {
                const that = this;
                const virtualHostNodeName = registry.byId("addVirtualHostNode.nodeName");
                virtualHostNodeName.set("regExpGen", util.virtualHostNameOrContextVarRegexp);

                // Readers are HTML5
                this.reader = window.FileReader ? new FileReader() : undefined;

                this.dialog = registry.byId("addVirtualHostNodeAndVirtualHost");
                this.dialog.on("hide", lang.hitch(this, this._cancel));
                this.addButton = registry.byId("addVirtualHostNodeAndVirtualHost.addButton");
                this.cancelButton = registry.byId("addVirtualHostNodeAndVirtualHost.cancelButton");
                this.cancelButton.on("click", lang.hitch(this, this._cancel));
                this.addButton.on("click", lang.hitch(this, this._add));

                this.virtualHostNodeTypeFieldsContainer = dom.byId("addVirtualHostNode.typeFields");
                this.virtualHostNodeSelectedFileContainer = dom.byId("addVirtualHostNode.selectedFile");
                this.virtualHostNodeSelectedFileStatusContainer = dom.byId("addVirtualHostNode.selectedFileStatus");
                this.virtualHostNodeUploadFields = dom.byId("addVirtualHostNode.uploadFields");
                this.virtualHostNodeFileFields = dom.byId("addVirtualHostNode.fileFields");

                this.virtualHostNodeForm = registry.byId("addVirtualHostNode.form");
                this.virtualHostNodeType = registry.byId("addVirtualHostNode.type");
                this.virtualHostNodeFileCheck = registry.byId("addVirtualHostNode.upload");
                this.virtualHostNodeFile = registry.byId("addVirtualHostNode.file");

                this.virtualHostNodeType.set("disabled", true);

                this.virtualHostTypeFieldsContainer = dom.byId("addVirtualHost.typeFields");
                this.virtualHostForm = registry.byId("addVirtualHost.form");
                this.virtualHostType = registry.byId("addVirtualHost.type");

                this.virtualHostType.set("disabled", true);

                this.virtualHostNodeType.set("disabled", false);
                this.virtualHostNodeType.on("change", function (type)
                {
                    that._vhnTypeChanged(type,
                        that.virtualHostNodeTypeFieldsContainer,
                        "qpid/management/virtualhostnode/");
                });

                this.virtualHostType.set("disabled", true);
                this.virtualHostType.on("change", function (type)
                {
                    that._vhTypeChanged(type, that.virtualHostTypeFieldsContainer, "qpid/management/virtualhost/");
                });

                if (this.reader)
                {
                    this.reader.onload = function (evt)
                    {
                        that._vhnUploadFileComplete(evt);
                    };
                    this.reader.onerror = function (ex)
                    {
                        console.error("Failed to load JSON file", ex);
                    };
                    this.virtualHostNodeFile.on("change", function (selected)
                    {
                        that._vhnFileChanged(selected)
                    });
                    this.virtualHostNodeFileCheck.on("change", function (selected)
                    {
                        that._vhnFileFlagChanged(selected)
                    });
                }
                else
                {
                    // Fall back for IE8/9 which do not support FileReader
                    this.virtualHostNodeFileCheck.set("disabled", "disabled");
                    this.virtualHostNodeFileCheck.set("title", "Requires a more recent browser with HTML5 support");
                }

                this.virtualHostNodeUploadFields.style.display = "none";
                this.virtualHostNodeFileFields.style.display = "none";
            },
            show: function (management)
            {
                this.management = management;
                this.virtualHostNodeForm.reset();
                this.virtualHostNodeType.set("value", null);

                this.virtualHostForm.reset();
                this.virtualHostType.set("value", null);

                const supportedVirtualHostNodeTypes = management.metadata.getTypesForCategory("VirtualHostNode");
                supportedVirtualHostNodeTypes.sort();

                const virtualHostNodeTypeStore = util.makeTypeStore(supportedVirtualHostNodeTypes);
                this.virtualHostNodeType.set("store", virtualHostNodeTypeStore);

                if (!this.virtualHostNodeContext)
                {
                    this.virtualHostNodeContext = new qpid.common.ContextVariablesEditor({
                        name: 'context',
                        title: 'Context variables'
                    });
                    this.virtualHostNodeContext.placeAt(dom.byId("addVirtualHostNode.context"));
                    const that = this;
                    this.virtualHostNodeContext.on("change", function (value)
                    {
                        const inherited = that.virtualHostContext.inheritedActualValues;
                        const effective = that.virtualHostContext.effectiveValues;
                        const actuals = that.virtualHostContext.value;
                        for (let key in value)
                        {
                            if (!actuals || !(key in actuals))
                            {
                                const val = value[key];
                                inherited[key] = val;
                                if (!(key in effective))
                                {
                                    effective[key] = val.indexOf("${") === -1 ? val : "";
                                }
                            }
                        }
                        that.virtualHostContext.setData(that.virtualHostContext.value, effective, inherited);
                    });
                }
                if (!this.virtualHostContext)
                {
                    this.virtualHostContext = new qpid.common.ContextVariablesEditor({
                        name: 'context',
                        title: 'Context variables'
                    });
                    this.virtualHostContext.placeAt(dom.byId("addVirtualHost.context"));

                }

                const that = this;
                util.loadEffectiveAndInheritedActualData(management, {type: "broker"}, function(data)
                {
                    that.virtualHostNodeContext.setData({},
                        data.effective.context,
                        data.inheritedActual.context);
                    that.virtualHostContext.setData({},
                        data.effective.context,
                        data.inheritedActual.context);
                });

                this.dialog.show();
                if (!this.resizeEventRegistered)
                {
                    this.resizeEventRegistered = true;
                    util.resizeContentAreaAndRepositionDialog(dom.byId("addVirtualHostNodeAndVirtualHost.contentPane"),
                        this.dialog);
                }
            },
            destroy: function ()
            {
                if (this.dialog)
                {
                    this.dialog.destroyRecursive();
                    this.dialog = null;
                }

                if (this.containerNode)
                {
                    domConstruct.destroy(this.containerNode);
                    this.containerNode = null;
                }
            },
            _vhnTypeChanged: function (type, typeFieldsContainer, urlStem)
            {
                const validChildTypes = this.management ? this.management.metadata.validChildTypes("VirtualHostNode",
                    type,
                    "VirtualHost") : [];
                validChildTypes.sort();

                const virtualHostTypeStore = util.makeTypeStore(validChildTypes);

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

                const vhnTypeSelected = !(type == '');
                this.virtualHostNodeUploadFields.style.display = vhnTypeSelected ? "block" : "none";

                if (!vhnTypeSelected)
                {
                    this._vhnFileFlagChanged(false);
                }

                this._typeChanged(type, typeFieldsContainer, urlStem, "VirtualHostNode");
            },
            _vhTypeChanged: function (type, typeFieldsContainer, urlStem)
            {
                this._typeChanged(type, typeFieldsContainer, urlStem, "VirtualHost");
            },
            _typeChanged: function (type, typeFieldsContainer, urlStem, category)
            {
                this._destroyContainerWidgets(typeFieldsContainer);
                if (category)
                {
                    const context = this["v" + category.substring(1) + "Context"];
                    if (context)
                    {
                        context.removeDynamicallyAddedInheritedContext();
                    }
                }
                if (type)
                {
                    const that = this;
                    require([urlStem + type.toLowerCase() + "/add"], function (typeUI)
                    {
                        try
                        {
                            const metadata = that.management.metadata;
                            typeUI.show({
                                containerNode: typeFieldsContainer,
                                parent: that,
                                metadata: metadata,
                                type: type
                            });
                        }
                        catch (e)
                        {
                            console.warn(e);
                        }
                    });
                }
            },
            _destroyContainerWidgets: function(typeFieldsContainer)
            {
                if (typeFieldsContainer)
                {
                    const widgets = registry.findWidgets(typeFieldsContainer);
                    array.forEach(widgets, function (item)
                    {
                        item.destroyRecursive();
                    });
                    domConstruct.empty(typeFieldsContainer);
                }
            },
            _vhnFileFlagChanged: function (selected)
            {
                this.virtualHostForm.domNode.style.display = selected ? "none" : "block";
                this.virtualHostNodeFileFields.style.display = selected ? "block" : "none";
                this.virtualHostType.set("required", !selected);
                this.virtualHostNodeFile.reset();
                this.virtualHostInitialConfiguration = undefined;
                this.virtualHostNodeSelectedFileContainer.innerHTML = "";
                this.virtualHostNodeSelectedFileStatusContainer.className = "";
            },
            _vhnFileChanged: function (evt)
            {
                // We only ever expect a single file
                const file = this.virtualHostNodeFile.domNode.children[0].files[0];

                this.addButton.set("disabled", true);
                this.virtualHostNodeSelectedFileContainer.innerHTML = file.name;
                this.virtualHostNodeSelectedFileStatusContainer.className = "loadingIcon";

                console.log("Beginning to read file " + file.name);
                this.reader.readAsDataURL(file);
            },
            _vhnUploadFileComplete: function (evt)
            {
                const reader = evt.target;
                const result = reader.result;
                console.log("File read complete, contents " + result);
                this.virtualHostInitialConfiguration = result;
                this.addButton.set("disabled", false);
                this.virtualHostNodeSelectedFileStatusContainer.className = "loadedIcon";
            },
            _cancel: function (e)
            {
                util.abortReaderSafely(this.reader);
                this._destroyContainerWidgets(this.virtualHostNodeTypeFieldsContainer);
                this._destroyContainerWidgets(this.virtualHostTypeFieldsContainer);
                this.dialog.hide();
            },
            _add: function (e)
            {
                event.stop(e);
                this._submit();
            },
            _submit: function ()
            {
                const uploadVHConfig = this.virtualHostNodeFileCheck.get("checked");
                let virtualHostNodeData = undefined;

                if (uploadVHConfig && this.virtualHostNodeFile.getFileList().length > 0
                    && this.virtualHostNodeForm.validate())
                {
                    // VH config is being uploaded
                    virtualHostNodeData = util.getFormWidgetValues(this.virtualHostNodeForm);
                    const virtualHostNodeContext = this.virtualHostNodeContext.get("value");
                    if (virtualHostNodeContext)
                    {
                        virtualHostNodeData["context"] = virtualHostNodeContext;
                    }

                    // Add the loaded virtualhost configuration
                    virtualHostNodeData["virtualHostInitialConfiguration"] = this.virtualHostInitialConfiguration;
                }
                else if (!uploadVHConfig && this.virtualHostNodeForm.validate() && this.virtualHostForm.validate())
                {
                    virtualHostNodeData = util.getFormWidgetValues(this.virtualHostNodeForm);
                    const virtualHostNodeContext = this.virtualHostNodeContext.get("value");
                    if (virtualHostNodeContext)
                    {
                        virtualHostNodeData["context"] = virtualHostNodeContext;
                    }

                    const virtualHostData = util.getFormWidgetValues(this.virtualHostForm);
                    const virtualHostContext = this.virtualHostContext.get("value");
                    if (virtualHostContext)
                    {
                        virtualHostData["context"] = virtualHostContext;
                    }

                    const keystore = dijit.registry.byId("addVirtualHost.keyStore")?.get('value');
                    if (keystore)
                    {
                        virtualHostData["keyStore"] = keystore;
                    }

                    const keystorePathPropertyName = dijit.registry.byId("addVirtualHost.keyStorePathPropertyName")?.get("value");
                    if (keystorePathPropertyName)
                    {
                        virtualHostData["keystorePathPropertyName"] = keystorePathPropertyName;
                    }

                    const keystorePasswordPropertyName = dijit.registry.byId("addVirtualHost.keyStorePasswordPropertyName")?.get("value");
                    if (keystorePasswordPropertyName)
                    {
                        virtualHostData["keystorePasswordPropertyName"] = keystorePasswordPropertyName;
                    }

                    const truststore = dijit.registry.byId("addVirtualHost.trustStore")?.get("value");
                    if (truststore)
                    {
                        virtualHostData["trustStore"] = truststore;
                    }

                    const truststorePathPropertyName = dijit.registry.byId("addVirtualHost.trustStorePathPropertyName")?.get("value");
                    if (truststorePathPropertyName)
                    {
                        virtualHostData["truststorePathPropertyName"] = truststorePathPropertyName;
                    }

                    const truststorePasswordPropertyName = dijit.registry.byId("addVirtualHost.trustStorePasswordPropertyName")?.get("value");
                    if (truststorePasswordPropertyName)
                    {
                        virtualHostData["truststorePasswordPropertyName"] = truststorePasswordPropertyName;
                    }

                    //Default the VH name to be the same as the VHN name.
                    virtualHostData["name"] = virtualHostNodeData["name"];

                    virtualHostNodeData["virtualHostInitialConfiguration"] = json.stringify(virtualHostData)

                }
                else
                {
                    alert('Form contains invalid data. Please correct first');
                    return;
                }

                const that = this;
                that.management.create("virtualhostnode", {type: "broker"}, virtualHostNodeData)
                    .then((x) => that.dialog.hide());
            }
        };

        addVirtualHostNodeAndVirtualHost.init();

        return addVirtualHostNodeAndVirtualHost;
    });
