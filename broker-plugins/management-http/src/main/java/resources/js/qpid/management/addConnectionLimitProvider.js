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
 */
define([
    "dojo/dom",
    "dojo/dom-construct",
    "dijit/registry",
    "dojo/parser",
    "dojo/_base/array",
    "dojo/_base/event",
    "qpid/common/util",
    "dojo/text!addConnectionLimitProvider.html",
    "dijit/Dialog",
    "dijit/form/Form",
    "dijit/form/TextBox",
    "dijit/form/ValidationTextBox",
    "dijit/form/FilteringSelect",
    "dijit/form/Button",
    "dojo/domReady!"], function (dom, construct, registry, parser, array, event, util, template)
{
    const addConnectionLimitProvider = {
        init: function ()
        {
            const that = this;
            this.containerNode = construct.create("div", {innerHTML: template});
            parser.parse(this.containerNode)
                .then(function ()
                {
                    that._dialog = registry.byId("addConnectionLimitProvider");

                    const providerName = registry.byId("addConnectionLimitProvider.name");
                    providerName.set("regExpGen", util.nameOrContextVarRegexp);

                    const addButton = registry.byId("addConnectionLimitProvider.addButton");
                    addButton.on("click", function (e)
                    {
                        that._add(e);
                    });

                    const cancelButton = registry.byId("addConnectionLimitProvider.cancelButton");
                    cancelButton.on("click", function (e)
                    {
                        that._cancel(e);
                    });

                    that._providerTypeFieldsContainer = dom.byId("addConnectionLimitProvider.typeFields");
                    that._providerForm = registry.byId("addConnectionLimitProvider.form");
                    that._providerType = registry.byId("addConnectionLimitProvider.type");
                    that._providerType.on("change", function (type)
                    {
                        that._providerTypeChanged(type);
                    });
                });
            return this;
        },

        show: function (management, modelObj)
        {
            this.management = management;
            this.modelObj = modelObj;
            this._providerForm.reset();

            this._category =
                modelObj && (modelObj.type === "virtualhost" || modelObj.type === "virtualhostconnectionlimitprovider")
                    ? "VirtualHostConnectionLimitProvider"
                    : "BrokerConnectionLimitProvider";
            const supportedProviderTypes = management.metadata.getTypesForCategory(this._category);
            supportedProviderTypes.sort();

            this._providerType.set("store", util.makeTypeStore(supportedProviderTypes));
            this._dialog.show();
        },

        _cancel: function (e)
        {
            event.stop(e);
            this._destroyTypeFields();
            this._dialog.hide();
        },

        _add: function (e)
        {
            event.stop(e);
            this._submit();
        },

        _submit: function ()
        {
            if (this._providerForm.validate())
            {
                const data = util.getFormWidgetValues(this._providerForm, this.initialData);
                const that = this;
                this.management.create(this._category, this.modelObj, data)
                    .then(function ()
                    {
                        that._dialog.hide();
                    });
            }
            else
            {
                alert('Form contains invalid data. Please correct first');
            }
        },

        _providerTypeChanged: function (type)
        {
            this._destroyTypeFields();
            if (type)
            {
                const that = this;
                require(["qpid/management/connectionlimitprovider/" + type.toLowerCase() + "/add"], function (typeUI)
                {
                    try
                    {
                        typeUI.show({
                            containerNode: that._providerTypeFieldsContainer,
                            parent: that,
                            initialData: that.initialData || {},
                            metadata: that.management.metadata,
                            effectiveData: {},
                            category: that._category,
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

        _destroyTypeFields: function ()
        {
            const widgets = registry.findWidgets(this._providerTypeFieldsContainer);
            array.forEach(widgets, function (item)
            {
                item.destroyRecursive();
            });
            construct.empty(this._providerTypeFieldsContainer);
        }
    };

    try
    {
        addConnectionLimitProvider.init();
    }
    catch (e)
    {
        console.warn(e);
    }
    return addConnectionLimitProvider;
});
