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

define("qpid/common/MapInputWidget", [
    "dojo/_base/declare",
    "dojo/dom-construct",
    "dojo/query",
    "dojo/_base/event",
    "dojox/html/entities",
    "dijit",
    "dijit/registry",
    "dijit/form/Form",
], function (declare, domConstruct, query, event, entities, dijit, registry, Form)
{
    return declare("qpid.common.MapInputWidget", [Form],
        {
            value: {},

            _setValueAttr: function (obj)
            {
                if (typeof obj == "object")
                {
                    this.value = obj;
                    this._widgetValue = this._initWidgetValue();
                    this._listContainers().forEach(container => this._initListContainer(container));
                }
            },

            _getValueAttr()
            {
                return this._widgetValue();
            },

            _initWidgetValue()
            {
                const widgetValue = {};

                for (let [key, value] of Object.entries(this.value || {}))
                {
                    const trimmedKey = key.trim();
                    if (trimmedKey.length)
                    {
                        widgetValue[trimmedKey] = value;
                    }
                }
                this.value = widgetValue;

                return () => this.value;
            },

            _widgetValue()
            {
                return (this._widgetValue = this._initWidgetValue())();
            },

            _getWidgetAttribute(widget, name)
            {
                const attribute = widget.get(name);
                return attribute != "undefined" ? attribute : undefined;
            },

            _getWidgetAttributeAsString(widget, name)
            {
                const attribute = widget.get(name);
                if (attribute !== undefined && attribute !== null)
                {
                    const str = String(attribute).trim();
                    return str !== "undefined" ? str : "";
                }
                return "";
            },

            _getWidgetName(widget)
            {
                return this._getWidgetAttributeAsString(widget, "name");
            },

            _findNodes(className, domNode)
            {
                if (domNode)
                {
                    return query("." + className, domNode) || [];
                }
                return [];
            },

            _initWidgetName()
            {
                const name = this._getWidgetName(this);
                return () => name;
            },

            _widgetName()
            {
                return (this._widgetName = this._initWidgetName())();
            },

            _initKeyClass()
            {
                const className = this._getWidgetAttributeAsString(this, "keyClass");
                if (!className.length)
                {
                    return () => "key";
                }
                return () => className;

            },

            _keyClass()
            {
                return (this._keyClass = this._initKeyClass())();
            },

            _initValueClass()
            {
                const className = this._getWidgetAttributeAsString(this, "valueClass");
                if (!className.length)
                {
                    return () => "value";
                }
                return () => className;
            },

            _valueClass()
            {
                return (this._valueClass = this._initValueClass())();
            },

            _initListClass()
            {
                const className = this._getWidgetAttributeAsString(this, "listClass");
                if (!className.length)
                {
                    return () => "keyValueList";
                }
                return () => className;
            },

            _listClass()
            {
                return (this._listClass = this._initListClass())();
            },

            _getWidgetById(attributeName)
            {
                const id = this._getWidgetAttribute(this, attributeName);
                if (id)
                {
                    return registry.byId(id, this.domNode);
                }
                return undefined;
            },

            _getWidgetByName(name)
            {
                const childName = this._widgetName() + "." + name;
                for (let childWidget of this.getChildren() || [])
                {
                    if (this._getWidgetName(childWidget) === childName)
                    {
                        return childWidget;
                    }
                }
                return undefined;
            },

            _newSupplier(widget)
            {
                if (!widget)
                {
                    return () => null;
                }
                if (widget instanceof dijit.form.RadioButton)
                {
                    return () => this._getWidgetAttribute(widget, "checked") ? this._getWidgetAttribute(widget, "value") : null;
                }
                if (widget instanceof dijit.form.CheckBox)
                {
                    return () => this._getWidgetAttribute(widget, "checked");
                }
                return () => this._getWidgetAttribute(widget, "value");
            },

            _widget: Symbol("widget"),

            _reset: Symbol("reset"),

            _decorateWithReset(obj)
            {
                if (["function", "object"].includes(typeof obj))
                {
                    const widget = obj[this._widget];
                    if (widget)
                    {
                        if (typeof widget.reset === "function")
                        {
                            obj[this._reset] = () => widget.reset();
                            return obj;
                        }
                    }
                    obj[this._reset] = () =>
                    {
                    }
                }
                return obj;
            },

            _initKeySupplier()
            {
                let widget = this._getWidgetById("keySupplierId");
                if (!widget)
                {
                    widget = this._getWidgetByName(this._keyClass());
                }
                if (widget)
                {
                    const rawSupplier = this._newSupplier(widget);
                    const supplier = function ()
                    {
                        let result = rawSupplier();
                        if (result)
                        {
                            result = String(result).trim();
                            return result.length ? result : null;
                        }
                        return null;
                    }
                    supplier[this._widget] = widget;
                    return this._decorateWithReset(supplier);
                }
                return this._decorateWithReset(() => null);
            },

            _keySupplier()
            {
                return (this._keySupplier = this._initKeySupplier())();
            },

            _initValueSupplier()
            {
                let widget = this._getWidgetById("valueSupplierId");
                if (!widget)
                {
                    widget = this._getWidgetByName(this._valueClass());
                }
                if (widget)
                {
                    const rawSupplier = this._newSupplier(widget);
                    const supplier = function ()
                    {
                        const value = rawSupplier();
                        if (typeof value == "string" && !value.length)
                        {
                            return null;
                        }
                        return value;
                    }
                    supplier[this._widget] = widget;
                    return this._decorateWithReset(supplier);
                }
                return this._decorateWithReset(() => null);
            },

            _valueSupplier()
            {
                return (this._valueSupplier = this._initValueSupplier())();
            },

            _initKeyValueTemplate()
            {
                let template = '<div class="clear">' +
                    '<div class="key formLabel-labelCell">:</div>' +
                    '<div class="value formValue-valueCell"></div>' +
                    '</div>';
                const file = this._getWidgetAttribute(this, "keyValueTemplate");
                if (file)
                {
                    try
                    {
                        require(["dojo/text!" + file], t =>
                        {
                            if (t)
                            {
                                template = t;
                            }
                        });
                    }
                    catch (e)
                    {
                        console.warn(e);
                    }
                }
                return () => template;
            },

            _keyValueTemplate()
            {
                return (this._keyValueTemplate = this._initKeyValueTemplate())();
            },

            _insertNodeToContainer: Symbol("insertNode"),

            _deleteNodeFromContainer: Symbol("deleteNode"),

            _initListContainer(container)
            {
                domConstruct.empty(container);

                const containerDomNodes = {};
                const mapWidget = this;
                const insertNode = function (key, value)
                {
                    let newDomNode;
                    if (containerDomNodes[key])
                    {
                        newDomNode = domConstruct.place(mapWidget._keyValueTemplate(), containerDomNodes[key], "replace");
                    }
                    else
                    {
                        newDomNode = domConstruct.place(mapWidget._keyValueTemplate(), container, "last");
                    }
                    containerDomNodes[key] = newDomNode;
                    mapWidget._findNodes(mapWidget._keyClass(), newDomNode).forEach(function (node)
                    {
                        const innerHTML = node.innerHTML;
                        if (typeof innerHTML === "string")
                        {
                            node.innerHTML = entities.encode(key) + innerHTML.trim();
                        }
                        else
                        {
                            node.innerHTML = entities.encode(key);
                        }
                    });
                    mapWidget._findNodes(mapWidget._valueClass(), newDomNode).forEach(node => node.innerHTML = entities.encode(String(value)));
                }
                container[this._insertNodeToContainer] = insertNode;

                container[this._deleteNodeFromContainer] = function (key)
                {
                    if (key && containerDomNodes[key])
                    {
                        domConstruct.destroy(containerDomNodes[key]);
                        delete containerDomNodes[key];
                    }
                }

                for (let [key, value] of Object.entries(this._widgetValue()))
                {
                    insertNode(key, value);
                }
            },

            _initListContainers()
            {
                const containers = this._findNodes(this._listClass(), this.domNode);
                containers.forEach(container => this._initListContainer(container));
                return () => containers;
            },

            _listContainers()
            {
                return (this._listContainers = this._initListContainers())();
            },

            _addKeyValueItem()
            {
                const key = this._keySupplier();
                const value = this._valueSupplier();
                if (key && value)
                {
                    this._listContainers().forEach(container => container[this._insertNodeToContainer](key, value));
                    this._widgetValue()[key] = value;
                    this._keySupplier[this._reset]();
                    this._valueSupplier[this._reset]();
                }
            },

            _deleteKeyValueItem(key)
            {
                if (key && this.value[key])
                {
                    delete this.value[key];
                }
                this._listContainers().forEach(container => container[this._deleteNodeFromContainer](key));
            },

            _initWidget()
            {
                this._keyClass = this._initKeyClass();
                this._valueClass = this._initValueClass();
                this._keySupplier = this._initKeySupplier();
                this._keyValueTemplate = this._initKeyValueTemplate();
                this._valueSupplier = this._initValueSupplier();
                this._listContainers = this._initListContainers();
                this._initWidget = () =>
                {
                };
            },

            onSubmit()
            {
                this.inherited(arguments);
                if (this.isValid())
                {
                    this._addKeyValueItem();
                }
                return false;
            },

            onReset()
            {
                this.inherited(arguments);
                const key = this._keySupplier();
                if (key)
                {
                    this._deleteKeyValueItem(key);
                }
                else
                {
                    this.value = {};
                    this._listContainers().forEach(container => this._initListContainer(container));
                }
                return true;
            },

            startup()
            {
                this.inherited(arguments);
                this._initWidget();
            }
        });
});