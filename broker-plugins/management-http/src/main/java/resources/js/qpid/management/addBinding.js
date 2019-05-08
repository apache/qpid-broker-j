/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
define(["dojo/_base/connect",
        "dojo/dom",
        "dojo/dom-construct",
        "dojo/_base/window",
        "dijit/registry",
        "dojo/parser",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/_base/json',
        "dojo/_base/lang",
        "dojo/_base/declare",
        "dojo/store/Memory",
        "dijit/form/FilteringSelect",
        "qpid/common/util",
        "dojo/text!addBinding.html",
        "dijit/form/NumberSpinner", // required by the form
        /* dojox/ validate resources */
        "dojox/validate/us",
        "dojox/validate/web",
        /* basic dijit classes */
        "dijit/Dialog",
        "dijit/form/CheckBox",
        "dijit/form/Textarea",
        "dijit/form/FilteringSelect",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/DateTextBox",
        "dijit/form/TimeTextBox",
        "dijit/form/Button",
        "dijit/form/RadioButton",
        "dijit/form/Form",
        "dijit/form/DateTextBox",
        /* basic dojox classes */
        "dojox/form/BusyButton",
        "dojox/form/CheckedMultiSelect",
        "dojox/grid/EnhancedGrid",
        "dojo/data/ObjectStore",
        "qpid/common/DestinationChooser",
        "dojo/domReady!"],
    function (connect,
              dom,
              construct,
              win,
              registry,
              parser,
              array,
              event,
              json,
              lang,
              declare,
              Memory,
              FilteringSelect,
              util,
              template)
    {

        var noLocalValues = new Memory({
            data: [{
                name: "true",
                id: true
            }, {
                name: "false",
                id: false
            }]
        });

        var xMatchValues = new Memory({
            data: [{
                name: "all",
                id: "all"
            }, {
                name: "any",
                id: "any"
            }]
        });

        var defaultBindingArguments = [{
            id: 0,
            name: "x-filter-jms-selector",
            value: null
        }, {
            id: 1,
            name: "x-qpid-no-local",
            value: null
        }];

        var GridWidgetProxy = declare("qpid.dojox.grid.cells.GridWidgetProxy", dojox.grid.cells._Widget, {
            createWidget: function (inNode, inDatum, inRowIndex)
            {
                var WidgetClass = this.widgetClass;
                var widgetProperties = this.getWidgetProps(inDatum);
                var getWidgetProperties = widgetProperties.getWidgetProperties;
                if (typeof getWidgetProperties == "function")
                {
                    var item = this.grid.getItem(inRowIndex);
                    if (item)
                    {
                        var additionalWidgetProperties = getWidgetProperties(inDatum, inRowIndex, item);
                        if (additionalWidgetProperties)
                        {
                            WidgetClass = additionalWidgetProperties.widgetClass;
                            for (var prop in additionalWidgetProperties)
                            {
                                if (additionalWidgetProperties.hasOwnProperty(prop) && !widgetProperties[prop])
                                {
                                    widgetProperties[prop] = additionalWidgetProperties[prop];
                                }
                            }
                        }
                    }
                }
                var widget = new WidgetClass(widgetProperties, inNode);
                return widget;
            },
            getValue: function (inRowIndex)
            {
                if (this.widget)
                {
                    return this.widget.get('value');
                }
                return null;
            },
            _finish: function (inRowIndex)
            {
                if (this.widget)
                {
                    this.inherited(arguments);
                    this.widget.destroyRecursive();
                    this.widget = null;
                }
            }
        });

        var addBinding = {};

        addBinding._init = function ()
        {
            var node = construct.create("div", {innerHTML: template});
            parser.parse(node)
                .then(lang.hitch(this, function (instances)
                {
                    this._postParse();
                }));
        };

        addBinding._postParse = function()
        {
            addBinding.dialogNode = dom.byId("addBinding");

            var theForm = registry.byId("formAddBinding");
            array.forEach(theForm.getDescendants(), function (widget)
            {
                if (widget.name === "type")
                {
                    widget.on("change", function (isChecked)
                    {

                        var obj = registry.byId(widget.id + ":fields");
                        if (obj)
                        {
                            if (isChecked)
                            {
                                obj.domNode.style.display = "block";
                                obj.resize();
                            }
                            else
                            {
                                obj.domNode.style.display = "none";
                                obj.resize();
                            }
                        }
                    })
                }

            });

            var argumentsGridNode = dom.byId("formAddbinding.bindingArguments");
            var objectStore = new dojo.data.ObjectStore({
                objectStore: new Memory({
                    data: lang.clone(defaultBindingArguments),
                    idProperty: "id"
                })
            });

            var layout = [[{
                name: "Argument Name",
                field: "name",
                width: "50%",
                editable: true
            }, {
                name: 'Argument Value',
                field: 'value',
                width: '50%',
                editable: true,
                type: GridWidgetProxy,
                widgetProps: {
                    getWidgetProperties: function (inDatum, inRowIndex, item)
                    {
                        if (item.name == "x-qpid-no-local")
                        {
                            return {
                                labelAttr: "name",
                                searchAttr: "id",
                                selectOnClick: false,
                                query: {id: "*"},
                                required: false,
                                store: noLocalValues,
                                widgetClass: dijit.form.FilteringSelect
                            };
                        }
                        else if (item.name && item.name.toLowerCase() == "x-match")
                        {
                            return {
                                labelAttr: "name",
                                searchAttr: "id",
                                selectOnClick: false,
                                query: {id: "*"},
                                required: false,
                                store: xMatchValues,
                                widgetClass: dijit.form.FilteringSelect
                            };
                        }
                        return {widgetClass: dijit.form.TextBox};
                    }
                }
            }]];

            var grid = new dojox.grid.EnhancedGrid({
                selectionMode: "multiple",
                store: objectStore,
                singleClickEdit: true,
                structure: layout,
                autoHeight: true,
                plugins: {indirectSelection: true}
            }, argumentsGridNode);
            grid.startup();

            addBinding.bindingArgumentsGrid = grid;
            addBinding.idGenerator = 1;
            var addArgumentButton = registry.byId("formAddbinding.addArgumentButton");
            var deleteArgumentButton = registry.byId("formAddbinding.deleteArgumentButton");

            var toggleGridButtons = function (index)
            {
                var data = grid.selection.getSelected();
                deleteArgumentButton.set("disabled", !data || data.length == 0);
            };
            connect.connect(grid.selection, 'onSelected', toggleGridButtons);
            connect.connect(grid.selection, 'onDeselected', toggleGridButtons);
            deleteArgumentButton.set("disabled", true);

            addArgumentButton.on("click", function (event)
            {
                addBinding.idGenerator = addBinding.idGenerator + 1;
                var newItem = {
                    id: addBinding.idGenerator,
                    name: "",
                    value: ""
                };
                grid.store.newItem(newItem);
                grid.store.save();
                grid.store.fetch({
                    onComplete: function (items, request)
                    {
                        var rowIndex = items.length - 1;
                        window.setTimeout(function ()
                        {
                            grid.focus.setFocusIndex(rowIndex, 1);
                        }, 10);
                    }
                });
            });

            deleteArgumentButton.on("click", function (event)
            {
                var data = grid.selection.getSelected();
                if (data.length)
                {
                    array.forEach(data, function (selectedItem)
                    {
                        if (selectedItem !== null)
                        {
                            grid.store.deleteItem(selectedItem);
                        }
                    });
                    grid.store.save();
                }
            });

            theForm.on("submit", function (e)
            {

                event.stop(e);
                if (theForm.validate())
                {
                    var formValues = util.getFormWidgetValues(registry.byId("formAddBinding"));
                    var parameters = {
                        destination: formValues.destination,
                        bindingKey: formValues.name,
                        replaceExistingArguments: formValues.replaceExistingArguments
                    };
                    addBinding.bindingArgumentsGrid.store.fetch({
                        onComplete: function (items, request)
                        {
                            if (items.length)
                            {
                                array.forEach(items, function (item)
                                {
                                    if (item && item.name && item.value)
                                    {
                                        parameters.arguments = parameters.arguments || {};
                                        parameters.arguments[item.name] = item.value;
                                    }
                                });
                            }
                        }
                    });

                    var exchangeModelObj = addBinding.modelObj;
                    if (exchangeModelObj.type !== "exchange")
                    {
                        exchangeModelObj = {
                            name: formValues.exchange,
                            type: "exchange",
                            parent: exchangeModelObj.parent
                        };
                    }
                    var operationModelObj = {
                        name: "bind",
                        type: "exchange",
                        parent: exchangeModelObj
                    };
                    addBinding.management.update(operationModelObj, parameters)
                        .then(function (x)
                        {
                            if (x !== 'true' && x !== true)
                            {
                                alert("Binding was not created.");
                            }
                            else
                            {
                                registry.byId("addBinding")
                                    .hide();
                            }
                        });
                    return false;
                }
                else
                {
                    alert('Form contains invalid data.  Please correct first');
                    return false;
                }

            });
        };

        addBinding.show = function (management, obj)
        {
            var that = this;
            addBinding.management = management;
            addBinding.modelObj = obj;

            addBinding.destinationChooser = registry.byId("formAddbinding.destinationChooser");

            addBinding.destinationChooserLoadPromise =
                addBinding.destinationChooser.loadData(management, addBinding.modelObj.parent);

            registry.byId("formAddBinding")
                .reset();

            var grid = addBinding.bindingArgumentsGrid;
            grid.store.fetch({
                onComplete: function (items, request)
                {
                    if (items.length)
                    {
                        array.forEach(items, function (item)
                        {
                            if (item !== null)
                            {
                                grid.store.deleteItem(item);
                            }
                        });
                    }
                }
            });
            array.forEach(lang.clone(defaultBindingArguments), function (item)
            {
                grid.store.newItem(item);
            });
            grid.store.save();

            addBinding.destinationChooserLoadPromise.then(lang.hitch(this, function (data)
            {

                var exchangeStore = new Memory({data: util.queryResultToObjects(data.exchanges)});

                if (that.exchangeChooser)
                {
                    that.exchangeChooser.destroy(false);
                }
                var exchangeDiv = dom.byId("addBinding.selectExchangeDiv");
                var input = construct.create("input", {id: "addBindingSelectExchange"}, exchangeDiv);

                that.exchangeChooser = new FilteringSelect({
                    id: "addBindingSelectExchange",
                    name: "exchange",
                    store: exchangeStore,
                    searchAttr: "name",
                    promptMessage: "Name of the exchange",
                    title: "Select the name of the exchange"
                }, input);

                if (obj.type === "exchange")
                {
                    that.exchangeChooser.set("value", obj.name);
                }
                that.exchangeChooser.set("disabled", obj.type === "exchange");

                if (obj.type === "queue")
                {
                    that.destinationChooser.set("value", obj.name);
                }
                that.destinationChooser.set("disabled", obj.type === "queue");

                registry.byId("addBinding").show();
            }));
        };

        addBinding._init();
        return addBinding;
    });
