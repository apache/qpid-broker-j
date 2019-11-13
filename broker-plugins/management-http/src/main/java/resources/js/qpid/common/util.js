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
define(["dojo/_base/xhr",
        "dojo/_base/array",
        "dojo/_base/event",
        "dojo/_base/lang",
        "dojo/json",
        "dojo/dom-construct",
        "dojo/dom-geometry",
        "dojo/dom-style",
        "dojo/Deferred",
        "dojo/_base/window",
        "dojo/query",
        "dojo/parser",
        "dojo/promise/all",
        "dojo/store/Memory",
        "dojo/window",
        "dojo/on",
        "dojox/html/entities",
        "qpid/common/widgetconfigurer",
        "dijit/registry",
        "qpid/common/WarningPane",
        "qpid/common/updater",
        "dijit/TitlePane",
        "dijit/Dialog",
        "dijit/form/Form",
        "dijit/form/Button",
        "dijit/form/RadioButton",
        "dijit/form/CheckBox",
        "dijit/form/FilteringSelect",
        "dijit/form/ValidationTextBox",
        "dojox/layout/TableContainer",
        "dijit/layout/ContentPane",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"],
    function (xhr,
              array,
              event,
              lang,
              json,
              dom,
              geometry,
              domStyle,
              Deferred,
              win,
              query,
              parser,
              all,
              Memory,
              w,
              on,
              entities,
              widgetconfigurer,
              registry,
              WarningPane,
              updater)
    {
        var util = {};

        if (Number.isInteger)
        {
            util.isInteger = function(value)
            {
                return Number.isInteger(value);
            };
        }
        else
        {
            util.isInteger = function(value)
            {
                return typeof value === "number" && isFinite(value) && Math.floor(value) === value;
            };
        }

        if (Array.isArray)
        {
            util.isArray = function (object)
            {
                return Array.isArray(object);
            };
        }
        else
        {
            util.isArray = function (object)
            {
                return object instanceof Array;
            };
        }

        util.flattenStatistics = function (data)
        {
            var attrName, stats, propName, theList;
            for (attrName in data)
            {
                if (data.hasOwnProperty(attrName))
                {
                    if (attrName == "statistics")
                    {
                        stats = data.statistics;
                        for (propName in stats)
                        {
                            if (stats.hasOwnProperty(propName))
                            {
                                data[propName] = stats[propName];
                            }
                        }
                    }
                    else if (data[attrName] instanceof Array)
                    {
                        theList = data[attrName];

                        for (var i = 0; i < theList.length; i++)
                        {
                            util.flattenStatistics(theList[i]);
                        }
                    }
                }
            }
        };

        util.isReservedExchangeName = function (exchangeName)
        {
            return exchangeName == null || exchangeName == "" || "<<default>>" == exchangeName || exchangeName.indexOf(
                    "amq.") == 0 || exchangeName.indexOf("qpid.") == 0;
        };

        util.confirmAndDeleteGridSelection = function (grid, confirmationMessageStart, deleteFunction)
        {
            var data = grid.selection.getSelected();
            var confirmed = false;
            if (data.length)
            {
                var confirmationMessage = null;
                if (data.length == 1)
                {
                    confirmationMessage = confirmationMessageStart + " '" + data[0].name + "'?";
                }
                else
                {
                    var names = '';
                    for (var i = 0; i < data.length; i++)
                    {
                        if (names)
                        {
                            names += ', ';
                        }
                        names += "\"" + data[i].name + "\"";
                    }
                    confirmationMessage = confirmationMessageStart + "s " + names + "?";
                }

                if (confirm(confirmationMessage))
                {
                    confirmed = true;
                    deleteFunction(data);
                }
            }
            return confirmed;
        };

        util.buildDeleteQuery = function (data, url, idParam)
        {
            var queryParam;
            for (var i = 0; i < data.length; i++)
            {
                if (queryParam)
                {
                    queryParam += "&";
                }
                else
                {
                    queryParam = "?";
                }
                queryParam += ( idParam || "id" ) + "=" + encodeURIComponent(data[i].id);
            }
            return url + queryParam;
        };

        util.deleteSelectedObjects =
            function (grid, confirmationMessageStart, management, modelObj, updater, idParam, callback)
            {
                return util.confirmAndDeleteGridSelection(grid, confirmationMessageStart, function (data)
                {
                    util.deleteObjects(management, data, modelObj, idParam, grid, updater, callback);
                });
            };

        util.deleteObjects = function (management, data, modelObj, idParam, grid, updater, callback)
        {
            var name = idParam || "id";
            var parameters = {};
            parameters[name] = [];
            for (var i = 0; i < data.length; i++)
            {
                parameters[name].push(data[i].id);

            }
            management.remove(modelObj, parameters)
                .then(function (result)
                {
                    grid.selection.deselectAll();
                    if (updater)
                    {
                        updater.update();
                    }
                    if (callback)
                    {
                        callback(data);
                    }
                }, util.xhrErrorHandler);
        };

        util.deleteSelectedRows = function (grid, confirmationMessageStart, management, url, updater, idParam, callback)
        {
            return util.confirmAndDeleteGridSelection(grid, confirmationMessageStart, function (data)
            {
                util.deleteData(management, data, url, idParam, grid, updater, callback);
            });
        };

        util.deleteData = function (management, data, url, idParam, grid, updater, callback)
        {
            var query = util.buildDeleteQuery(data, url, idParam);
            management.del({url: query})
                .then(function (result)
                {
                    grid.selection.deselectAll();
                    if (updater)
                    {
                        updater.update();
                    }
                    if (callback)
                    {
                        callback(data);
                    }
                }, util.xhrErrorHandler);
        };

        util.findAllWidgets = function (root)
        {
            return query("[widgetid]", root)
                .map(registry.byNode)
                .filter(function (w)
                {
                    return w;
                });
        };

        util.tabErrorHandler = function (error, tabData)
        {
            var category = tabData.category;
            var name = tabData.name;
            var message = category.charAt(0)
                              .toUpperCase() + category.slice(1) + " '" + name
                          + "' is unavailable or deleted. Tab auto-refresh is stopped.";

            var cleanUpTab = function ()
            {
                // stop updating the tab
                updater.remove(tabData.updater);

                // delete tab widgets
                var widgets = registry.findWidgets(tabData.contentPane.containerNode);
                array.forEach(widgets, function (item)
                {
                    item.destroyRecursive();
                });
                dom.empty(tabData.contentPane.containerNode);
            };

            var closeTab = function (e)
            {
                tabData.contentPane.onClose();
                tabData.tabContainer.removeChild(tabData.contentPane);
                tabData.contentPane.destroyRecursive();
            };

            util.responseErrorHandler(error, {
                "404": util.warnOn404ErrorHandler(message, tabData.contentPane.containerNode, cleanUpTab, closeTab)
            });
        };

        util.warnOn404ErrorHandler = function (message, containerNode, cleanUpCallback, onClickHandler)
        {
            return function ()
            {
                try
                {
                    if (cleanUpCallback)
                    {
                        cleanUpCallback();
                    }

                    var node = dom.create("div", {innerHTML: message}, containerNode);
                    var warningPane = new WarningPane({message: message}, node);
                    if (onClickHandler)
                    {
                        warningPane.on("click", onClickHandler);
                    }
                    else
                    {
                        warningPane.closeButton.set("disabled", true);
                    }
                }
                catch (e)
                {
                    console.error(e);
                }
            };
        };

        util.responseErrorHandler = function (error, responseCodeHandlerMap, defaultCallback)
        {
            var handler;
            if (error)
            {
                var status = error.status || (error.response ? error.response.status : null);
                if (status != undefined && status != null)
                {
                    handler = responseCodeHandlerMap[String(status)];
                }
            }

            if (!handler)
            {
                handler = defaultCallback || util.consoleLoggingErrorHandler;
            }

            handler(error);
        };

        util.consoleLoggingErrorHandler = function (error)
        {
            var message = util.getErrorMessage(error, "Unexpected error is reported by the broker");
            console.error(message);
        };

        util.xhrErrorHandler = function (error)
        {
            var fallback = "Unexpected error - see server logs";
            var statusCodeNode = dojo.byId("errorDialog.statusCode");
            var errorMessageNode = dojo.byId("errorDialog.errorMessage");
            var userMustReauth = false;

            if (error)
            {
                var status = error.status || (error.response ? error.response.status : null);
                if (status != undefined && status != null)
                {
                    var message;

                    if (status == 0)
                    {
                        message = "Unable to contact the Broker";
                    }
                    else if (status == 401)
                    {
                        message = "Authentication required";
                        userMustReauth = true;
                    }
                    else if (status == 403)
                    {
                        message = "Access Forbidden";
                        var m = util.getErrorMessage(error, "");
                        if (m)
                        {
                            message += ": " + m;
                        }
                    }
                    else
                    {
                        message = util.getErrorMessage(error, fallback);
                    }

                    errorMessageNode.innerHTML = entities.encode(message ? message : fallback);
                    statusCodeNode.innerHTML = entities.encode(String(status));

                    dojo.byId("errorDialog.advice.retry").style.display = userMustReauth ? "none" : "block";
                    dojo.byId("errorDialog.advice.reconnect").style.display = userMustReauth ? "block" : "none";

                    domStyle.set(registry.byId("errorDialog.button.cancel").domNode,
                        'display',
                        userMustReauth ? "none" : "block");
                    domStyle.set(registry.byId("errorDialog.button.relogin").domNode,
                        'display',
                        userMustReauth ? "block" : "none");

                }
                else
                {
                    statusCodeNode.innerHTML = "";
                    errorMessageNode.innerHTML = fallback;
                }

                var dialog = dijit.byId("errorDialog");
                if (!dialog.open)
                {
                    dialog.show();
                }
            }
        };

        util.getErrorMessage = function (error, fallback)
        {
            var message = error.message ? error.message : fallback;

            var responseText = error.responseText ? error.responseText : (error.response ? error.response.text : null);

            // Try for a more detail error sent by the Broker as json
            if (responseText)
            {
                try
                {
                    var errorObj = json.parse(responseText);
                    message = errorObj.hasOwnProperty("errorMessage") ? errorObj.errorMessage : message;
                }
                catch (e)
                {
                    // Ignore
                }
            }
            return message || fallback;
        };

        util.equals = function (object1, object2)
        {
            if (object1 && object2)
            {
                if (typeof object1 != typeof object2)
                {
                    return false;
                }
                else
                {
                    if (object1 instanceof Array || typeof object1 == "array")
                    {
                        if (object1.length != object2.length)
                        {
                            return false;
                        }

                        for (var i = 0, l = object1.length; i < l; i++)
                        {
                            var item = object1[i];
                            if (item && (item instanceof Array || typeof item == "array" || item instanceof Object))
                            {
                                if (!this.equals(item, object2[i]))
                                {
                                    return false;
                                }
                            }
                            else if (item != object2[i])
                            {
                                return false;
                            }
                        }

                        return true;
                    }
                    else if (object1 instanceof Object)
                    {
                        for (propName in object1)
                        {
                            if (object1.hasOwnProperty(propName) != object2.hasOwnProperty(propName))
                            {
                                return false;
                            }
                            else if (typeof object1[propName] != typeof object2[propName])
                            {
                                return false;
                            }
                        }

                        for (propName in object2)
                        {
                            var object1Prop = object1[propName];
                            var object2Prop = object2[propName];

                            if (object2.hasOwnProperty(propName) != object1.hasOwnProperty(propName))
                            {
                                return false;
                            }
                            else if (typeof object1Prop != typeof object2Prop)
                            {
                                return false;
                            }

                            if (!object2.hasOwnProperty(propName))
                            {
                                // skip functions
                                continue;
                            }

                            if (object1Prop && (object1Prop instanceof Array || typeof object1Prop == "array"
                                                || object1Prop instanceof Object))
                            {
                                if (!this.equals(object1Prop, object2Prop))
                                {
                                    return false;
                                }
                            }
                            else if (object1Prop != object2Prop)
                            {
                                return false;
                            }
                        }
                        return true;
                    }
                }
            }
            return object1 === object2;
        };

        util.parseHtmlIntoDiv = function (containerNode, htmlTemplateLocation, postParseCallback)
        {
            xhr.get({
                url: htmlTemplateLocation,
                load: function (template)
                {
                    util.parse(containerNode, template, postParseCallback);
                }
            });
        };

        util.parse = function (containerNode, template, postParseCallback)
        {
            containerNode.innerHTML = template;
            parser.parse(containerNode)
                .then(function (instances)
                {
                    if (postParseCallback && typeof postParseCallback == "function")
                    {
                        postParseCallback();
                    }
                }, function (e)
                {
                    console.error("Parse error:" + e);
                });
        };
        util.buildUI = function (containerNode, parent, htmlTemplateLocation, fieldNames, obj, postParseCallback)
        {
            this.parseHtmlIntoDiv(containerNode, htmlTemplateLocation, function ()
            {
                if (fieldNames && obj)
                {
                    for (var i = 0; i < fieldNames.length; i++)
                    {
                        var fieldName = fieldNames[i];
                        obj[fieldName] = query("." + fieldName, containerNode)[0];
                    }
                }

                if (postParseCallback && typeof postParseCallback == "function")
                {
                    postParseCallback();
                }
            });

        };

        util.updateUI = function (data, fieldNames, obj, formatters)
        {
            for (var i = 0; i < fieldNames.length; i++)
            {
                var fieldName = fieldNames[i];
                var value = data[fieldName];
                var fieldNode = obj[fieldName];
                if (fieldNode)
                {
                    var formatter = null;
                    if (formatters && fieldNode.className)
                    {
                        var clazzes = fieldNode.className.split(" ");
                        for (var idx in clazzes)
                        {
                            var clazz = clazzes[idx];
                            var fmt = formatters[clazz];
                            if (!!fmt)
                            {
                                formatter = fmt;
                                break;
                            }
                        }
                    }
                    var innerHtml = "";
                    if (value !== undefined && value !== null)
                    {
                        innerHtml = formatter === null ? entities.encode(String(value)) : formatter(value);
                    }
                    fieldNode.innerHTML = innerHtml;
                }
            }
        };

        util.getTypeFields = function (metadata, category, type)
        {
            var fields = [];
            var typeMeta = metadata.getMetaData(category, type);
            if (!!typeMeta)
            {
                var attributes = typeMeta.attributes;
                for (var name in attributes)
                {
                    if (attributes.hasOwnProperty(name))
                    {
                        fields.push(name);
                    }
                }
            }
            return fields;
        };

        util.applyMetadataToWidgets = function (domRoot, category, type, meta)
        {
            this.applyToWidgets(domRoot, category, type, null, meta);
        };

        util.applyToWidgets = function (domRoot, category, type, data, meta, effectiveData)
        {
            var widgets = util.findAllWidgets(domRoot);
            array.forEach(widgets, function (widget)
            {
                widgetconfigurer.config(widget, category, type, data, meta, effectiveData);
            });
        };

        util.disableWidgetsForImmutableFields = function (domRoot, category, type, meta)
        {
            var widgets = util.findAllWidgets(domRoot);
            array.forEach(widgets, function (widget)
            {
                widgetconfigurer.disableIfImmutable(widget, category, type, meta);
            });
        };

        util.getFormWidgetValues = function (form, initialData)
        {
            var values = {};
            var formWidgets = form.getChildren();
            for (var i in formWidgets)
            {
                var widget = formWidgets[i];
                var value = widget.get("value") != "undefined" ? widget.get("value") : undefined;
                var propName = widget.get("name") != "undefined" ? widget.get("name") : undefined;
                var required = widget.get("required") != "undefined" ? widget.get("required") : undefined;
                var excluded = widget.get("excluded") != "undefined" ? widget.get("excluded") : undefined;
                var checked = widget.get("checked") != "undefined" ? widget.get("checked") : undefined;
                var type = widget.get("type") != "undefined" ? widget.get("type") : undefined;
                if (!excluded && propName && (value !== undefined || required))
                {
                    if (widget instanceof dijit.form.RadioButton)
                    {
                        if (checked)
                        {
                            var currentValue = values[propName];
                            if (currentValue)
                            {
                                if (lang.isArray(currentValue))
                                {
                                    currentValue.push(value)
                                }
                                else
                                {
                                    values[propName] = [currentValue, value];
                                }
                            }
                            else
                            {
                                values[propName] = value;
                            }
                        }
                    }
                    else if (widget instanceof dijit.form.CheckBox)
                    {
                        values[propName] = checked;
                    }
                    else
                    {
                        if (type === "password" || widget.hasOwnProperty("secureAttribute"))
                        {
                            if (value)
                            {
                                values[propName] = value;
                            }
                        }
                        else
                        {
                             if (widget.hasOwnProperty("effectiveDefaultValue") &&
                                 value === widget.effectiveDefaultValue && initialData &&
                                 (initialData[propName] === null || initialData[propName] === undefined))
                             {
                                 // widget value is effective default value, thus, skipping it...
                                 continue;
                             }
                            values[propName] = value ? value : null;
                        }
                    }
                }
            }
            if (initialData)
            {
                for (var propName in values)
                {
                    if (values[propName] == initialData[propName])
                    {
                        delete values[propName];
                    }
                }
            }
            return values;
        };

        util.updateUpdatableStore = function (updatableStore, data)
        {
            var currentRowCount = updatableStore.grid.rowCount;
            updatableStore.grid.domNode.style.display = data ? "block" : "none";
            updatableStore.update(data || []);
            if (data)
            {
                if (currentRowCount == 0 && data.length == 1)
                {
                    // grid with a single row is not rendering properly after being hidden
                    // force rendering
                    updatableStore.grid.render();
                }
            }
        };

        util.makeTypeStore = function (types)
        {
            var typeData = [];
            for (var i = 0; i < types.length; i++)
            {
                var type = types[i];
                typeData.push({
                    id: type,
                    name: type
                });
            }
            return new Memory({data: typeData});
        };

        util.makeInstanceStore = function (management, parentCategory, category, callback)
        {
            var obj = {
                type: category.toLowerCase(),
                parent: {type: parentCategory.toLowerCase()}
            };
            management.load(obj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    var items = [];
                    for (var i = 0; i < data.length; i++)
                    {
                        items.push({
                            id: data[i].name,
                            name: data[i].name
                        });
                    }
                    var store = new Memory({data: items});
                    callback(store);
                });
        };

        util.queryResultToObjects = function (queryResult)
        {
            var objects = [];
            var headers = queryResult.headers;
            var results = queryResult.results;
            for (var i = 0, l1 = results.length; i < l1; ++i)
            {
                var result = {};
                for (var j = 0, l2 = headers.length; j < l2; ++j)
                {
                    result[headers[j]] = results[i][j];
                }
                objects.push(result);
            }
            return objects;
        };

        util.setMultiSelectOptions = function (multiSelectWidget, options)
        {
            util.addMultiSelectOptions(multiSelectWidget, options, true);
        };

        util.addMultiSelectOptions = function (multiSelectWidget, options, clearExistingOptions)
        {
            if (clearExistingOptions)
            {
                var children = multiSelectWidget.children;
                var initialLength = children.length;
                for (var i = initialLength - 1; i >= 0; i--)
                {
                    var child = children.item(i);
                    multiSelectWidget.removeChild(child);
                }
            }
            for (var i = 0; i < options.length; i++)
            {
                // construct new option for list
                var newOption = win.doc.createElement('option');
                var value = options[i];
                newOption.innerHTML = value;
                newOption.value = value;

                // add new option to list
                multiSelectWidget.appendChild(newOption);
            }
        };

        var singleContextVarRegexp = "(\\${[\\w+\\.\\-:]+})";

        util.numericOrContextVarRegexp = function (constraints)
        {
            return "^(\\d+)|" + singleContextVarRegexp + "$";
        };

        util.signedOrContextVarRegexp = function (constraints)
        {
            return "^(-?\\d+)|" + singleContextVarRegexp + "$";
        };

        util.nameOrContextVarRegexp = function (constraints)
        {
            return "^([0-9a-zA-Z\\-_.:]+)|" + singleContextVarRegexp + "$";
        };

        util.virtualHostNameOrContextVarRegexp = function (constraints)
        {
            return "^([0-9a-zA-Z\\-_]+)|" + singleContextVarRegexp + "$";
        };

        util.jdbcUrlOrContextVarRegexp = function (constraints)
        {
            return "^(jdbc:.*:.*)|" + singleContextVarRegexp + "$";
        };

        util.nodeAddressOrContextVarRegexp = function (constraints)
        {
            return "^(([0-9a-zA-Z.\\-_]|::)+:[0-9]{1,5})|" + singleContextVarRegexp + "$";
        };

        util.resizeContentAreaAndRepositionDialog = function (contentNode, dialog)
        {
            var viewport = w.getBox();
            var contentDimension = dojo.position(contentNode);
            var dialogDimension = dojo.position(dialog.domNode);
            var dialogTitleAndFooterHeight = dialogDimension.h - contentDimension.h;
            var dialogLeftRightSpaces = dialogDimension.w - contentDimension.w;

            var resize = function ()
            {
                var viewport = w.getBox();
                var width = viewport.w * dialog.maxRatio;
                var height = viewport.h * dialog.maxRatio;
                var dialogDimension = dojo.position(dialog.domNode);

                var maxContentHeight = height - dialogTitleAndFooterHeight;

                // if width style is set on a dialog node, use dialog width
                if (dialog.domNode.style && dialog.domNode.style.width)
                {
                    width = dialogDimension.w;
                }
                var maxContentWidth = width - dialogLeftRightSpaces;
                domStyle.set(contentNode, {
                    "overflow": "auto",
                    maxHeight: maxContentHeight + "px",
                    maxWidth: maxContentWidth + "px"
                });

                var dialogX = viewport.w / 2 - dialogDimension.w / 2;
                var dialogY = viewport.h / 2 - dialogDimension.h / 2;
                domStyle.set(dialog.domNode, {
                    top: dialogY + "px",
                    left: dialogX + "px"
                });
                dialog.resize();
            };
            resize();
            on(window, "resize", resize);
        };

        var _loadData = function (promisesObject, callback)
        {
            all(promisesObject)
                .then(function (data)
                {
                    callback({
                        actual: data.actual,
                        inheritedActual: data.inheritedActual,
                        effective: data.effective
                    });
                });
        };

        util.loadData = function (management, modelObj, callback, requestOptions)
        {
            var request = lang.mixin({depth: 0}, requestOptions);

            var inheritedEffectivePromise = management.load(modelObj, lang.mixin(lang.clone(request), {excludeInheritedContext: false, actuals: false}));
            var localActualsPromise = management.load(modelObj, lang.mixin(lang.clone(request), {excludeInheritedContext: true, actuals: true}));
            var inheritedActualsPromise = management.load(modelObj, lang.mixin(lang.clone(request), {
                actuals: true,
                excludeInheritedContext: false
            }));
            _loadData({
                actual: localActualsPromise,
                inheritedActual: inheritedActualsPromise,
                effective: inheritedEffectivePromise
            }, callback);
        };

        util.loadEffectiveAndInheritedActualData = function (management, modelObj, callback, requestOptions)
        {
            var request = lang.mixin({depth: 0}, requestOptions);

            var effectiveResponsePromise = management.load(modelObj, lang.mixin(lang.clone(request), {excludeInheritedContext: false}));
            var inheritedActualResponsePromise = management.load(modelObj, lang.mixin(lang.clone(request), {
                actuals: true,
                excludeInheritedContext: false
            }));
            var deferred = new Deferred();
            deferred.resolve([{}]);
            var actualResponsePromise = deferred.promise;
            _loadData({
                actual: actualResponsePromise,
                inheritedActual: inheritedActualResponsePromise,
                effective: effectiveResponsePromise
            }, callback);
        };

        util.initialiseFields = function (data, containerNode, metadata, category, type)
        {
            var attributes = metadata.getMetaData(category, type).attributes;

            var widgets = registry.findWidgets(containerNode);
            array.forEach(widgets, function (item)
            {
                var widgetName = item.name;
                if (widgetName in attributes)
                {
                    var attribute = attributes[widgetName];
                    var value = data[widgetName];
                    if (value)
                    {
                        if (item instanceof dijit.form.CheckBox)
                        {
                            item.set("checked", value);
                        }
                        else
                        {
                            var copy = lang.clone(value);
                            if (attribute.secure && /^\*+/.test(value))
                            {
                                item.set("required", false);
                                item.set("placeHolder", copy);
                            }
                            else
                            {
                                item.set("value", copy);
                            }
                        }
                    }
                }
            });
        };

        util.abortReaderSafely = function (reader)
        {
            if (reader && reader.readyState > 0)
            {
                try
                {
                    this.reader.abort();
                }
                catch (ex)
                {
                    // Ignore - read no longer in progress
                }
            }
        };

        util.buildCheckboxMarkup = function (val)
        {
            return "<input type='checkbox' disabled='disabled' " + (val ? "checked='checked'" : "") + " />";
        };

        util.makeTypeStoreFromMetadataByCategory = function (metadata, category)
        {
            var supportedTypes = metadata.getTypesForCategory(category);
            supportedTypes.sort();
            return this.makeTypeStore(supportedTypes);
        };

        util.extend = function (childConstructor, parentConstructor)
        {
            var childPrototype = Object.create(parentConstructor.prototype);
            childPrototype.constructor = childConstructor;
            childConstructor.prototype = childPrototype;
            return childConstructor;
        };

        util.generateName = function (obj)
        {
            if (obj)
            {
                var name = obj.type + (obj.type == "broker" ? "" : ":" + obj.name);
                if (obj.parent)
                {
                    name = this.generateName(obj.parent) + "/" + name;
                }
                return name;
            }
            return "";
        };

        util.toFriendlyUserName = function(serializedUserName)
        {
            var atPosition = serializedUserName.indexOf("@");
            if (atPosition !== -1)
            {
                return decodeURIComponent(serializedUserName.substring(0, atPosition));
            }
            return serializedUserName;
        };

        util.stopEventPropagation = function(domNode, eventName)
        {
            on(domNode, eventName, function(evt)
            {
                if (evt)
                {
                    evt.stopPropagation();
                }
            });
        };

        util.updateSyncDStore = function (dstore, data, idProperty) {
            if (data)
            {
                for (var i = 0; i < data.length; i++)
                {
                    dstore.putSync(data[i]);
                }
            }
            var objectsToRemove = [];
            dstore.fetchSync()
                .forEach(function (object) {
                    if (object)
                    {
                        if (data)
                        {
                            for (var i = 0; i < data.length; i++)
                            {
                                if (data[i][idProperty] === object[idProperty])
                                {
                                    return;
                                }
                            }
                        }
                        objectsToRemove.push(object[idProperty]);
                    }
                });
            for (var i = 0 ; i < objectsToRemove.length; i++)
            {
                dstore.removeSync(objectsToRemove[i]);
            }
        };

        util.collectAttributeNodes = function (containerNode, metadata, category, type) {
            var containerObject = {};
            if (metadata && category && type)
            {
                var attributes = metadata.getMetaData(category, type).attributes;
                for (var attrName in attributes)
                {
                    if (attributes.hasOwnProperty(attrName))
                    {
                        var queryResult = query("." + attrName, containerNode);
                        if (queryResult && queryResult[0])
                        {
                            var attr = attributes[attrName];
                            containerObject[attrName] = {
                                containerNode: queryResult[0],
                                attributeType: attr.type
                            };
                        }
                    }
                }
            }
            return containerObject;
        };

        util.updateAttributeNodes = function (containerObject, restData) {
            for (var attrName in containerObject)
            {
                if (containerObject.hasOwnProperty(attrName) && attrName in restData)
                {
                    var content = "";
                    if (containerObject[attrName].attributeType === "Boolean")
                    {
                        content = util.buildCheckboxMarkup(restData[attrName]);
                    }
                    else
                    {
                        content = entities.encode(String(restData[attrName]));
                    }
                    containerObject[attrName].containerNode.innerHTML = content;
                }
            }
        };

        return util;
    });
