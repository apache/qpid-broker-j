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
define(["dojo/_base/lang",
        "dojo/Deferred",
        "dojo/date",
        "dojo/date/locale",
        "dojo/number"], function (lang, Deferred, date, locale, number)
{
    var timeZonePreferenceName = "Time Zone";
    var updatePeriodPreferenceName = "Update Period";
    var tabsPreferenceName = "Tabs";
    var defaultPreferences = [
        {name: timeZonePreferenceName, type: "X-TimeZone", value: {timeZone: "UTC"} },
        {name: updatePeriodPreferenceName, type: "X-UpdatePeriod", value: {updatePeriod: 5} },
        {name: tabsPreferenceName, type: "X-Tabs", value: {tabs: []} }
    ];
    function UserPreferences(management)
    {
        this.timeZonePreferenceName = timeZonePreferenceName;
        this.updatePeriodPreferenceName = updatePeriodPreferenceName;
        this.tabsPreferenceName = tabsPreferenceName;
        this.preferences = {};
        for(var i = 0; i< defaultPreferences.length; i++)
        {
            var preference = defaultPreferences[i];
            this.preferences[preference.name] = preference;
        }
        this.management = management;
        this.listeners = {};
    }

    UserPreferences.prototype.load = function ()
    {
        var deferred = new Deferred();
        management.getUserPreferences({type: "broker"}).then(lang.hitch(this, function (preferences)
        {
            this._init(preferences);
            deferred.resolve(preferences);
        }),lang.hitch(this, function (error)
        {
            this.lastError = error;
            deferred.reject(error);
        }));
        return deferred.promise;
    };

    UserPreferences.prototype.getLastError = function ()
    {
        return this.lastError;
    };

    UserPreferences.prototype._init= function(preferences)
    {
        var timezone = preferences["X-TimeZone"];
        if (timezone && timezone[0])
        {
            this.preferences[timeZonePreferenceName] = timezone[0];
        }
        var tabs = preferences["X-Tabs"];
        if (tabs && tabs[0])
        {
            this.preferences[tabsPreferenceName] = tabs[0];
        }
        var updatePeriod = preferences["X-UpdatePeriod"];
        if (updatePeriod && updatePeriod[0])
        {
            this.preferences[updatePeriodPreferenceName] = updatePeriod[0];
        }
    };

    UserPreferences.prototype.save = function (preferences)
    {
        var deferred = new Deferred();
        var result = this.management.savePreferences({type: "broker"}, preferences);
        result.then(lang.hitch(this, function ()
        {
            deferred.resolve(preferences);
            this._notifyListeners(preferences);
        }), lang.hitch(this, function (error)
        {
            this.lastError = error;
            deferred.reject(error);
        }));
        return deferred.promise;
    };

    UserPreferences.prototype.addListener = function (listener, preferenceName)
    {
        var preference = this.preferences[preferenceName];
        if (preference)
        {
            var preferenceListeners = this.listeners[preferenceName];
            if (!preferenceListeners)
            {
                preferenceListeners = [];
                this.listeners[preferenceName] = preferenceListeners;
            }
            preferenceListeners.push(listener);
            this._notifyListener(listener, preference);
        }
        else
        {
            throw new Error("Unsupported preference '" + preferenceName + "'");
        }
    };

    UserPreferences.prototype.removeListener = function (obj)
    {
        for (var i = 0; i < this.listeners.length; i++)
        {
            if (this.listeners[i] === obj)
            {
                this.listeners.splice(i, 1);
                return;
            }
        }
    };

    UserPreferences.prototype._notifyListeners = function (preferences)
    {
        for( var type in preferences)
        {
            var typeList = preferences[type];
            for(var i = 0; i < typeList.length; i++)
            {
                var preference = typeList[i];
                var name = preference.name;
                var listenerList = this.listeners[name];
                if (listenerList)
                {
                    for(var j=0; j < listenerList.length; j++)
                    {
                        this._notifyListener(listenerList[j], preference);
                    }
                }
            }
        }
    };

    UserPreferences.prototype._notifyListener = function (listener, preference)
    {
        try
        {
            listener.onPreferenceChange(preference);
        }
        catch (e)
        {
            if (console && console.warn)
            {
                console.warn(e);
            }
        }
    };

    UserPreferences.prototype.getPreferenceByName= function(preferenceName)
    {
        return this.preferences[preferenceName];
    };

    UserPreferences.prototype.getTimeZoneInfo = function (timeZoneName)
    {
        var timeZonePreference = this.getPreferenceByName(timeZonePreferenceName);
        if (!timeZoneName && timeZonePreference.value && timeZonePreference.value.timeZone)
        {
            timeZoneName = timeZonePreference.value.timeZone;
        }

        if (!timeZoneName)
        {
            return null;
        }

        return this.management.timezone.getTimeZoneInfo(timeZoneName);
    };

    UserPreferences.prototype.addTimeZoneOffsetToUTC = function (utcTimeInMilliseconds, timeZone)
    {
        var tzi = null;
        if (timeZone && timeZone.hasOwnProperty("offset"))
        {
            tzi = timeZone;
        }
        else
        {
            tzi = this.getTimeZoneInfo(timeZone);
        }

        if (tzi)
        {
            var browserTimeZoneOffsetInMinutes = -new Date().getTimezoneOffset();
            return utcTimeInMilliseconds + ( tzi.offset - browserTimeZoneOffsetInMinutes ) * 60000;
        }
        return utcTimeInMilliseconds;
    };

    UserPreferences.prototype.getTimeZoneDescription = function (timeZone)
    {
        var tzi = null;
        if (timeZone && timeZone.hasOwnProperty("offset"))
        {
            tzi = timeZone;
        }
        else
        {
            tzi = this.getTimeZoneInfo(timeZone);
        }

        if (tzi)
        {
            var timeZoneOfsetInMinutes = tzi.offset;
            return (timeZoneOfsetInMinutes > 0 ? "+" : "") + number.format(timeZoneOfsetInMinutes / 60, {pattern: "00"})
                   + ":" + number.format(timeZoneOfsetInMinutes % 60, {pattern: "00"}) + " " + tzi.name;
        }
        return date.getTimezoneName(new Date());
    };

    UserPreferences.prototype.formatDateTime = function (utcTimeInMilliseconds, options)
    {
        var dateTimeOptions = options || {};
        var tzi = this.getTimeZoneInfo(dateTimeOptions.timeZoneName);
        var timeInMilliseconds = utcTimeInMilliseconds;

        if (tzi && dateTimeOptions.addOffset)
        {
            timeInMilliseconds = this.addTimeZoneOffsetToUTC(utcTimeInMilliseconds, tzi);
        }

        var d = new Date(timeInMilliseconds);

        var formatOptions = {
            datePattern: dateTimeOptions.datePattern || "yyyy-MM-dd",
            timePattern: dateTimeOptions.timePattern || "HH:mm:ss.SSS"
        };

        if ("date" == dateTimeOptions.selector)
        {
            formatOptions.selector = "date";
        }
        else if ("time" == dateTimeOptions.selector)
        {
            formatOptions.selector = "time";
        }

        var result = locale.format(d, formatOptions);
        if (dateTimeOptions.appendTimeZone)
        {
            result += " (" + this.getTimeZoneDescription(tzi) + ")";
        }
        return result;
    };

    UserPreferences.prototype.appendTab = function (tab)
    {
        if (!this.isTabStored(tab))
        {
            var tabsPreference = this.getPreferenceByName(this.tabsPreferenceName);
            var tabs;
            if (tabsPreference.value && tabsPreference.value.tabs)
            {
                tabs = tabsPreference.value.tabs;
            }
            else
            {
                tabs = [];
                tabsPreference.value = {"tabs": tabs};
            }
            var storedTab = {tabType: tab.tabType};
            if (tab.modelObject && tab.modelObject.id)
            {
                storedTab.configuredObjectId = tab.modelObject.id;
            }
            if (tab.preferenceId)
            {
                storedTab.preferenceId = tab.preferenceId;
            }
            tabs.push(storedTab);
            this._savePreference(tabsPreference);
        }
    };

    UserPreferences.prototype.removeTab = function (tab)
    {
        if (this.isTabStored(tab))
        {
            var index = this._getTabIndex(tab);
            if (index != -1)
            {
                var tabsPreference = this.getPreferenceByName(this.tabsPreferenceName);
                tabsPreference.value.tabs.splice(index, 1);
                this._savePreference(tabsPreference);
            }
        }
    };

    UserPreferences.prototype._savePreference = function (preference)
    {
        var preferences = {};
        preferences[preference.type] = [preference];
        var result = this.save(preferences);
        if (!preference.id)
        {
            result.then(lang.hitch(this, this.load));
        }
    };

    UserPreferences.prototype.isTabStored = function (tab)
    {
        return this._getTabIndex(tab) != -1;
    };

    UserPreferences.prototype._getTabIndex = function (tab)
    {
        var index = -1;
        var tabsPreference = this.getPreferenceByName(this.tabsPreferenceName);
        if (tabsPreference && tabsPreference.value && tabsPreference.value.tabs)
        {
            var savedTabs = tabsPreference.value.tabs;
            for (var i = 0; i < savedTabs.length; i++)
            {
                var savedTab = savedTabs[i];
                if (savedTab.tabType === tab.tabType
                    && ((!savedTab.configuredObjectId && !tab.modelObject) || (savedTab.configuredObjectId && tab.modelObject && savedTab.configuredObjectId === tab.modelObject.id))
                    && (!tab.preferenceId && !savedTab.preferenceId || tab.preferenceId === savedTab.preferenceId ))
                {
                    index = i;
                    break;
                }
            }
        }
        return index;
    };

    UserPreferences.prototype.getSavedTabs = function ()
    {
        var tabsPreference = this.getPreferenceByName(this.tabsPreferenceName);
        if (tabsPreference && tabsPreference.value && tabsPreference.value.tabs)
        {
            return lang.clone(tabsPreference.value.tabs);
        }
        return [];
    };

    var preferencesDialog = null, userPreferenceForm = null;
    UserPreferences.prototype.showEditor = function ()
    {
        this.load().then(lang.hitch(this, function ()
            {
                var tzp = this.getPreferenceByName(timeZonePreferenceName);
                var upp = this.getPreferenceByName(updatePeriodPreferenceName);
                if (preferencesDialog)
                {
                    userPreferenceForm.set("timezone", tzp.value ? tzp.value.timeZone : "UTC");
                    userPreferenceForm.set("updatePeriod", upp.value ? upp.value.updatePeriod : 5, false);
                    preferencesDialog.show();
                }
                else
                {
                    require(["dojo/_base/lang", "qpid/management/userpreference/UserPreferenceForm", "dijit/Dialog"],
                        lang.hitch(this, function (lang, UserPreferenceForm, Dialog)
                        {
                            userPreferenceForm = new UserPreferenceForm({});
                            preferencesDialog = new Dialog({title: "User preferences", content: userPreferenceForm});
                            userPreferenceForm.set("timezones", this.management.timezone.getAllTimeZones());
                            userPreferenceForm.set("timezone", tzp.value ? tzp.value.timeZone : "UTC");
                            userPreferenceForm.set("updatePeriod", upp.value ? upp.value.updatePeriod : 5, false);
                            userPreferenceForm.on("cancel", lang.hitch(this, function ()
                            {
                                preferencesDialog.hide();
                            }));
                            userPreferenceForm.on("save", lang.hitch(this, function (event)
                            {
                                var timeZonePreference = this.getPreferenceByName(timeZonePreferenceName);
                                var updatePeriodPreference = this.getPreferenceByName(updatePeriodPreferenceName);
                                var preferences = {};
                                if (event.preferences.timeZone)
                                {
                                    timeZonePreference.value = {timeZone: event.preferences.timeZone};
                                    preferences[timeZonePreference.type] = [timeZonePreference];
                                }
                                if (event.preferences.updatePeriod)
                                {
                                    updatePeriodPreference.value = {updatePeriod: event.preferences.updatePeriod};
                                    preferences[updatePeriodPreference.type] = [updatePeriodPreference];
                                }

                                this.save(preferences)
                                    .then(lang.hitch(this, function ()
                                    {
                                        preferencesDialog.hide();
                                    }));
                            }));
                            preferencesDialog.show();
                        }));
                }
            }));
    };

    return UserPreferences;
});
