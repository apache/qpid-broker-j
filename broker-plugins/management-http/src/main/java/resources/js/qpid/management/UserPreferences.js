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
define(["dojo/date",
        "dojo/date/locale",
        "dojo/number"], function (date, locale, number) {

  function UserPreferences(management)
  {
      this.listeners = [];
      /* set time zone to 'UTC' by default*/
      this.timeZone = "UTC";
      this.tabs = [];
      this.management = management;
  }

  UserPreferences.prototype.load = function(successCallback, failureCallback)
  {
      var that = this;
      this.management.get({url: "service/preferences"},
                       function(preferences)
                       {
                          that.preferences = preferences;
                          for(var name in preferences)
                          {
                              that[name] = preferences[name];
                          }
                          if (successCallback)
                          {
                              successCallback();
                          }
                       },
                       function(error)
                       {
                        that.preferencesError = error;
                        if (failureCallback)
                        {
                            failureCallback();
                        }
                       });
  }

  UserPreferences.prototype.save = function(preferences, successCallback, failureCallback)
  {
      var that = this;
      this.management.post({url: "service/preferences"},
                          preferences,
                          function(x)
                          {
                            that.preferences = preferences;
                            for(var name in preferences)
                            {
                              if (preferences.hasOwnProperty(name))
                              {
                                  that[name] = preferences[name];
                              }
                            }
                            that._notifyListeners(preferences);
                            if (successCallback)
                            {
                              successCallback(preferences);
                            }
                          },
                          failureCallback);
  };

  var fields = ["preferencesError", "management", "listeners"];
  UserPreferences.prototype.resetPreferences = function()
  {
      var preferences = {};
      for(var name in this)
      {
          if (this.hasOwnProperty(name) && typeof this[name] != "function")
          {
              if (fields.indexOf(name) != -1)
              {
                  continue;
              }
              this[name] = null;
              preferences[name] = undefined;
              delete preferences[name];
          }
      }
      this.timeZone = "UTC";
      this.preferences ="UTC";
      this.preferences = preferences;
      this._notifyListeners(preferences);
  };

  UserPreferences.prototype.addListener = function(obj)
  {
    this.listeners.push(obj);
    this._notifyListener(obj, this.preferences);
  };

  UserPreferences.prototype.removeListener = function(obj)
  {
    for(var i = 0; i < this.listeners.length; i++)
    {
      if(this.listeners[i] === obj)
      {
        this.listeners.splice(i,1);
        return;
      }
    }
  };

  UserPreferences.prototype._notifyListeners = function(preferences)
  {
    for(var i = 0; i < this.listeners.length; i++)
    {
      this._notifyListener(this.listeners[i], preferences);
    }
  };

  UserPreferences.prototype._notifyListener = function(listener,preferences)
  {
     try
     {
       listener.onPreferencesChange(preferences);
     }
     catch(e)
     {
       if (console && console.warn)
       {
         console.warn(e);
       }
     }
  };

  UserPreferences.prototype.getTimeZoneInfo = function(timeZoneName)
  {
    if (!timeZoneName && this.timeZone)
    {
      timeZoneName = this.timeZone;
    }

    if (!timeZoneName)
    {
      return null;
    }

    return this.management.timezone.getTimeZoneInfo(timeZoneName);
  };

  UserPreferences.prototype.addTimeZoneOffsetToUTC = function(utcTimeInMilliseconds, timeZone)
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

  UserPreferences.prototype.getTimeZoneDescription = function(timeZone)
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
      return (timeZoneOfsetInMinutes>0? "+" : "")
        + number.format(timeZoneOfsetInMinutes/60, {pattern: "00"})
        + ":" + number.format(timeZoneOfsetInMinutes%60, {pattern: "00"})
        + " " + tzi.name;
    }
    return date.getTimezoneName(new Date());
  };

  UserPreferences.prototype.formatDateTime = function(utcTimeInMilliseconds, options)
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
    if(dateTimeOptions.appendTimeZone)
    {
      result += " (" + this.getTimeZoneDescription(tzi) + ")";
    }
    return result;
  };

  UserPreferences.prototype.appendTab = function(tab)
  {
    if (!this.tabs)
    {
      this.tabs = [];
    }
    if (!this.isTabStored(tab))
    {
      this.tabs.push(tab);
      this.save({tabs: this.tabs});
    }
  };

  UserPreferences.prototype.removeTab = function(tab)
  {
    if (this.tabs)
    {
      var index = this._getTabIndex(tab);
      if (index != -1)
      {
        this.tabs.splice(index, 1);
        this.save({tabs: this.tabs});
      }
    }
  };

  UserPreferences.prototype.isTabStored = function(tab)
  {
    return this._getTabIndex(tab) != -1;
  };

  UserPreferences.prototype._getTabIndex = function(tab)
  {
    var index = -1;
    if (this.tabs)
    {
      for(var i = 0 ; i < this.tabs.length ; i++)
      {
        var t = this.tabs[i];
        if ( t.objectId == tab.objectId && t.objectType == tab.objectType )
        {
          index = i;
          break;
        }
      }
    }
    return index;
  }

  return UserPreferences;
});