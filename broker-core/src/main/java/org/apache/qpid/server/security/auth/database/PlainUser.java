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
package org.apache.qpid.server.security.auth.database;

import org.apache.qpid.server.model.AuthenticationProvider;

public class PlainUser implements PasswordPrincipal
{
    private static final long serialVersionUID = 1L;

    private final AuthenticationProvider<?> _authenticationProvider;
    private String _name;
    private char[] _password;
    private boolean _modified = false;
    private boolean _deleted = false;

    PlainUser(String[] data, final AuthenticationProvider<?> authenticationProvider)
    {
        if (data.length != 2)
        {
            throw new IllegalArgumentException("User Data should be length 2, username, password");
        }

        _name = data[0];

        _password = data[1].toCharArray();
        _authenticationProvider = authenticationProvider;
    }

    public PlainUser(String name, char[] password, final AuthenticationProvider<?> authenticationProvider)
    {
        _name = name;
        _password = password;
        _authenticationProvider = authenticationProvider;
        _modified = true;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public String toString()
    {
        return _name;
    }

    @Override
    public char[] getPassword()
    {
        return _password;
    }
    
    @Override
    public byte[] getEncodedPassword()
    {
        byte[] byteArray = new byte[_password.length];
        int index = 0;
        for (char c : _password)
        {
            byteArray[index++] = (byte) c;
        }
        return byteArray;
    }



    @Override
    public void restorePassword(char[] password)
    {
        setPassword(password);
    }

    @Override
    public void setPassword(char[] password)
    {
        _password = password;
        _modified = true;
    }

    @Override
    public boolean isModified()
    {
        return _modified;
    }

    @Override
    public boolean isDeleted()
    {
        return _deleted;
    }

    @Override
    public void delete()
    {
        _deleted = true;
    }

    @Override
    public void saved()
    {
        _modified = false;
    }

}
