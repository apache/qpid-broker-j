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
package org.apache.qpid.server.store.berkeleydb;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;

/**
 * Standalone tool to remove one or more configuration records from a BDB store.
 * Intended for exceptional use only.
 *
 * If targeting a BDB HA store, then it is important to establish which node was
 * most recently master and perform the update there.
 */
public class OrphanConfigurationRecordPurger
{
    private static final String USAGE_STRING =
            "usage: " + (String.format("java %s\n"
                                       + "       -dryRun                  # Dry run mode\n"
                                       + "       -storePath <dir>         # Store path\n"
                                       + "       [-ha                     # HA mode\n"
                                       + "        -nodeName <nodename>    # HA node name\n"
                                       + "        -nodeHost <nodehost>    # HA node host\n"
                                       + "        -groupName <groupName>] # HA group name\n"
                                       + "       -targetUuid <uuid1>      # UUID to delete\n"
                                       + "       [-targetUuid <uuid2>...] # UUID to delete\n",
                                       OrphanConfigurationRecordPurger.class.getName()));

    private static final String VERSION_DB_NAME = "DB_VERSION";

    private static final String CONFIGURED_OBJECTS_DB_NAME = "CONFIGURED_OBJECTS";
    private static final String CONFIGURED_OBJECT_HIERARCHY_DB_NAME = "CONFIGURED_OBJECT_HIERARCHY";

    private static final Set<Integer> ALLOWED_VERSIONS = new HashSet<>(Arrays.asList(8, 9));
    private static final DatabaseConfig READ_ONLY_DB_CONFIG = DatabaseConfig.DEFAULT.setAllowCreate(false).setReadOnly(true).setTransactional(true);
    private static final DatabaseConfig READ_WRITE_DB_CONFIG = READ_ONLY_DB_CONFIG.setReadOnly(false);

    private String _storePath;
    private Set<UUID> _uuids = new HashSet<>();
    private boolean _dryRun;
    private boolean _ha;
    private String _nodeName;
    private String _nodeHost;
    private String _groupName;

    public static void main(String[] argv) throws Exception
    {
        final OrphanConfigurationRecordPurger purger = new OrphanConfigurationRecordPurger();
        purger.parseArgs(argv);
        purger.purge();
    }

    private void purge() throws Exception
    {
        EnvironmentConfig config = EnvironmentConfig.DEFAULT;
        config.setAllowCreate(false);
        config.setTransactional(true);

        try (Environment env = createEnvironment(config))
        {
            final int version = getVersion(env, READ_ONLY_DB_CONFIG);
            if (!ALLOWED_VERSIONS.contains(version))
            {
                throw new IllegalStateException(String.format("Store has unexpected version. Found %d expected %s",
                                                              version,
                                                              ALLOWED_VERSIONS));
            }

            final Transaction tx = env.beginTransaction(null,
                                                        TransactionConfig.DEFAULT.setReadOnly(_dryRun));
            boolean success = false;
            int configChanges = 0;
            try
            {
                for (final UUID uuid : _uuids)
                {
                    configChanges += purgeOrphans(env, tx, uuid);
                }

                success = true;
            }
            finally
            {
                if (!_dryRun && success && configChanges > 0)
                {
                    tx.commit();
                    System.out.format("%d config records(s) and associated hierarchy records purged.", configChanges);
                }
                else
                {
                    System.out.println("No config or config hierarchy records purged.");
                    tx.abort();
                }
            }
        }
    }

    private Environment createEnvironment(final EnvironmentConfig config) throws Exception
    {
        final Environment env;
        if (_ha)
        {
            final ReplicationConfig repConfig = (ReplicationConfig) ReplicationConfig.DEFAULT
                    .setNodeHostPort(_nodeHost)
                    .setGroupName(_groupName)
                    .setNodeName(_nodeName)
                    .setDesignatedPrimary(true)
                    .setElectableGroupSizeOverride(1);

            env = new ReplicatedEnvironment(new File(_storePath), repConfig, config);
        }
        else
        {
            env = new Environment(new File(_storePath), config);
        }
        return env;
    }

    private int getVersion(final Environment env, final DatabaseConfig dbConfig)
    {
        try (Database versionDb = env.openDatabase(null, VERSION_DB_NAME, dbConfig);
             Cursor cursor = versionDb.openCursor(null, null))
        {

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();

            int version = 0;

            while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS)
            {
                int ver = IntegerBinding.entryToInt(key);
                if (ver > version)
                {
                    version = ver;
                }
            }

            return version;
        }
    }

    private int purgeOrphans(Environment env, final Transaction tx, UUID uuid) throws Exception
    {
        try(Database configDb = env.openDatabase(tx, CONFIGURED_OBJECTS_DB_NAME, READ_WRITE_DB_CONFIG))
        {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();

            TupleOutput output = new TupleOutput();
            output.writeLong(uuid.getMostSignificantBits());
            output.writeLong(uuid.getLeastSignificantBits());
            TupleBase.outputToEntry(output, key);

            OperationStatus status =
                    _dryRun ? configDb.get(tx, key, value, LockMode.DEFAULT) : configDb.delete(tx, key);

            if (status == OperationStatus.SUCCESS)
            {
                System.out.format("Config record for UUID %s found\n", uuid);

                try (Database hierarchyDb = env.openDatabase(null, CONFIGURED_OBJECT_HIERARCHY_DB_NAME,
                                                             READ_WRITE_DB_CONFIG);
                     Cursor hierarchyCursor = hierarchyDb.openCursor(tx, null))
                {

                    DatabaseEntry hkey = new DatabaseEntry();
                    DatabaseEntry hvalue = new DatabaseEntry();

                    int count = 0;
                    while (hierarchyCursor.getNext(hkey, hvalue, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                    {
                        TupleInput dis = new TupleInput(hkey.getData());
                        final long mostSigBits = dis.readLong();
                        final long leastSigBits = dis.readLong();
                        final UUID recId = new UUID(mostSigBits, leastSigBits);
                        if (recId.equals(uuid))
                        {
                            if (!_dryRun)
                            {
                                hierarchyCursor.delete();
                            }
                            count++;
                        }
                    }

                    System.out.format("%d config hierarchy record(s) found\n", count);
                    return 1;
                }
            }
            else
            {
                System.out.format("Config object record for UUID %s NOT found\n", uuid);
                return 0;
            }
        }
    }

    private void parseArgs(final String[] argv)
    {
        final int argCount = argv.length;

        if (argCount == 0)
        {
            printUsage(null);
        }

        int argc = 0;
        while (argc < argCount)
        {
            String thisArg = argv[argc++];
            switch (thisArg)
            {
                case "-storePath":
                    if (argc < argCount)
                    {
                        _storePath = argv[argc++];
                    }
                    else
                    {
                        printUsage("-storePath requires an argument");
                    }
                    break;
                case "-targetUuid":
                    if (argc < argCount)
                    {
                        String uuid = argv[argc++];
                        _uuids.add(UUID.fromString(uuid));
                    }
                    else
                    {
                        printUsage("-targetUuid requires an argument");
                    }
                break;
                case "-ha":
                    _ha = true;
                    break;
                case "-nodeName":
                if (argc < argCount)
                    {
                        _nodeName = argv[argc++];
                    }
                    else
                    {
                        printUsage("-nodeName requires an argument");
                    }
                break;
                case "-nodeHost":
                if (argc < argCount)
                    {
                        _nodeHost = argv[argc++];
                    }
                    else
                    {
                        printUsage("-nodeHost requires an argument");
                    }
                break;
                case "-groupName":
                if (argc < argCount)
                    {
                        _groupName = argv[argc++];
                    }
                    else
                    {
                        printUsage("-groupName requires an argument");
                    }
                break;
                case "-dryRun":
                _dryRun = true;
                break;
                default:
                    printUsage(thisArg + " is not a valid argument");
                    break;
            }
        }

        if (_storePath == null)
        {
            printUsage("-storePath is a required argument");
        }
        if (_uuids.isEmpty())
        {
            printUsage("-targetUuid is a required argument");
        }
        if (_ha)
        {
            if (_nodeName == null)
            {
                printUsage("-nodeName is a required argument when in ha mode");
            }
            if (_nodeHost == null)
            {
                printUsage("-nodeHost is a required argument when in ha mode");
            }
            if (_groupName == null)
            {
                printUsage("-groupName is a required argument when in ha mode");
            }
        }
    }

    private void printUsage(String msg)
    {
        if (msg != null)
        {
            System.err.println(msg);
        }

        System.err.println(USAGE_STRING);
        System.exit(-1);
    }

}
