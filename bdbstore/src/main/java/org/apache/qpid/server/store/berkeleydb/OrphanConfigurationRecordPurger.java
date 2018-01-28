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
                                       + "       -dryRun                   # Dry run mode\n"
                                       + "       -parentRootCategory <dir> # Parent root category\n"
                                       + "       -storePath <dir>          # Store path\n"
                                       + "       [-ha                      # HA mode\n"
                                       + "        -nodeName <nodename>     # HA node name\n"
                                       + "        -nodeHost <nodehost>     # HA node host\n"
                                       + "        -groupName <groupName>]  # HA group name\n",
                                       OrphanConfigurationRecordPurger.class.getName()));

    private static final String VERSION_DB_NAME = "DB_VERSION";

    private static final String CONFIGURED_OBJECTS_DB_NAME = "CONFIGURED_OBJECTS";
    private static final String CONFIGURED_OBJECT_HIERARCHY_DB_NAME = "CONFIGURED_OBJECT_HIERARCHY";

    private static final Set<Integer> ALLOWED_VERSIONS = new HashSet<>(Arrays.asList(8, 9));
    private static final DatabaseConfig READ_ONLY_DB_CONFIG = DatabaseConfig.DEFAULT.setAllowCreate(false).setReadOnly(true).setTransactional(true);
    private static final DatabaseConfig READ_WRITE_DB_CONFIG = READ_ONLY_DB_CONFIG.setReadOnly(false);

    private String _parentRootCategory;
    private String _storePath;
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
                                                              version, ALLOWED_VERSIONS));
            }

            final Transaction tx = env.beginTransaction(null, TransactionConfig.DEFAULT);
            boolean success = false;
            try
            {
                purgeOrphans(env, tx);
                success = true;
            }
            finally
            {
                if (!success)
                {
                    System.out.println("No config or config hierarchy records purged.");
                    tx.abort();
                }
                else if (_dryRun)
                {
                    System.out.println("No config or config hierarchy records purged - -dryRun flag specified.");
                    tx.abort();
                }
                else
                {
                    tx.commit();
                    System.out.format("Config records(s) and associated hierarchy records purged.");
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

    private void purgeOrphans(Environment env, final Transaction tx) throws Exception
    {
        try(Database configDb = env.openDatabase(tx, CONFIGURED_OBJECTS_DB_NAME, READ_WRITE_DB_CONFIG))
        {

            final Set<UUID> records = new HashSet<>();

            try (Cursor configCursor = configDb.openCursor(tx, null))
            {
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry value = new DatabaseEntry();

                while (configCursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                {
                    final UUID recId = entryToUuid(new TupleInput(key.getData()));
                    records.add(recId);
                }
            }

            int configRecordDeleted = 0;
            int configHierarchyRecordsDeleted = 0;

            try (Database hierarchyDb = env.openDatabase(null, CONFIGURED_OBJECT_HIERARCHY_DB_NAME,
                                                         READ_WRITE_DB_CONFIG))
            {
                boolean loopAgain;
                do
                {
                    loopAgain = false;
                    try (Cursor hierarchyCursor = hierarchyDb.openCursor(tx, null))
                    {

                        DatabaseEntry key = new DatabaseEntry();
                        DatabaseEntry value = new DatabaseEntry();

                        boolean parentReferencingRecordFound = false;
                        while (hierarchyCursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                        {
                            final TupleInput keyInput = new TupleInput(key.getData());
                            final UUID childId = entryToUuid(keyInput);
                            final String parentType = keyInput.readString();
                            final UUID parentId = entryToUuid(new TupleInput(value.getData()));

                            if (_parentRootCategory.equals(parentType))
                            {
                                parentReferencingRecordFound = true;
                            }
                            else if (!records.contains(parentId))
                            {
                                System.out.format("Orphan UUID : %s (has unknown parent with UUID %s of type %s)\n",
                                                  childId, parentId, parentType);
                                hierarchyCursor.delete();
                                configHierarchyRecordsDeleted++;
                                loopAgain = true;

                                DatabaseEntry uuidKey = new DatabaseEntry();
                                final TupleOutput tupleOutput = uuidToKey(childId);
                                TupleBase.outputToEntry(tupleOutput, uuidKey);

                                final OperationStatus delete = configDb.delete(tx, uuidKey);
                                if (delete == OperationStatus.SUCCESS)
                                {
                                    records.remove(childId);
                                    configRecordDeleted++;
                                }
                            }
                        }

                        if (!parentReferencingRecordFound)
                        {
                            throw new IllegalStateException(String.format(
                                    "No hierarchy record found with root category type (%s)."
                                    + " Cannot modify store.", _parentRootCategory));
                        }
                    }
                }
                while(loopAgain);

                System.out.format("Identified %d orphaned configured object record(s) "
                                  + "and %d hierarchy records for purging\n",
                                   configRecordDeleted, configHierarchyRecordsDeleted);
            }
        }
    }

    private TupleOutput uuidToKey(final UUID uuid)
    {
        DatabaseEntry key = new DatabaseEntry();
        TupleOutput output = new TupleOutput();
        output.writeLong(uuid.getMostSignificantBits());
        output.writeLong(uuid.getLeastSignificantBits());
        return output;
    }

    private UUID entryToUuid(final TupleInput input)
    {
        final long mostSigBits = input.readLong();
        final long leastSigBits = input.readLong();
        return new UUID(mostSigBits, leastSigBits);
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
                case "-parentRootCategory":
                    if (argc < argCount)
                    {
                        _parentRootCategory = argv[argc++];
                    }
                    else
                    {
                        printUsage("-parentRootCategory requires an argument");
                    }
                    break;
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

        if (_parentRootCategory == null)
        {
            printUsage("-parentRootCategory is a required argument");
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
