/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.*;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.AbortableEntryBackupProcessor;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides common backup operation functionality for {@link com.hazelcast.map.EntryProcessor}
 * that can run on multiple entries.
 */
abstract class AbstractMultipleEntryBackupOperation extends MapOperation implements Versioned, HazelcastInstanceAware {
    private static volatile AtomicBoolean addedMigrationListener = new AtomicBoolean();
    EntryBackupProcessor backupProcessor;
    protected PartitionService partitionService;
    protected HazelcastInstance hazelcastInstance;

    public AbstractMultipleEntryBackupOperation() {
    }

    public AbstractMultipleEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        partitionService = hazelcastInstance.getPartitionService();
        this.hazelcastInstance = hazelcastInstance;

        if (addedMigrationListener.compareAndSet(false, true)) {
            System.out.println("Adding listener to migrations");
            //still to be put in config per node lifecycle before starting with config, so we have single listener in cluster if we used it
            hazelcastInstance.getPartitionService().addMigrationListener(new MigrationListener() {
                @Override
                public void migrationStarted(MigrationEvent migrationEvent) {
                    System.out.println("migrationStarted(" + migrationEvent);
                }

                @Override
                public void migrationCompleted(MigrationEvent migrationEvent) {
                    System.out.println("migrationComplete(" + migrationEvent);
                }

                @Override
                public void migrationFailed(MigrationEvent migrationEvent) {
                    System.out.println("migrationFailed(" + migrationEvent);
                }
            });
        }
    }

    protected Predicate getPredicate() {
        return null;
    }

    class OwnerDownDetector {
        int i = 0;
        Partition partition = null;

        boolean isOwnerDown(Object key) {
            boolean ownerDownForPartition = false;
            if (backupProcessor instanceof AbortableEntryBackupProcessor && (i++) % 100 == 0) {
                if (partition == null) {
                    partition = partitionService.getPartition(key);
                }
                if (partition.getOwner() == null) {
                    System.out.println("Breaking from loop");
                    ownerDownForPartition = true;
                }
            }
            return ownerDownForPartition;
        }
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // RU_COMPAT_3_9
        if (out.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            out.writeInt(0);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // RU_COMPAT_3_9
        if (in.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            final int size = in.readInt();
            for (int i = 0; i < size; i++) {
                // key
                in.readData();
                // value
                in.readData();
                // event type
                in.readInt();
            }
        }
    }
}