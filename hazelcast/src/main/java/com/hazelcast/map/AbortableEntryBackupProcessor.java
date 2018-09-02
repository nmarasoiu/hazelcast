package com.hazelcast.map;

/**
 * This {@link EntryBackupProcessor} declares that when multiple entries are processed with this EBP,
 * and if/when the master node for the partition crashes, then it is ok to aborting processing for
 * the remainder of the keys (we wait for current one), so that the failover can happen faster.
 * Maybe a config can be used alternatively to the existence of this interface?
 *
 */
public interface AbortableEntryBackupProcessor<K, V> extends EntryBackupProcessor<K, V> {
}
