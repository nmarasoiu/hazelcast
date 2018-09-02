package com.hazelcast.cache.entryprocessor;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.AbortableEntryBackupProcessor;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Thread.sleep;

public class AbortingEntryBackupProcessorTest {
    private static final Random random = new Random();

    @Test
    public void checkTimeFromMasterCrashTillProcessorReturnsWhenAborting() {
        checkTimeFromMAsterCrashTillProcessorReturns(true);
    }

    @Test
    public void checkTimeFromMasterCrashTillProcessorReturnsWhenNotAborting() {
        checkTimeFromMAsterCrashTillProcessorReturns(false);
    }

    private void checkTimeFromMAsterCrashTillProcessorReturns(final boolean isAborting) {
        final HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(getConfig());
        final HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(getConfig());
        hazelcastInstance1.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                System.out.println(event.getState());
                Set<Member> members = hazelcastInstance2.getCluster().getMembers();
                System.out.println(members);
            }
        });
        System.out.println("waiting for the join");
        Set<Member> members;
        do {
            members = hazelcastInstance2.getCluster().getMembers();
        } while (members.size() < 2);
        System.out.println("ok.cluster is " + members.size() + "; it took " + (System.nanoTime() - 1) / 1000 / 1000 + "ms");
        final IMap<Integer, Integer> map = hazelcastInstance2.getMap("map1");
        insertValues(map);
        Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
            @Override
            public void run() {
                int membersCountBefore = hazelcastInstance2.getCluster().getMembers().size();
                System.out.println("terminating");
                hazelcastInstance1.getLifecycleService().terminate();
                System.out.println("terminated");
                System.out.println("waiting while there are still " + membersCountBefore + " members ");
                long t1 = System.nanoTime();
                while (hazelcastInstance2.getCluster().getMembers().size() == membersCountBefore) ;
                System.out.println(System.nanoTime() - t1);
                System.out.println("ok! remaining " + hazelcastInstance2.getCluster().getMembers().size() + " members");
            }
        }, 500, TimeUnit.MILLISECONDS);
        int parallelism = 10;
        Future[] futures = new Future[parallelism];
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < parallelism; i++) {
            futures[i] = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    long t1 = System.nanoTime();
//                    System.out.println("Executing abortable executeOnEntries");
                    try {
                        map.executeOnEntries(new EP(isAborting, hazelcastInstance2.getLocalEndpoint().getUuid()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
//                    System.out.println("\nTime for abortable=" + isAborting + ": " + (System.nanoTime() - t1) / 1000 / 1000);
                }
            });
        }
        System.out.println("waiting for results");
        for (Future future : futures) {
            try {
                Object o = future.get();
//                if (o != null) System.out.print(".");
//                else System.out.print("?");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof HazelcastInstanceNotActiveException) {
                    System.out.println("HazelInstNotActive, lets see members");
                    members = hazelcastInstance2.getCluster().getMembers();
                    System.out.println(members);
                }
                e.printStackTrace();
            }
        }
        System.out.println("Results received");
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void insertValues(IMap<Integer, Integer> map) {
        System.out.println("Inserting values");
        Random random = new Random();
        Map<Integer, Integer> m1 = new HashMap<Integer, Integer>();
        for (int i = 0; i < 101000; i++) {
            m1.put(random.nextInt(), i);
        }
        map.putAll(m1);
        System.out.println("values inserted");
    }

    public static Config getConfig() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig("map1");
        mapConfig.setBackupCount(1);
        config.addMapConfig(mapConfig);
//        config.setProperty("hazelcast.logging.type", "jdk");
        MulticastConfig expectedConfig = new MulticastConfig()
                .setEnabled(true)
                .setLoopbackModeEnabled(true);
        config.getNetworkConfig().getJoin().setMulticastConfig(expectedConfig);
        return config;
    }

    static class EP implements Serializable, EntryProcessor<Integer, Integer> {

        private final boolean abortableBackupProcessor;
        private final String uuid2;

        EP(boolean abortableBackupProcessor, String uuid2) {
            this.abortableBackupProcessor = abortableBackupProcessor;
            this.uuid2 = uuid2;
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            entry.setValue(0);
            return null;
        }

        @Override
        public EntryBackupProcessor<Integer, Integer> getBackupProcessor() {
//            System.out.println("getBackupProcessor called");
            if (abortableBackupProcessor) {
                return new EBP(uuid2);
            } else {
                return new EntryBackupProcessor() {
                    @Override
                    public void processBackup(Map.Entry entry) {
                        entry.setValue(0);
                    }
                };
            }
        }
    }

    static class EBP implements AbortableEntryBackupProcessor<Integer, Integer>, Serializable, HazelcastInstanceAware {
        private volatile transient HazelcastInstance hz;
        private final String uuid2;

        EBP(String uuid2) {
            this.uuid2 = uuid2;
        }

        @Override
        public void processBackup(Map.Entry entry) {
            if (hz.getLocalEndpoint().getUuid().equals(uuid2)) {
                try {
                    System.out.println("sleeping");
                    sleep(random.nextInt(250000));
                } catch (InterruptedException ignore) {
                }
            }
            entry.setValue(0);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hz = hazelcastInstance;
        }
    }

}