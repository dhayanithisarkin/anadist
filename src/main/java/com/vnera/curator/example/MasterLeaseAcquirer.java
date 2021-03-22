package com.vnera.curator.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.Revoker;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class MasterLeaseAcquirer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(MasterLeaseAcquirer.class);
    private static final double reassign_probability = 0.95d;

    private final CuratorFramework client;
    private final InterProcessMutex masterLease;
    private final InterProcessReadWriteLock guardLease;
    private final ServiceDiscovery<InstanceDetails> serviceDiscovery;

    public MasterLeaseAcquirer(ServiceDiscovery<InstanceDetails> serviceDiscovery,
                               CuratorFramework client,
                               InterProcessReadWriteLock guardLease) {
        this.serviceDiscovery = serviceDiscovery;
        this.client = client;
        // re-entrant lock so we can re-acquire it on the same thread.
        this.masterLease = new InterProcessMutex(client, Leases.masterLease);
        this.guardLease = guardLease;
    }

    public void run() {
        // check if I am the current owner of the master lease
        // if no, then try to acquire master lease with a timeout
        // if acquited, run cluster check and see if a re-assignment is needed
        // if not acquired, log the attempt and do nothing.
        try {
            boolean acquired = masterLease.acquire(1, TimeUnit.SECONDS);
            if (acquired) {
                System.out.println("MASTER: I am master!");
                double rand = ThreadLocalRandom.current().nextDouble();

                // 10% of the time re-assignments to happen
                if (rand < reassign_probability) {
                    System.out.println("Trying to reassign shards");
                    // since revocation is co-operative and is happening in
                    // distributed setting, we need to wait for all the holders
                    // to release the guard read lock. For analytics service this
                    // can take several minutes.

                    boolean guardWriteAcquired = false;
                    while (!guardWriteAcquired) {
                        // revoke the guard leases from all the shard. This is called inside
                        // the loop to account for readers coming up after the first attempt
                        // of revocation is complete. Revocation handlers should be able
                        // to handle duplicate revocation requests.
                        // TODO: What is the time between release() on the read lock
                        // and the node disappearing from the participant's list.
                        Collection<String> participants = guardLease.readLock().getParticipantNodes();
                        System.out.println("Number of read lockers: " + participants.size());

                        for (String node : participants) {
                            Revoker.attemptRevoke(client, node);
                        }
                        guardWriteAcquired = guardLease.writeLock().acquire(10, TimeUnit.SECONDS);
                    }
                    System.out.println("Write lock acquired!");
                    // do reassignment for the service instances
                    Collection<String> serviceNames = this.serviceDiscovery.queryForNames();
                    System.out.println("Service names = " + serviceNames);
                    Collection<ServiceInstance<InstanceDetails>> instances =
                            this.serviceDiscovery.queryForInstances(AnalyticsService.SERVICE_NAME);
                    System.out.println("Instances = " + instances);

                    System.out.println("Shard re-assignment done!");
                    guardLease.writeLock().release();
                    System.out.println("Write lock released!");
                } else {
                    System.out.println("Nothing to do");
                }
            } else {
                System.out.println("I am not master");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
