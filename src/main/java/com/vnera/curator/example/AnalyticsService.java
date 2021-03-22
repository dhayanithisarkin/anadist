package com.vnera.curator.example;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.RevocationListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class AnalyticsService {
    private static final Logger logger = LoggerFactory.getLogger(AnalyticsService.class);
    private static final String PATH = "/examples/locks";
    public static final String SERVICE_NAME = "AnalyticsService";
    private final ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private final CuratorFramework client;
    private final InterProcessReadWriteLock guardLease;
    private final CountDownLatch guardRead;
    private final Semaphore sem;
    private final ScheduledExecutorService lockHandlerThread;
    private BucketizationService bucketizationService;
    private MetricInjector metricInjector;

    public AnalyticsService(String identifier) throws Exception {
        this.client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                new ExponentialBackoffRetry(1000, 3));
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("State changed: " + newState);
                // TODO: If connection state is lost, block the workers with the semaphore.
            }
        });

        client.blockUntilConnected(30, TimeUnit.SECONDS);
        client.start();
        System.out.println("Client connected");

        UriSpec uriSpec = new UriSpec("{scheme}://foo.com:{port}");
        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<>(InstanceDetails.class);
        ServiceInstance<InstanceDetails> thisInstance = ServiceInstance.<InstanceDetails>builder()
                .name(SERVICE_NAME) // service name, same for all instances.
                .id(identifier) // unique id for the instance
                .uriSpec(uriSpec)
                .port((int)(65535 * Math.random()))
                .payload(new InstanceDetails(SERVICE_NAME + identifier))
                .build();

        serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .basePath(Leases.baseLeasePath)
                .serializer(serializer)
                .thisInstance(thisInstance)
                .build();

        // Curator's interprocess needs to be locked and released on the same thread.
        // this thread serves the purpose.
        lockHandlerThread = Executors.newSingleThreadScheduledExecutor();

        guardLease = new InterProcessReadWriteLock(client, Leases.guardLease);
        sem = new Semaphore(2, true);
        guardLease.readLock().makeRevocable(new MyRevocationListener(guardLease, sem), lockHandlerThread);
        guardRead = new CountDownLatch(1);
    }

    public void start() throws Exception {
        serviceDiscovery.start();
        bucketizationService = new BucketizationService(guardRead, sem);
        metricInjector = new MetricInjector(guardRead, sem);

        ScheduledExecutorService masterLeaseAcquirer = Executors.newSingleThreadScheduledExecutor();
        // doing fixed delay scheduling because some runs of this can take a lot longer if
        // the shard re-assignment work is done.
        masterLeaseAcquirer.scheduleWithFixedDelay(new MasterLeaseAcquirer(serviceDiscovery, client, guardLease),
                1,
                60,
                TimeUnit.SECONDS);

        ScheduledExecutorService bucketizationExecutor = Executors.newSingleThreadScheduledExecutor();
        bucketizationExecutor.scheduleWithFixedDelay(bucketizationService::bucketize,
                1,
                60,
                TimeUnit.SECONDS);

        ScheduledExecutorService analyticsExecutor = Executors.newSingleThreadScheduledExecutor();
        analyticsExecutor.scheduleAtFixedRate(metricInjector::analyze,
                1,
                60,
                TimeUnit.SECONDS);


        lockHandlerThread.execute(new Runnable() {
            @Override
            public void run() {
                boolean acquired = false;
                while (!acquired) {
                    try {
                        acquired = guardLease.readLock().acquire(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        logger.error("Exception during guard lock acquisition", e);
                    }
                }
                System.out.println("Got the guard read lock");
                System.out.println("Reading assignment");
                guardRead.countDown();
            }
        });
        System.out.println("Waiting for shutdown");
        Thread.sleep(60 * 10000);
    }

    public static void main(String[] args) throws Exception {
        // leader to do assignment.
        String ident = args[0];
        AnalyticsService service = new AnalyticsService(ident);
        service.start();
    }

    private static class BucketizationService {
        private final CountDownLatch guardRead;
        private final Semaphore sem;

        public BucketizationService(CountDownLatch guardRead, Semaphore sem) {
            this.guardRead = guardRead;
            this.sem = sem;
        }
        public void bucketize() {
            try {
                // wait till there is an assignment
                System.out.println("Waiting for assignment " + sem.availablePermits() + ":" + guardRead.getCount());
                guardRead.await();
                sem.acquire();
                // TODO: Assert that the guardLease's readLock() is held by this process
                try {
                    System.out.println("Doing bucketization");
                    Thread.sleep(20 * 1000);
                } finally {
                    sem.release();
                    System.out.println("Done bucketization");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class MetricInjector {
        private final CountDownLatch guardRead;
        private final Semaphore sem;

        public MetricInjector(CountDownLatch guardRead, Semaphore sem) {
            this.guardRead = guardRead;
            this.sem = sem;
        }

        public void analyze() {
            try {
                // wait till there is an assigment
                System.out.println("Waiting for assignment " + sem.availablePermits() + ":" + guardRead.getCount());
                guardRead.await();
                sem.acquire();
                // TODO: Assert that the guardLease's readLock() is held by this process
                try {
                    System.out.println("Doing metric injection");
                    Thread.sleep(35 * 1000);
                } finally {
                    sem.release();
                    System.out.println("Done metric injection");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class MyRevocationListener implements RevocationListener<InterProcessMutex> {
        private final InterProcessReadWriteLock guardLease;
        private final Semaphore sem;

        public MyRevocationListener(InterProcessReadWriteLock guardLease, Semaphore sem) {
            this.guardLease = guardLease;
            this.sem = sem;

        }

        @Override
        public void revocationRequested(InterProcessMutex guardReadLock) {
            Preconditions.checkArgument(guardLease.readLock() == guardReadLock);
            System.out.println("Revocation requested");
            try {
                sem.acquire(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                guardReadLock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Revocation succeeded!");
            // wait for new assignment.
            try {
                Thread.sleep(80 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                guardReadLock.acquire();
                // The fairness of the distributed locking guarantees that we have
                // a new assignment at this stage.
                // TODO: Assert new assignment.
                System.out.println("Reacquired guardLease");
            } catch (Exception e) {
                e.printStackTrace();
            }
            // TODO: Refresh all the caches based on the new assigment.
            sem.release(2);
        }

    }
}
