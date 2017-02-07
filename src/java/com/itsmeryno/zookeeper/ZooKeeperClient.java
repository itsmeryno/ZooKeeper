package com.itsmeryno.zookeeper;

import org.apache.log4j.Logger;

import java.util.Random;

public class ZooKeeperClient extends AbstractZooKeeperElectionThread {
    
    private static final Logger LOG = Logger.getLogger(ZooKeeperClient.class);
    
    private Random random = new Random();
    
    public ZooKeeperClient(int clientId) {
        super(clientId);
    }

    @Override
    protected void doWork() {
        log("doWork()");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        simulateNetworkFailure();
    }

    private void simulateNetworkFailure() {
        //2% change of failing 
        if (random.nextInt(50) == 42) {
            LOG.info("randomly failing ...");
            try {
                getZooKeeper().close();
                Thread.sleep(10000);
                connectToZooKeeper();
            }
            catch (Exception ex) {
                LOG.error("Error during random fail!!!", ex);
            }
        }
    }
}
