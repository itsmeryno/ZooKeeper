package com.itsmeryno.zookeeper;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZooKeeperMain {
    private static final Logger LOG = Logger.getLogger(ZooKeeperMain.class);
    
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        LOG.info("Starting ZooKeeper ...");
        new ZooKeeperServerThread().start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            ZooKeeperClient zooKeeperClient = new ZooKeeperClient(i);
            threadPool.submit(zooKeeperClient);
        }
    }
}
