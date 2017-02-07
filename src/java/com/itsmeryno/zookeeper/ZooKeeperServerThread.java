package com.itsmeryno.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.net.InetSocketAddress;

public class ZooKeeperServerThread extends Thread {
    
    private static final Logger LOG = Logger.getLogger(ZooKeeperServerThread.class);
    
    @Override
    public void run() {
        try {
            ZooKeeperServer zookeeperServer = new ZooKeeperServer();
            QuorumPeerConfig config = new QuorumPeerConfig();

            zookeeperServer.setTxnLogFactory(new FileTxnSnapLog(
                    new File("zk/data"),
                    new File("zk/data")));
            zookeeperServer.setTickTime(1000);
            zookeeperServer.setMinSessionTimeout(config.getMinSessionTimeout());
            zookeeperServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            ServerCnxnFactory connectionFactory = ServerCnxnFactory.createFactory();
            connectionFactory.configure(new InetSocketAddress(2181), 20);
            connectionFactory.startup(zookeeperServer);
            connectionFactory.join();
            if (zookeeperServer.isRunning()) {
                zookeeperServer.shutdown();
            }
        }
        catch (Exception ex) {
            LOG.error("Unable to start ZooKeeper!!", ex);
        }
    }
}
