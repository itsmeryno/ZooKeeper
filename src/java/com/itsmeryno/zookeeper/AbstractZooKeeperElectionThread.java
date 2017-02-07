package com.itsmeryno.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public abstract class AbstractZooKeeperElectionThread extends AbstractZooKeeperClientThread {
    
    private static Logger LOG = Logger.getLogger(AbstractZooKeeperElectionThread.class);
    
    private boolean running = true;
    private boolean leader = false;
    private String zookeeperNode;
    private long sequenceNumber;

    public AbstractZooKeeperElectionThread(int clientId) {
        super(clientId);
        resetZookeeperNode();
    }

    @Override
    protected void process() {
        while (running) {
            if (leader) {
                doWork();
            }
            else {
                waitForLeader();
            }
        }
    }

    private void waitForLeader() {
        try {
            Thread.sleep(1000);
        }
        catch (Exception ex) {
        }
    }

    protected abstract void doWork();

    private void resetZookeeperNode() {
        log("resetZookeeperNode()");
        zookeeperNode = ROOT + "/" + getClientId() + ".";
    }

    @Override
    public void processResult(int resultCode, String path, Object object, List<String> children) {
        if (resultCode == KeeperException.Code.OK.intValue()) {
            deleteOldChildren(children);
            electLeaderFromChildren(children);
        }
        else {
            leader = false;
        }
    }

    private void deleteOldChildren(List<String> children) {
        children.stream()
                .filter(child -> getNodeId(child).equals(getClientId()))
                .filter(child -> (parseSequenceNumber(child) != sequenceNumber))
                .forEach(child -> deleteOldChild(child));
    }

    private void electLeaderFromChildren(List<String> children) {
        long lowestSequenceNumber = sequenceNumber;

        for (String child : children) {
            String nodeId = getNodeId(child);
            long childSequenceNumber = parseSequenceNumber(child);
            if (nodeId.equals(getClientId()) && (sequenceNumber != childSequenceNumber)) {
                deleteOldChild(child);
            }
            lowestSequenceNumber = Math.min(lowestSequenceNumber, childSequenceNumber);
        }
        
        leader = (sequenceNumber == lowestSequenceNumber);
    }


    private void deleteOldChild(String child) {
        log("deleteOldChild() [" + child + "]");
        String nodeToDelete = ROOT + "/" + child;
        log("Attempting to delete node [" + nodeToDelete + "]");
        try {
            getZooKeeper().delete(nodeToDelete, -1);
        } catch (InterruptedException | KeeperException e) {
            log("Unable to delete node");
            e.printStackTrace();
        }
    }

    private long createZnode() throws KeeperException, InterruptedException {
        zookeeperNode = getZooKeeper().create(zookeeperNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        return parseSequenceNumber(zookeeperNode);
    }

    private long parseSequenceNumber(String zookeeperNode) {
        String[] nodeParts = zookeeperNode.split("\\.");
        return Long.parseLong(nodeParts[nodeParts.length-1]);
    }

    private String getNodeId(String zookeeperNode) {
        String[] nodeParts = zookeeperNode.split("\\.");
        return nodeParts[0];
    }

    private void createRootIfNotExists() throws KeeperException, InterruptedException {
        Stat root = getZooKeeper().exists(ROOT, false);

        if (root == null) {
            try {
                getZooKeeper().create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    @Override
    protected void processConnectedEvent() {
        try {
            createRootIfNotExists();
            getZooKeeper().getChildren(ROOT, true, this, null);
            sequenceNumber = createZnode();
            log("Assigned sequenceNumber [" + sequenceNumber + "]");
        }
        catch (Exception ex) {
            LOG.error("Unable to process Connected event!!", ex);
        }
    }

    @Override
    protected void processExpiredEvent() {
        super.processExpiredEvent();
        leader = false;
        resetZookeeperNode();
    }

    @Override
    protected void processDisconnectedEvent() {
        leader = false;
        resetZookeeperNode();
    }
}
