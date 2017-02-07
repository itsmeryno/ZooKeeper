package com.itsmeryno.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

public abstract class AbstractZooKeeperClientThread implements Watcher, AsyncCallback.ChildrenCallback, Runnable {
    
    private static final Logger LOG = Logger.getLogger(AbstractZooKeeperClientThread.class);

    private int clientId;
    private ZooKeeper zooKeeper;
    protected final String ROOT = "/VOTING_STATION";

    public AbstractZooKeeperClientThread(int clientId) {
        this.clientId = clientId;
    }

    protected abstract void process();
    protected abstract void processDisconnectedEvent();
    protected abstract void processConnectedEvent();

    @Override
    public void run() {
        try {
            connectToZooKeeper();
        } catch (IOException e) {
            LOG.error("Unable to connect to ZooKeeper!!");    
        }
        process();
    }

    protected void connectToZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper("localhost:2181",1000,this);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.None) {
            processStateEvent(event);
        }
        else {
            processNodeChangeEvent(event);
        }
    }

    private void processNodeChangeEvent(WatchedEvent event) {
        String path = event.getPath();
        if (path != null && path.equals(ROOT)) {
            zooKeeper.getChildren(ROOT, true, this, null);
        }
    }

    private void processStateEvent(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                processConnectedEvent();
                break;
            case Disconnected:
                processDisconnectedEvent();
                break;
            case Expired:
                processExpiredEvent();
                break;
        }
    }

    protected void processExpiredEvent() {
        try {
            zooKeeper.close();
        } catch (InterruptedException ex) {
            LOG.error("Error closing ZooKeeper!!", ex);
            
        }
        try {
            connectToZooKeeper();
        } catch (IOException ex) {
            LOG.error("Uanble to connect to ZooKeeper!!", ex);
        }
    }

    public int getClientId() {
        return clientId;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    protected void log(String message) {
        LOG.info(String.format("Client [%d] - %s", clientId, message));
    }
}
