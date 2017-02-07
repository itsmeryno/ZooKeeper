# ZooKeeper
Some random ZooKeeper leader election code.

Starts up a ZooKeeper server as well as a bunch of client threads. Only one thread is allowed to do work at a time,
so ZooKeeper is used to do leader election.

As each thread connects to ZooKeeper, it writes a sequence number.

The thread with the lowest sequence number is allowed to process. 

Once a thread dies (simulated), the next thread in line is elected leader and starts processing.

Handy where you have situations where you need failover for Singleton type services
