# Rotakka Graph Store

... is a distributed in-memory and on-disk storage for a graph. 
It stores the graph in shards, which are stored by the GraphStoreSlaves. 
Every shard is stored on duplicationLevel many different slaves, therefore, 
any stored shard is called a shard copy. 
In a complete system with at least duplicationLevel many slaves, 
there are shardCount * duplicationLevel many shard copies.

The other cluster components only communicate with the GraphStoreMaster. The graph store is a
master-slave system, in which all actions have to go through the master to keep the data consistent.

The messages that can be received by the actors are usually defined as inner classes of the actor classes.

## Data Model

The graph consists of vertices and edges. 
All vertices and edges must have a unique string key. 
The contents of the vertices and edges can be seen in the GraphStoreMaster class.

## Actors

### GraphStoreMaster

The GraphStoreMaster is capable of receiving vertices, edges, and subgraphs (consisting of vertices and edges).
Those graph elements do not have any information about the shard they are supposed to be stored on.
The master assigns a shard number to every graph element and forwards it as sharded graph element
(with information about the shard number) to a graph store buffer.

* responsible of:
  * receiving and forwarding of graph elements
  * maintaining a list of available slaves
  * maintaining a GraphStoreBuffer for every shard copy
  * coordinating shard copy procedures
  * initiating deletion of shard copies from slaves
  * answering location requests for vertex and edge keys
* uses the ShardMapper class to handle the buffers and slaves 

### GraphStoreBuffer

Every GraphStoreBuffer is mapped by the master to exactly one shard copy. 
However, the shard number does not play a role for the buffer, because it receives and forwards
sharded graph elements (already containing the assigned shard number). 
Only the GraphStoreSlaves interprete the shard number again.

The GraphStoreBuffers are controlled by the GraphStoreMaster, which is also the parent of all buffers.
When a new buffer is created, it can be specified whether to set a destination or not. 
If there is a destination specified, all received graph elements will be forwarded to it.
If there is no destination specified, all received graph elements will be dropped, until the buffering is started.
Then, all received graph elements get stored in a queue. 
As soon as a StopBuffering command is received, the whole buffer queue gets forwarded to the destination. 
If there is no destination set yet, the StopBuffering command must also include an ActorRef as destination, 
otherwise this field is optional. The parameter can also be used to override the previously configured destination.

Therefore, the GraphStoreBuffers are responsible of:
* receiving graph elements
* eventually buffering graph elements
* eventually forwarding graph elements
* eventually dropping graph elements

The GraphStoreBuffers were needed as an intermediate layer between the master and the slaves 
to guarantee a simultaneous buffering start for multiple shard copies and to take some logic out of the master. 
In order to work properly, the buffer depends on message orders, which are luckily guaranteed by TCP. 
Therefore, never switch to UDP without proper measures.
In the future, the buffers might also be moved to a different server to take some load off the master's server. 

### GraphStoreSlave

The GraphStoreSlaves are supposed to run once per system/server. They communicate only with the master and with other slaves.
From the buffers they only receive sharded graph elements, which they add to their local shard copies.

They are responsible of:
* receiving sharded graph elements
* holding their assigned shards in memory
* backing up their assigned shards to disk
* requesting a shard from another slave on assignment
* sending a shard to another slave on request
* receiving a shard from another slave
* enabling their shard after a copy procedure at the master
* deleting an assigned shard on request
* adding sharded graph elements to the right shard
* updating already existing graph elements
* detecting when there has been no change to the previous graph element and, therefore, 
preventing unnecessary duplication in the append-only shard storage file

The shard storage files are, by default, placed at "shards/\<actor name\>/\<shardNumber\>/" and are called vertices.json and edges.json
The responsible class for the storage operation is called GraphFileOutput and could be replaced by any other storage backend.

