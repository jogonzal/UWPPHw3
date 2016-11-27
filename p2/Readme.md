For each node, initialize a boolean array of whether it has been "touched". The array will be of length 256. Only the root node has the first level set to 1.

We put all the nodes in a big array, and we also put all the edges in a big array.

The nodes have an position in the array, as well as the edges.

Example:

IN this example node 3 is root

Nodes:
	[0] Node 0, level -1
	[1] Node 1, level -1
	[2] Node 2, level -1
	[3] Node 3, level 0

Edges (note they are bidirectional)
	[0] 0 - 3
	[1] 1 - 2
	[2] 3 - 0
	[3] 0 - 2
	[4] 2 - 3

The algorithm is as follows.

In every GPU iteration for each work item, we:
1. Iterate over the corresponding edges for the work item
2. If the node is visited, visit all of its children (how do you get the children of a node?)
3. When visiting a child, simply set its level to the next level index (if it hasn't been visited before)
4. Barriers are used to prevent work items from going rogue into the next iteration
4. A global variable that is reset every cycle is needed to know whether any new nodes were found in any iteration

In the example above, after the first iteration, things will look like this:

Nodes:
	[0] Node 0, level 1
	[1] Node 1, level -1
	[2] Node 2, level -1
	[3] Node 3, level 0
	
Because node 0 and 3 are connected. Then it will look like this:

Nodes:
	[0] Node 0, level 1
	[1] Node 1, level -1
	[2] Node 2, level 2
	[3] Node 3, level 0

And so on. The vertex and nodes can be counted as we increase levels in each node.