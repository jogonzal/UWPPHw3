The way I decided to parallelize here is by row - every process gets N rows assigned, and is therefore responsible for producing N numbers in the result vertex.

Because of this partitioning, no locking is required and we can simply kick off the processes with MPI and then join them.