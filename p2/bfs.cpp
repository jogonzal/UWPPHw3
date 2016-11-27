#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <time.h>
#include <map>
#include <vector>
#include <queue>
#include <stack>
#include <stdbool.h>
#include <time.h>
#include <fstream>
#include <mpi.h>

using namespace std;

class Node {
public:
	unsigned long long id;
	bool visited;
	Node *previous;
	vector<Node*> *children;

	int index;

	Node(int indexParam) {
		visited = false;
		previous = NULL;
		children = new vector<Node*>();
		index = indexParam;
	}
};

struct edge {
	unsigned long long origin;
	unsigned long long destination;
};

Node* GetOrCreateNode(map<unsigned long long, Node*> *graphInputMap, unsigned long long id, int *nodeIndexOffset) {
	Node *node;
	std::map<unsigned long long, Node*>::iterator it;
	it = graphInputMap->find(id);
	if (it == graphInputMap->end()) {
		node = new Node(*nodeIndexOffset);
		(*nodeIndexOffset)++;
		node->id = id;
		(*graphInputMap)[id] = node;
		return node;
	}
	else {
		// This would mean the dictionary already contained the node
		// simply return it
		return it->second;
	}
}

void PrintToFileAndConsole(int verticesTotal, int edgesTotal, unsigned long long rootValue, int vertexCount, int maxLevel) {
	printf("\n\nGraph vertices: %d with total edges %d. Reached vertices from %lld is %d and max level is %d.\n\n",
		verticesTotal, edgesTotal, rootValue, vertexCount, maxLevel);

	FILE *f = fopen("output.txt", "w+");
	if (f == NULL)
	{
		printf("Error opening file!\n");
		exit(1);
	}

	fprintf(f, "Graph vertices: %d with total edges %d. Reached vertices from %lld is %d and max level is %d.\n\n",
		verticesTotal, edgesTotal, rootValue, vertexCount, maxLevel);

	printf("I wrote this to output.txt\n");

	fclose(f);
}

void mpiBfs(
	int *maxLevelMpi,
	int *vertexCountMpi,
	int *edgeCountMpi,
	int edgeCount,
	int nodeListSize,
	int *nodeList,
	int *edgeList,
	int processCount,
	int rank) {
	// For every edge...
	int currentLevel = 0;
	int totalEdges = 0;
	int totalVertex = 0;
	int foundNewLevel = 0;
	
	int chunkSize = edgeCount / processCount;
	int localChunk = chunkSize;
	if (rank == (processCount - 1)){
		localChunk = edgeCount - (processCount - 1) * chunkSize;
	}
	//printf("%d: Starting with parameters: edgeCount:%d, nodeListSize:%d, processCount:%d, localChunk:%d\n", rank, edgeCount, nodeListSize, processCount, localChunk);
	do {
		foundNewLevel = 0;
		for (int i = 0; i < localChunk / 2; i++) {
			int offset = i * 2 + chunkSize * rank;
			// printf("%d: My offset is %d\n", rank, offset);
			int origin = edgeList[offset];
			int destination = edgeList[offset + 1];

			// If the node is the current level, activate it's destination
			int currentLevelOrigin = nodeList[origin];
			int currentLevelDestination = nodeList[destination];

			if (currentLevelOrigin == currentLevel) {
				totalEdges++;
			}
			if (currentLevelDestination == currentLevel) {
				totalEdges++;
			}

			if (currentLevelOrigin == currentLevel && currentLevelDestination == -1) {
				nodeList[destination] = currentLevel + 1;
				foundNewLevel = 1;
				totalVertex++;
			}
			else if (currentLevelDestination == currentLevel && currentLevelOrigin == -1) {
				nodeList[origin] = currentLevel + 1;
				foundNewLevel = 1;
				totalVertex++;
			}
		}
		
		// Synchronize the list of nodes via AllReduce
		// printf("%d: Reducing...\n", rank, offset);
		MPI_Allreduce(MPI_IN_PLACE, nodeList, nodeListSize, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
		// Synchronize the list of found nodes via AllReduce
		// printf("%d: Reducing 2...\n", rank, offset);
		MPI_Allreduce(MPI_IN_PLACE, &foundNewLevel, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
		
		//printf("%d: Done with level %d. FoundNewLevel: %d\n", rank, currentLevel, foundNewLevel);
		
		currentLevel++;
	} while (foundNewLevel >= 1);

	*edgeCountMpi = totalEdges + 1;
	*vertexCountMpi = totalVertex + 1;
	*maxLevelMpi = currentLevel - 1;
	
	//printf("%d: Done completely edgeCount%d. vertexCount:%d, maxLevel%d\n", rank, *edgeCountMpi, *vertexCountMpi, *maxLevelMpi);
	
	MPI_Allreduce(MPI_IN_PLACE, edgeCountMpi, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	MPI_Allreduce(MPI_IN_PLACE, vertexCountMpi, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	MPI_Allreduce(MPI_IN_PLACE, maxLevelMpi, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
	
	*edgeCountMpi += 1;
	*vertexCountMpi += 1;
	*maxLevelMpi -= 1;
	
	// Actually, the vertex count is imprecise, as there are multiple race conditions that can arise as part of this processing
	// To calculate the right number, one of the processees will have to loop through the node list  and find all the nodes that were visited.
	int trueVertexCount = 0;
	for(int i = 0; i < nodeListSize; i++){
		if (nodeList[i] > -1){
			trueVertexCount++;
		}
	}
	*vertexCountMpi = trueVertexCount;
	// printf("%d: Done completely edgeCount%d. vertexCount:%d, maxLevel%d\n", rank, *edgeCountMpi, *vertexCountMpi, *maxLevelMpi);
}

// NOTE: The function below follows the same algorithm proposed in the Readme.md file, but single threaded
// It only exists as a guide.
void singleThreadedProposedBfsAlgorithm(int *maxLevelMpi,
	int *vertexCountMpi,
	int *edgeCountMpi,
	int dim,
	int nodeListSize,
	int *nodeList,
	int *edgeList) {

	// For every edge...
	int currentLevel = 0;
	int totalEdges = 0;
	int totalVertex = 0;
	bool foundNewLevel;
	do {
		foundNewLevel = false;
		for (int i = 0; i < dim; i++) {
			int origin = edgeList[i * 2];
			int destination = edgeList[i * 2 + 1];

			// If the node is the current level, activate it's destination
			int currentLevelOrigin = nodeList[origin];
			int currentLevelDestination = nodeList[destination];

			if (currentLevelOrigin == currentLevel) {
				totalEdges++;
			}
			if (currentLevelDestination == currentLevel) {
				totalEdges++;
			}

			if (currentLevelOrigin == currentLevel && currentLevelDestination == -1) {
				nodeList[destination] = currentLevel + 1;
				foundNewLevel = true;
				totalVertex++;
			}
			else if (currentLevelDestination == currentLevel && currentLevelOrigin == -1) {
				nodeList[origin] = currentLevel + 1;
				foundNewLevel = true;
				totalVertex++;
			}
		}
		currentLevel++;
	} while (foundNewLevel);

	*edgeCountMpi = totalEdges + 1;
	*vertexCountMpi = totalVertex + 1;
	*maxLevelMpi = currentLevel - 1;
}

void printEdge(unsigned long long origin, unsigned long long destination) {
	printf("%llx -> %llx\n", origin, destination);
}

void printEdge(struct edge edge) {
	printf("%llx -> %llx\n", edge.origin, edge.destination);
}

int fileSize(const char *add) {
	ifstream mySource;
	mySource.open(add, ios_base::binary);
	mySource.seekg(0, ios_base::end);
	int size = mySource.tellg();
	mySource.close();
	return size;
}

int main(int argc, char *argv[]) {
	int size, rank;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	printf("SIZE = %d RANK = %d\n",size,rank);
	
	unsigned long long edgeMaxCount = 3000000 * 2;
	unsigned long long nodeMaxCount = 64000 * 1;
	
	if (argc != 3) {
		printf("usage: %s root inputfile\n", argv[0]);
		printf("Sample root: 0");
		return -1;
	}

	int dim = edgeMaxCount / 2;
	int *edgeList = NULL;
	int *nodeList = NULL;

	int edgeCount, nodeCount;
	
	unsigned long long rootValue = (unsigned long long)atoi(argv[1]);

	// First, read the file just in process 0
	if (rank == 0){
		char* fileName = argv[2];

		printf("Starting. root %llx, inputfile %s\n", rootValue, fileName);
		FILE* file = fopen(fileName, "rb");

		int buffSize = 1000;

		struct edge *buff = (struct edge *) malloc(sizeof(struct edge) * buffSize);

		int edgesTotal = 0;

		map<unsigned long long, Node*> *graphInputMap = new map<unsigned long long, Node*>();

		edgeList = new int[dim * 2];
		nodeList = new int[nodeMaxCount];
		for (int i = 0; i < dim * 2; i++) {
			edgeList[i] = -1;
		}
		for (int i = 0; i < nodeMaxCount; i++) {
			nodeList[i] = -1;
		}

		int nodeOffset = 0;
		long edgeListOffset = 0;
		for (;;) {
			size_t elementsRead = fread(buff, sizeof(struct edge), buffSize, file);
			edgesTotal += elementsRead * 2;
			for (int i = 0; i < elementsRead; i++) {
				struct edge edge = buff[i];
				//printEdge(edge);
				Node *nodeOrigin = GetOrCreateNode(graphInputMap, edge.origin, &nodeOffset);
				Node *nodeDestination = GetOrCreateNode(graphInputMap, edge.destination, &nodeOffset);
				nodeOrigin->children->push_back(nodeDestination);
				nodeDestination->children->push_back(nodeOrigin);
				edgeList[edgeListOffset] = nodeOrigin->index;
				edgeList[edgeListOffset + 1] = nodeDestination->index;
				edgeListOffset+=2;
			}
			if (elementsRead < buffSize) { break; }
		}

		edgeCount = edgesTotal;
		
		fclose(file);

		// Get the pointer to the root
		Node *root = NULL;
		std::map<unsigned long long, Node*>::iterator it;
		it = graphInputMap->find(rootValue);
		if (it != graphInputMap->end()) {
			printf("The root you specified was in the input file. Now calculating stats...\n");
			root = it->second;
		}
		else {
			printf("Where did you get this value from? The root you specified wasn't in the input file\n");
			exit(1);
		}

		nodeList[root->index] = 0;

		int verticesTotal = graphInputMap->size();
		nodeCount = verticesTotal;

		printf("I read %d edges and have found %d vertices (in total). Now doing breadth first search to calculate the number of edges and vertices from root 0x%llx \n",
			edgesTotal, verticesTotal, rootValue);
		printf("NodeCount: %d, EdgeCount:%d \n",
			nodeCount, edgeCount);
		free(graphInputMap);
		free(buff);
	}
	
	edgeCount = edgeCount;
	// Broadcast the dimensions from process 0
	MPI_Bcast(&edgeCount, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&nodeCount, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	// Allocate edges and nodes in the rest of the processees
	if (rank != 0){
		edgeList = new int[edgeCount];
		nodeList = new int[nodeCount];
	}
	
	// Broadcast their values from node 0
	MPI_Bcast(nodeList, nodeCount, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(edgeList, edgeCount, MPI_INT, 0, MPI_COMM_WORLD);
	
	int maxLevelBfs = 0; 		// assumming at least root exists, level is 1
	int vertexCountBfs = 0;
	int edgeCountBfs = 0;
	mpiBfs(&maxLevelBfs, &vertexCountBfs, &edgeCountBfs, edgeCount, nodeCount, nodeList, edgeList, size, rank);

	// Only node 0 should print
	if (rank == 0){
		PrintToFileAndConsole(nodeCount, edgeCount, rootValue, vertexCountBfs, maxLevelBfs);
	}

	MPI_Finalize();   
	return(0);
}