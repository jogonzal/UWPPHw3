#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <mpi.h>

void host_muls(int *matrix,
	int *vector,
	int *result,
	int dim) {
	for (int r = 0; r < dim; r++) {
		int acc = 0;
		for (int c = 0; c < dim; c++) {
			int matrixNumber = matrix[r * dim + c];
			int vectorNumber = vector[c];
			acc += matrixNumber * vectorNumber;
		}
		result[r] = acc;
	}
}

void printVector(int *vector, int dim) {
	printf("[");
	for (int i = 0; i < dim; i++) {
		printf("%d\t", vector[i]);
	}
	printf("]\n");
}

void writeArrayToFile(int columns, int *arr) {
	FILE *f = fopen("output.txt", "w+");
	if (f == NULL)
	{
		printf("Error opening file!\n");
		exit(1);
	}

	for (int c = 0; c < columns; c++)
	{
		int element = arr[c];
		if (c == columns - 1) {
			fprintf(f, "%d", element);
		}
		else {
			fprintf(f, "%d,", element);
		}
	}

	fclose(f);
}

int main(int argc, char *argv[]) {
	
	int size, rank;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	printf("SIZE = %d RANK = %d\n",size,rank);

	int   *matrix, *vector, *resultHost, *resultMpi;
	int matrixSize;
	int i;
    if (argc != 2) {
        printf("{%d}: Usage: %s inputfile\n", rank, argv[0]);
        return -1;
    }
	// Only the first processor will do the file reading into arrays
	if (rank == 0){
		char* inputFile = argv[1];
		printf("{%d}: Will do processing on inputfile %s.\n", rank, inputFile);

		FILE* file = fopen(inputFile, "r"); /* should check the result */
		char line[1024];

		int inputMatrixRows = -1;
		int inputMatrixColumns = -1;
		bool arraysAreInitialized = false;
		bool matrixFilled = false;
		bool vectorFilled = false;

		int matrixOffset = 0;
		int vectorOffset = 0;
		int expectedMatrixElements = -1;
		int expectedVectorElements = -1;

		while (fgets(line, sizeof(line), file)) {
			char *element = strtok(line, ",");
			while (element != NULL) {
				int elementAsInteger = atoi(element);

				//printf("%d\n", elementAsInteger);

				// State machine goes here
				if (inputMatrixRows == -1) {
					inputMatrixRows = elementAsInteger;
				}
				else if (inputMatrixColumns == -1) {
					inputMatrixColumns = elementAsInteger;
				}
				else {
					if (!arraysAreInitialized) {
						if (inputMatrixRows != inputMatrixColumns) {
							printf("Dimensions must match on matrix (square). %d and %d don't match.\n",
								inputMatrixColumns, inputMatrixRows);
							exit(-__LINE__);
						}
						printf("Dimensions are %d x %d. Creating arrays...\n", inputMatrixRows, inputMatrixColumns);
						matrix = (int*)malloc(sizeof(*matrix) * inputMatrixRows * inputMatrixColumns);
						vector = (int*)malloc(sizeof(*vector) * inputMatrixColumns);
						resultHost = (int*)malloc(sizeof(*resultHost) * inputMatrixRows);
						resultMpi = (int*)malloc(sizeof(*resultMpi) * inputMatrixRows);
						for (i = 0; i < inputMatrixRows * inputMatrixColumns; i++) {
							matrix[i] = 0;
						}
						for (i = 0; i < inputMatrixColumns; i++) {
							vector[i] = 0;
						}
						expectedMatrixElements = inputMatrixRows * inputMatrixColumns;
						expectedVectorElements = inputMatrixColumns;
						arraysAreInitialized = true;
					}


					if (!matrixFilled) {
						int r = matrixOffset / inputMatrixColumns;
						int c = matrixOffset%inputMatrixColumns;
						matrix[r * inputMatrixColumns + c] = elementAsInteger;
						matrixOffset++;
						if (matrixOffset == expectedMatrixElements) {
							matrixFilled = true;
						}
					}
					else if (!vectorFilled) {
						vector[vectorOffset] = elementAsInteger;
						vectorOffset++;
						if (vectorOffset == expectedVectorElements) {
							vectorFilled = true;
						}
					}
					else {
						printf("\nERROR IN INPUT!!!!!!!\nToo many numbers?");
						return -1;
					}
				}

				// Advance to next
				element = strtok(NULL, ",");
			}
		}
		
		if (!(matrixFilled && vectorFilled)){
			printf("\nERROR IN INPUT!!!!!!!\nNot enough numbers?");
			return -1;
		}
		
		matrixSize = inputMatrixColumns;
		
		//DoMPIMultiplication(inputVectorRows, matrix, vector);
	}
	
	MPI_Bcast(&matrixSize, 1, MPI_INT, 0, MPI_COMM_WORLD);
	printf("{%d}: Matrix size: %d inputfile\n", rank, matrixSize);
	if (rank != 0){
		// Do allocations
		matrix = (int*)malloc(sizeof(*matrix) * matrixSize * matrixSize);
		vector = (int*)malloc(sizeof(*vector) * matrixSize);
		resultHost = (int*)malloc(sizeof(*resultHost) * matrixSize);
		resultMpi = (int*)malloc(sizeof(*resultMpi) * matrixSize);
	}
	
	MPI_Bcast(matrix, matrixSize * matrixSize, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(vector, matrixSize, MPI_INT, 0, MPI_COMM_WORLD);
	
	// Now that everybody has the matrix and vector, we should do matrix multiplication
	int chunkSize = matrixSize / size;
	{
		int localChunk = chunkSize;
		if (rank == size - 1){
			localChunk = matrixSize - chunkSize * (size - 1);
		}
		for(int row = 0; row < localChunk; row++){
			int offset = row + rank * chunkSize;
			int acc = 0;
			for(int col = 0; col < matrixSize; col++){
				acc += vector[col] * matrix[offset * matrixSize + col];
			}
			resultMpi[offset] = acc;
		}
	}
	
	// Now that everybody calculated what they should, some sending needs to be done
	if (rank == 0){
		for(int i = 1; i < size; i++){
			int localChunk = chunkSize;
			if (i == size - 1){
				localChunk = matrixSize - chunkSize * (size - 1);
			}
			int offset = i * chunkSize;
			MPI_Recv(&resultMpi[offset], localChunk, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	} else {
		int localChunk = chunkSize;
		if (rank == size - 1){
			localChunk = matrixSize - chunkSize * (size - 1);
		}
		int offset = rank * chunkSize;
		MPI_Send(&resultMpi[offset], localChunk, MPI_INT, 0, 0, MPI_COMM_WORLD);
	}
	
	if (rank == 0){
		printVector(resultMpi, matrixSize);
		writeArrayToFile(matrixSize, resultMpi);
	}
	
	free(matrix);
	free(vector);
	free(resultHost);
	free(resultMpi);
	
	MPI_Finalize();   
	return(0);
}