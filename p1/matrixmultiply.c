#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

void printRow(int columns, int *arr){
    printf("[");
    for(int c = 0; c < columns; c++)
    {
        int element = arr[c];
        printf("%d\t", element);
    }
    printf("]\n");
}

void printInputParameters(int inputMatrixRows, int inputMatrixColumns, int inputVectorRows, int **matrix, int *vector){
    printf("\n The matrix is %d x %d\n", inputMatrixRows, inputMatrixColumns);
    for(int r = 0; r < inputMatrixRows; r++){
        printRow(inputMatrixColumns, matrix[r]);
    }
    printf("\nThe vector is %d x 1\n", inputVectorRows);
    printRow(inputVectorRows, vector);
}

void singleThreadedMatrixMultiply(int inputMatrixRows, int inputMatrixColumns, int **matrix, int *vector, int *result){
    for(int r = 0; r < inputMatrixRows; r++){
        int *rowToMultiply = matrix[r];
        int acc = 0;
        for(int c = 0; c < inputMatrixColumns; c++){
            int localResult = rowToMultiply[c] * vector[c];
            acc = acc + localResult;
        }
        result[r] = acc;
    }
}

struct multiply_work {
	int startRow;
    int numRows;
    int numColumns;
	int threadId;
	int *vector;
	int *result;
    int** matrix;
};

void printMultiplyWork(struct multiply_work multiplyWork){
	printf("ThreadId:%d, NumRows:%d, NumColumns: %d, StartRow:%d\n", multiplyWork.threadId, multiplyWork.numRows, multiplyWork.numColumns, multiplyWork.startRow);
}

void *partialMatrixMultiplication(void *arg) {
    struct multiply_work  *multiplyWork = arg;

	// printMultiplyWork(*multiplyWork);

	for(int r = multiplyWork->startRow; r < multiplyWork->startRow + multiplyWork->numRows; r++){
        int *rowToMultiply = multiplyWork->matrix[r];
        int acc = 0;
        for(int c = 0; c < multiplyWork->numColumns; c++){
			int x = rowToMultiply[c];
			int y = multiplyWork->vector[c];
			int res = x * y;
			
            acc = acc + res;
        }
        multiplyWork->result[r] = acc;
	}
	
	return NULL;
}
void parallelMatrixMultiply(int threadCount, int inputMatrixRows, int inputMatrixColumns, int **matrix, int *vector, int *result){
    // Every thread will receive an equal interval to process (startRow + numRows)
	struct multiply_work *multiplyWorkArr = malloc(sizeof(struct multiply_work) * threadCount);
	int chunk = inputMatrixRows / threadCount;
	pthread_t *threads = malloc(sizeof(*threads) * threadCount);
	int currentRow = 0;
	for(int i = 0; i < threadCount; i++){
		struct multiply_work *multiplyWork = &multiplyWorkArr[i];
		multiplyWork->matrix = matrix;
		multiplyWork->vector = vector;
		multiplyWork->startRow = currentRow;
		multiplyWork->result = result;
		multiplyWork->threadId = i;
		multiplyWork->numColumns = inputMatrixColumns;
		if (i == threadCount - 1){
			multiplyWork->numRows = inputMatrixRows - chunk * (threadCount - 1);
		} else {
			multiplyWork->numRows = chunk;
		}
		currentRow+=multiplyWork->numRows;
	}
	

	for(int i = 0; i < threadCount; i++){
		struct multiply_work *multiplyWork = &multiplyWorkArr[i];
		
		// Create and kick off the thread
        if (pthread_create(&threads[i], NULL, partialMatrixMultiplication, multiplyWork) != 0) {
            perror("Failed to create thread\n");
        }
	}

	// Join threads after they are done
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
    }
	
    free(multiplyWorkArr);
    free(threads);
}

void writeArrayToFile(int columns, int *arr){
    FILE *f = fopen("output.txt", "w+");
    if (f == NULL)
    {
        printf("Error opening file!\n");
        exit(1);
    }
    
    for(int c = 0; c < columns; c++)
    {
        int element = arr[c];
        if (c == columns - 1){
            fprintf(f, "%d", element);
        } else {
            fprintf(f, "%d,", element);
        }
    }
    
    fclose(f);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("usage: %s threads inputfile\n", argv[0]);
        return -1;
    }
	
	int threadCount = atoi(argv[1]);
	char* inputFile = argv[2];
	printf("Will do processing with %d threads and on inputfile %s.\n", threadCount, inputFile);
	
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
    
    int **matrix;
    int *vector;

    while (fgets(line, sizeof(line), file)) {
        char *element = strtok(line, ",");
        while (element != NULL){
            int elementAsInteger = atoi(element);

            //printf("%d\n", elementAsInteger);
            
            // State machine goes here
            if (inputMatrixRows == -1){
                inputMatrixRows = elementAsInteger;
            } else if (inputMatrixColumns == -1){
                inputMatrixColumns = elementAsInteger;
            } else {
                if (!arraysAreInitialized){
                    printf("Dimensions are %d x %d. Creating arrays...\n", inputMatrixRows, inputMatrixColumns);
                    vector = (int *) malloc(sizeof(int) * inputMatrixColumns);
                    matrix = (int **) malloc(sizeof(int*) * inputMatrixRows);
                    for(int i = 0; i < inputMatrixRows; i++){
                        matrix[i] = (int *) malloc(sizeof(int) * inputMatrixColumns);
                    }
                    expectedMatrixElements = inputMatrixRows * inputMatrixColumns;
                    expectedVectorElements = inputMatrixColumns;
                    arraysAreInitialized = true;
                }
                
                
                if (!matrixFilled){
                    int r = matrixOffset/inputMatrixColumns;
                    int c = matrixOffset%inputMatrixColumns;
                    matrix[r][c] = elementAsInteger;
                    matrixOffset++;
                    if (matrixOffset == expectedMatrixElements){
                        matrixFilled = true;
                    }
                } else if (!vectorFilled){
                    vector[vectorOffset] = elementAsInteger;
                    vectorOffset++;
                    if (vectorOffset == expectedVectorElements){
                        vectorFilled = true;
                    }
                } else {
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
    
    int inputVectorRows = inputMatrixColumns;
    //printInputParameters(inputMatrixRows, inputMatrixColumns, inputVectorRows, matrix, vector);
    fclose(file);

    // At this point, we are done reading from the file. We will perform matrix multiply now

	printf("Running BFS...\n\n");
    
    // Matrix multiply - grab the entire vector and the ith row, then multiply and sum one by one
    int *result = (int*) malloc(sizeof(int) * inputVectorRows);
    
	struct timeval start, end;
	gettimeofday(&start, NULL);

		
    //singleThreadedMatrixMultiply(inputMatrixRows, inputMatrixColumns, matrix, vector, result);
    parallelMatrixMultiply(threadCount, inputMatrixRows, inputMatrixColumns, matrix, vector, result);

	gettimeofday(&end, NULL);

	printf("Done!===============\n");
	printf("Time taken: %ld\n", ((end.tv_sec * 1000000 + end.tv_usec)
	  - (start.tv_sec * 1000000 + start.tv_usec)));
	printf("\n\n");
	

    printf("The resulting vector is\n");
    printRow(inputVectorRows, result);
    
	printf("\nI wrote this to output.txt.\n\n");
    writeArrayToFile(inputMatrixColumns, result);
    
    free(vector);
    free(result);
    for(int i = 0; i < inputMatrixRows; i++){
        free(matrix[i]);
    }
    free(matrix);
    
    return 0;
}