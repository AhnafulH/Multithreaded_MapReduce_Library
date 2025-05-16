/****************************************************************************

  @file         mapreduce.c

  @author       Ahnaful Hoque

  @date         3/11/2024

*******************************************************************************/

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include "mapreduce.h"
#include "threadpool.h"

/**
 * @brief Node structure to store a key-value pair in a linked list.
 *
 */
typedef struct KeyValueNode {
    char *key;
    char *value;
    struct KeyValueNode *next;
} KeyValueNode;

/**
 * @brief Structure to manage a partition in the MapReduce framework.
 *
 */
typedef struct Partition {
    KeyValueNode *head;
    pthread_mutex_t mutex;
    char **unique_keys;               // Array of unique keys for reduce phase
    unsigned int unique_key_count;    // Number of unique keys in this partition
} Partition;

/**
 * @brief Structure to bundle arguments for the Reducer function.
 *
 */
typedef struct {
    char *key;
    unsigned int partition_idx;
} ReduceArgs;

/**
 * @brief Wrapper structure to pass additional information to the ReduceWrapper function.
 *
 */
typedef struct {
    char *key;
    unsigned int partition_idx;
    Reducer reducer;
} ReduceWrapperArgs;

// Globals for partitions and their count
static Partition *partitions;
static unsigned int num_partitions;

/**
 * @brief Initializes partitions for the MapReduce process.
 *
 * Allocates memory for each partition and initializes the necessary mutex 
 * and key-value structures for each partition. 
 *
 * @param n Number of partitions to create.
 * @return void
 */
void init_partitions(unsigned int n) {
    num_partitions = n;
    partitions = malloc(n * sizeof(Partition));
    for (unsigned int i = 0; i < n; i++) {
        partitions[i].head = NULL;
        partitions[i].unique_keys = NULL;
        partitions[i].unique_key_count = 0;
        pthread_mutex_init(&partitions[i].mutex, NULL);
    }
}

/**
 * @brief Frees allocated memory and cleans up partitions.
 *
 * Deallocates memory used by each partition's key-value list, unique keys, 
 * and mutexes.
 *
 * @param void
 * @return void
 */
void free_partitions() {
    for (unsigned int i = 0; i < num_partitions; i++) {
        KeyValueNode *current = partitions[i].head;
        while (current) {
            KeyValueNode *temp = current;
            current = current->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
        pthread_mutex_destroy(&partitions[i].mutex);

        // Free unique keys array
        for (unsigned int j = 0; j < partitions[i].unique_key_count; j++) {
            free(partitions[i].unique_keys[j]);
        }
        free(partitions[i].unique_keys);
    }
    free(partitions);
}

/**
 * @brief Adds a unique key to a partition's list if not already present.
 *
 * Checks if the given key already exists in the partition's unique key list. 
 * If not, it adds a copy of the key to the list.
 *
 * @param partition Pointer to the Partition to which the key should be added.
 * @param key The key to add to the partition's unique key list.
 * @return void
 */
void add_unique_key(Partition *partition, char *key) {
    for (unsigned int i = 0; i < partition->unique_key_count; i++) {
        if (strcmp(partition->unique_keys[i], key) == 0) {
            return; // Key already exists
        }
    }
    partition->unique_keys = realloc(partition->unique_keys, (partition->unique_key_count + 1) * sizeof(char *));
    partition->unique_keys[partition->unique_key_count] = strdup(key); // Use strdup to ensure a copy
    partition->unique_key_count++;
}

/**
 * @brief Wrapper function for executing the Reducer function.
 *
 * This wrapper unpacks arguments and calls the reducer function with the 
 * key and partition index.
 *
 * @param args Pointer to ReduceWrapperArgs struct containing key, partition index, and reducer function.
 * @return void
 */
void ReduceWrapper(void *args) {
    ReduceWrapperArgs *wrapped_args = (ReduceWrapperArgs *)args;
    wrapped_args->reducer(wrapped_args->key, wrapped_args->partition_idx);
    free(wrapped_args->key); // Free the key copy
    free(wrapped_args);      // Free the wrapper arguments
}

/**
 * @brief Executes the MapReduce process.
 *
 * Sets up partitions, creates a thread pool for map and reduce tasks, and 
 * initiates the map and reduce phases. Waits for all tasks to complete before 
 * cleaning up and freeing resources.
 *
 * @param file_count Number of input files.
 * @param file_names Array of input file names.
 * @param mapper Function pointer to the Mapper function.
 * @param reducer Function pointer to the Reducer function.
 * @param num_workers Number of worker threads.
 * @param num_parts Number of partitions for MapReduce.
 * @return void
 */
void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer,
            unsigned int num_workers, unsigned int num_parts) {
    init_partitions(num_parts);

    ThreadPool_t *tp = ThreadPool_create(num_workers);

    // Map phase
    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(tp, (thread_func_t)mapper, file_names[i]);
    }
    ThreadPool_check(tp); // Ensure all map tasks are completed

    // Reduce phase
    for (unsigned int i = 0; i < num_parts; i++) {
        for (unsigned int j = 0; j < partitions[i].unique_key_count; j++) {
            ReduceWrapperArgs *wrapper_args = malloc(sizeof(ReduceWrapperArgs));
            wrapper_args->key = strdup(partitions[i].unique_keys[j]); // Duplicate the key to prevent corruption
            wrapper_args->partition_idx = i;
            wrapper_args->reducer = reducer;
            ThreadPool_add_job(tp, ReduceWrapper, wrapper_args);
        }
    }
    ThreadPool_check(tp); // Ensure all reduce tasks are completed

    ThreadPool_destroy(tp);
    free_partitions();
}

/**
 * @brief Emits a key-value pair to the appropriate partition.
 *
 * Allocates and adds a key-value pair to the appropriate partition based on 
 * the hash value of the key.
 *
 * @param key The key to be emitted.
 * @param value The value associated with the key.
 * @return void
 */
void MR_Emit(char *key, char *value) {
    if (key == NULL || value == NULL || strlen(key) == 0) {
        return; // Ignore NULL or empty keys
    }

    unsigned int partition_idx = MR_Partitioner(key, num_partitions);

    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->mutex);
    add_unique_key(partition, key);

    KeyValueNode *kv = malloc(sizeof(KeyValueNode));
    kv->key = strdup(key);  // Use strdup to ensure key is copied
    kv->value = strdup(value);  // Use strdup to ensure value is copied
    kv->next = partition->head;
    partition->head = kv;

    pthread_mutex_unlock(&partition->mutex);
}

/**
 * @brief Partitions the data based on a hash function.
 *
 * Computes a hash of the key and returns the partition number for distributing 
 * key-value pairs among partitions.
 *
 * @param key The key to hash.
 * @param num_partitions Total number of partitions available.
 * @return The partition index for the key.
 */
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % num_partitions;
}

/**
 * @brief Retrieves the next value for a given key in a specific partition.
 *
 * Searches the partition for the specified key, returning the next available 
 * value and removing it from the partition.
 *
 * @param key The key to retrieve the next value for.
 * @param partition_idx The index of the partition to search.
 * @return The next value associated with the key or NULL if none remain.
 */
char *MR_GetNext(char *key, unsigned int partition_idx) {
    if (key == NULL || strlen(key) == 0) {
        return NULL; // Ignore NULL or empty keys
    }

    Partition *partition = &partitions[partition_idx];
    pthread_mutex_lock(&partition->mutex);

    KeyValueNode *current = partition->head;
    KeyValueNode *prev = NULL;
    while (current) {
        if (strcmp(current->key, key) == 0) {
            if (prev) prev->next = current->next;
            else partition->head = current->next;

            char *value = strdup(current->value);
            free(current->key);
            free(current->value);
            free(current);
            pthread_mutex_unlock(&partition->mutex);
            return value;
        }
        prev = current;
        current = current->next;
    }
    pthread_mutex_unlock(&partition->mutex);
    return NULL;
}
