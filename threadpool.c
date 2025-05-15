/****************************************************************************

  @file         threadpool.c

  @author       Ahnaful Hoque

  @date         3/11/2024

*******************************************************************************/


#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include "threadpool.h"

/**
 * @brief Helper function to create a new job for the thread pool.
 *
 * Allocates memory for a new job and initializes its function and argument
 * fields. The job is set to the end of the job queue and ready for execution.
 *
 * @param func A pointer to the function to execute as a job.
 * @param arg  The argument to pass to the function when it is executed.
 * @return Pointer to the created job, or NULL if memory allocation fails.
 */
ThreadPool_job_t *create_job(thread_func_t func, void *arg) {
    ThreadPool_job_t *job = malloc(sizeof(ThreadPool_job_t));
    if (job) {
        job->func = func;
        job->arg = arg;
        job->next = NULL;
    }
    return job;
}

/**
 * @brief Creates and initializes a thread pool with a specified number of threads.
 *
 * Initializes the thread pool by creating the specified number of threads,
 * initializing synchronization mechanisms, and preparing the job queue.
 *
 * @param num The number of threads to create in the thread pool.
 * @return Pointer to the created thread pool, or NULL if memory allocation fails.
 */
ThreadPool_t *ThreadPool_create(unsigned int num) {
    ThreadPool_t *tp = malloc(sizeof(ThreadPool_t));
    if (!tp) return NULL;

    tp->num_threads = num;
    tp->shutdown = false;
    tp->active_threads = 0;
    pthread_mutex_init(&tp->mutex, NULL);
    pthread_cond_init(&tp->idle, NULL);
    pthread_cond_init(&tp->all_idle, NULL);

    tp->jobs.size = 0;
    tp->jobs.head = NULL;
    pthread_mutex_init(&tp->jobs.mutex, NULL);
    pthread_cond_init(&tp->jobs.has_jobs, NULL);

    tp->threads = malloc(num * sizeof(pthread_t));
    if (!tp->threads) {
        free(tp);
        return NULL;
    }

    for (unsigned int i = 0; i < num; ++i) {
        pthread_create(&tp->threads[i], NULL, (void *(*)(void *))Thread_run, tp);
    }

    return tp;
}

/**
 * @brief Destroys the thread pool and cleans up resources.
 *
 * Shuts down all threads in the thread pool, waiting for any ongoing
 * jobs to complete, and then frees allocated resources.
 *
 * @param tp Pointer to the thread pool to destroy.
 */
void ThreadPool_destroy(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);
    tp->shutdown = true;
    pthread_cond_broadcast(&tp->jobs.has_jobs);
    pthread_mutex_unlock(&tp->jobs.mutex);

    for (unsigned int i = 0; i < tp->num_threads; ++i) {
        pthread_join(tp->threads[i], NULL);
    }

    pthread_mutex_destroy(&tp->jobs.mutex);
    pthread_cond_destroy(&tp->jobs.has_jobs);
    pthread_mutex_destroy(&tp->mutex);
    pthread_cond_destroy(&tp->idle);
    pthread_cond_destroy(&tp->all_idle);
    free(tp->threads);
    free(tp);
}

/**
 * @brief Adds a job to the thread pool's job queue.
 *
 * Enqueues a job in the thread pool, signaling a worker thread to start processing
 * if it is idle. The job function and argument are stored in the job structure.
 *
 * @param tp   Pointer to the thread pool.
 * @param func Function pointer representing the job to execute.
 * @param arg  Argument to pass to the job function.
 * @return True if the job was successfully added, false on failure.
 */
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg) {
    ThreadPool_job_t *job = create_job(func, arg);
    if (!job) return false;

    pthread_mutex_lock(&tp->jobs.mutex);
    if (tp->jobs.head == NULL) {
        tp->jobs.head = job;
    } else {
        ThreadPool_job_t *current = tp->jobs.head;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = job;
    }
    tp->jobs.size++;
    pthread_cond_signal(&tp->jobs.has_jobs);
    pthread_mutex_unlock(&tp->jobs.mutex);

    return true;
}

/**
 * @brief Retrieves the next job from the job queue.
 *
 * This function retrieves and removes a job from the job queue. If the job queue is
 * empty and the thread pool is not shutting down, it will wait for a job to become available.
 *
 * @param tp Pointer to the thread pool.
 * @return Pointer to the retrieved job, or NULL if the pool is shutting down.
 */
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->jobs.mutex);
    while (tp->jobs.size == 0 && !tp->shutdown) {
        pthread_cond_wait(&tp->jobs.has_jobs, &tp->jobs.mutex);
    }

    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->jobs.mutex);
        return NULL;
    }

    ThreadPool_job_t *job = tp->jobs.head;
    if (job != NULL) {
        tp->jobs.head = job->next;
        tp->jobs.size--;
    }

    pthread_mutex_unlock(&tp->jobs.mutex);
    return job;
}

/**
 * @brief Start routine for each thread in the thread pool.
 *
 * Each thread repeatedly fetches a job from the queue and executes it. The thread
 * exits when the thread pool is shutting down and there are no more jobs to execute.
 *
 * @param tp Pointer to the thread pool.
 * @return NULL upon exit.
 */
void *Thread_run(ThreadPool_t *tp) {
    while (true) {
        ThreadPool_job_t *job = ThreadPool_get_job(tp);
        if (job == NULL) break;  // Exit if the pool is shutting down

        // Increment the active threads count
        pthread_mutex_lock(&tp->mutex);
        tp->active_threads++;
        pthread_mutex_unlock(&tp->mutex);

        // Execute the job's function with its argument
        job->func(job->arg);
        free(job);

        // Decrement the active threads count
        pthread_mutex_lock(&tp->mutex);
        tp->active_threads--;

        // Signal if all threads are idle and the job queue is empty
        if (tp->active_threads == 0 && tp->jobs.size == 0) {
            pthread_cond_signal(&tp->all_idle);
        }

        pthread_mutex_unlock(&tp->mutex);
    }

    pthread_exit(NULL);
}

/**
 * @brief Waits until all threads are idle and the job queue is empty.
 *
 * This function blocks until all jobs are processed and no threads are
 * actively executing tasks. Useful for synchronizing job completion.
 *
 * @param tp Pointer to the thread pool.
 */
void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->mutex);
    while (tp->jobs.size > 0 || tp->active_threads > 0) {
        pthread_cond_wait(&tp->all_idle, &tp->mutex);
    }
    pthread_mutex_unlock(&tp->mutex);
}
