#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <stdatomic.h>
/* Fast single-character read: unlocked on POSIX, plain fgetc on Windows. */

#define GETC_FAST(f) getc_unlocked(f)

double benchmark_seconds = 0.0;

//STRUCTURES
/* Event classification */
typedef enum {
    INFO,
    WARN,
    ERROR
} Level;

/* One parsed event record */
typedef struct {
    int id;
    Level level;
    int amount;
} Event;
/*
 * OWNERSHIP RULE (exactly one owner at any time; the owner frees):
 *  - Batch (char **lines): the producer owns it until create_task() succeeds,
 *    then the Task owns it (batch pointer set to NULL in the producer).
 *  - Task: the producer owns it until queue_push() returns 1 or 2 (queue
 *    now owns it). After queue_pop() the worker owns it and frees it with
 *    free_task() as soon as process_task() returns.
 *  - Result: the worker owns it until queue_push() to the result queue returns
 *    1 or 2. After queue_pop() the collector owns it; it either parks it in
 *    the reorder buffer (still owned by the collector) or frees it after
 *    writing it.
 *  - Abort: items still inside a queue are freed by main through
 *    cleanup_task_queue()/cleanup_result_queue() after all threads have been
 *    joined; the collector frees its own reorder buffer on exit. Push return
 *    values 0/-1 mean "not inserted", so the caller still owns the item.
 */
typedef struct {
    int task_id;
    int first_line;
    int count;
    char **lines;
} Task;

typedef struct {
    int task_id;

    int64_t records;
    int64_t info;
    int64_t warn;
    int64_t error;

    int64_t amount_sum;

    Event *alerts;
    int alert_count;
} Result;
typedef enum {
    QUEUE_OPEN,
    QUEUE_CLOSED,
    QUEUE_ABORTED
} QueueState;
typedef struct {
    void **items;

    int capacity;
    int head;
    int tail;
    int count;

    QueueState state;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} Queue;
typedef struct {
    Queue task_queue;
    Queue result_queue;

    pthread_mutex_t credit_mutex;
    pthread_cond_t credit_cond;

    int64_t issued;
    int64_t committed;

    int window;

    atomic_bool aborted;

    char error_message[256];

} Pipeline;

typedef struct {
    char *mode;
    char *input_path;
    char *output_path;

    int threads;
    int queue_size;
    int batch_size;

    bool verify;

    bool mode_set;
    bool input_set;
    bool output_set;
    bool threads_set;
    bool queue_set;
    bool batch_set;

} Config;


typedef struct {
    Result **items;
    int capacity;
} PendingResults;

typedef struct {
    int64_t records;
    int64_t info;
    int64_t warn;
    int64_t error;
    int64_t amount_sum;
    int64_t alerts;
} Summary;
typedef struct {
    Pipeline *pipeline;
    FILE *output;
    Summary *summary;
} CollectorArgs;

//FUNCTION 
int parse_integer(const char *text, int min, int max, int *value)
{
    char *end;
    long number;

    errno = 0;
    number = strtol(text, &end, 10);

    if (errno != 0 || end == text || *end != '\0') {
        return 0;
    }

    if (number < min || number > max) {
        return 0;
    }

    *value = (int)number;

    return 1;
}

int parse_arguments(int argc, char *argv[], Config *config)
{
    memset(config, 0, sizeof(Config));

    for (int i = 1; i < argc; i++) {

        if (strcmp(argv[i], "--mode") == 0) {

            if (config->mode_set || i + 1 >= argc)
                return 0;

            config->mode = argv[++i];

            if (strcmp(config->mode, "serial") != 0 &&
                strcmp(config->mode, "parallel") != 0)
                return 0;

            config->mode_set = true;
        }

        else if (strcmp(argv[i], "--input") == 0) {

            if (config->input_set || i + 1 >= argc)
                return 0;

            config->input_path = argv[++i];
            config->input_set = true;
        }

        else if (strcmp(argv[i], "--output") == 0) {

            if (config->output_set || i + 1 >= argc)
                return 0;

            config->output_path = argv[++i];
            config->output_set = true;
        }

        else if (strcmp(argv[i], "--threads") == 0) {

            if (config->threads_set || i + 1 >= argc)
                return 0;

            if (!parse_integer(argv[++i], 1, 32,
                               &config->threads))
                return 0;

            config->threads_set = true;
        }

        else if (strcmp(argv[i], "--queue") == 0) {

            if (config->queue_set || i + 1 >= argc)
                return 0;

            if (!parse_integer(argv[++i], 1, 64,
                               &config->queue_size))
                return 0;

            config->queue_set = true;
        }

        else if (strcmp(argv[i], "--batch") == 0) {

            if (config->batch_set || i + 1 >= argc)
                return 0;

            if (!parse_integer(argv[++i], 1, 1024,
                               &config->batch_size))
                return 0;

            config->batch_set = true;
        }

        else if (strcmp(argv[i], "--verify") == 0) {

            if (config->verify)
                return 0;

            config->verify = true;
        }

        else {
            return 0;
        }
    }

    if (!config->mode_set ||
        !config->input_set ||
        !config->output_set)
        return 0;

    if (strcmp(config->mode, "parallel") == 0) {

        if (!config->threads_set ||
            !config->queue_set ||
            !config->batch_set)
            return 0;
    }

    else {

        if (config->threads_set ||
            config->queue_set ||
            config->batch_set ||
            config->verify)
            return 0;
    }

    return 1;
}
// function to parse a line of text into an Event structure
int parse_event(const char *line, int expected_id, Event *event)
{
        char buffer[129];                 /* 128 bytes + NUL */
    size_t length = strlen(line);

    /* Strip the LF first: the 128-byte limit excludes it. */
    if (length > 0 && line[length - 1] == '\n') {
        length--;
    }

    if (length == 0 || length > 128) {
        return 0;
    }

    memcpy(buffer, line, length);
    buffer[length] = '\0';

    char *first = strchr(buffer, ',');
    if (first == NULL)
        return 0;

    char *second = strchr(first + 1, ',');
    if (second == NULL)
        return 0;

    /* Reject extra fields */
    if (strchr(second + 1, ',') != NULL)
        return 0;

    *first = '\0';
    *second = '\0';

    /* Validate ID: positive decimal digits only */
    if (buffer[0] == '\0')
        return 0;

    for (char *p = buffer; *p; p++) {
        if (*p < '0' || *p > '9')
            return 0;
    }

    char *end;
    errno = 0;

    long id = strtol(buffer, &end, 10);

    if (errno != 0 || *end != '\0' ||
        id != expected_id || id > 10000000)
        return 0;

    /* Validate level */
    Level level;

    if (strcmp(first + 1, "INFO") == 0) {
        level = INFO;
    }
    else if (strcmp(first + 1, "WARN") == 0) {
        level = WARN;
    }
    else if (strcmp(first + 1, "ERROR") == 0) {
        level = ERROR;
    }
    else {
        return 0;
    }

   
/* Validate amount: optional sign followed by digits only. */
char *amount_text = second + 1;
char *p = amount_text;

if (*p == '+' || *p == '-') {
    p++;
}

if (*p == '\0') {
    return 0;
}

while (*p != '\0') {
    if (*p < '0' || *p > '9') {
        return 0;
    }

    p++;
}

/* Convert amount after strict validation. */
char *amount_end;
errno = 0;

long amount = strtol(amount_text, &amount_end, 10);

if (errno != 0 || *amount_end != '\0' ||
    amount < -1000000 || amount > 1000000) {
    return 0;
}


    event->id = (int)id;
    event->level = level;
    event->amount = (int)amount;

    return 1;
}


// Initialize the queue with the specified capacity
int queue_init(Queue *queue, int capacity)
{
    queue->items = calloc(capacity, sizeof(void *));

    if (queue->items == NULL) {
        return -1;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->state = QUEUE_OPEN;

    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        free(queue->items);
        queue->items = NULL;
        return -1;
    }

    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {

        if (pthread_mutex_destroy(&queue->mutex) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot destroy queue mutex\n");
        }

        free(queue->items);
        queue->items = NULL;
        return -1;
    }

    if (pthread_cond_init(&queue->not_full, NULL) != 0) {

        if (pthread_cond_destroy(&queue->not_empty) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot destroy not_empty condition\n");
        }

        if (pthread_mutex_destroy(&queue->mutex) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot destroy queue mutex\n");
        }

        free(queue->items);
        queue->items = NULL;
        return -1;
    }

    return 0;
}

// function to push an item onto the queue
int queue_push(Queue *queue, void *item)
{
    if (pthread_mutex_lock(&queue->mutex) != 0) {
        return -1;
    }

    while (queue->count == queue->capacity &&
           queue->state == QUEUE_OPEN) {

        if (pthread_cond_wait(&queue->not_full,
                              &queue->mutex) != 0) {

            if (pthread_mutex_unlock(&queue->mutex) != 0) {
                fprintf(stderr, "Fatal error: Cannot unlock queue mutex\n");
                _Exit(EXIT_FAILURE);
            }

            return -1;
        }
    }

    if (queue->state != QUEUE_OPEN) {
        int aborted = (queue->state == QUEUE_ABORTED);

        if (pthread_mutex_unlock(&queue->mutex) != 0) {
            fprintf(stderr, "Fatal error: Cannot unlock queue mutex\n");
            _Exit(EXIT_FAILURE);
        }

        return aborted ? -1 : 0;
    }

    /* The queue takes ownership of the item here. */
    queue->items[queue->tail] = item;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    int signal_status =
        pthread_cond_signal(&queue->not_empty);

    int unlock_status =
        pthread_mutex_unlock(&queue->mutex);

    if (unlock_status != 0) {
        fprintf(stderr, "Fatal error: Cannot unlock queue mutex\n");
        _Exit(EXIT_FAILURE);
    }

    if (signal_status != 0) {
        return 2; /* Item inserted, but signalling failed. */
    }

    return 1;
}

// function to pop an item from the queue
int queue_pop(Queue *queue, void **item)
{
    if (pthread_mutex_lock(&queue->mutex) != 0) {
        return -1;
    }

    while (queue->count == 0 &&
           queue->state == QUEUE_OPEN) {

        if (pthread_cond_wait(&queue->not_empty,
                              &queue->mutex) != 0) {

            if (pthread_mutex_unlock(&queue->mutex) != 0) {
                fprintf(stderr,
                        "Fatal error: Cannot unlock queue mutex\n");
                _Exit(EXIT_FAILURE);
            }

            return -1;
        }
    }

    if (queue->state == QUEUE_ABORTED) {
        if (pthread_mutex_unlock(&queue->mutex) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot unlock queue mutex\n");
            _Exit(EXIT_FAILURE);
        }

        return -1;
    }

    if (queue->count == 0) {
        if (pthread_mutex_unlock(&queue->mutex) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot unlock queue mutex\n");
            _Exit(EXIT_FAILURE);
        }

        return 0;
    }

    /* Ownership transfers from the queue to the caller. */
    *item = queue->items[queue->head];

    queue->items[queue->head] = NULL;
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    int signal_status =
        pthread_cond_signal(&queue->not_full);

    int unlock_status =
        pthread_mutex_unlock(&queue->mutex);

    if (unlock_status != 0) {
        fprintf(stderr,
                "Fatal error: Cannot unlock queue mutex\n");
        _Exit(EXIT_FAILURE);
    }

    if (signal_status != 0) {
        return 2;
    }

    return 1;
}

// function to close the queue
int queue_close(Queue *queue)
{
    if (pthread_mutex_lock(&queue->mutex) != 0) {
        return -1;
    }

    if (queue->state == QUEUE_OPEN) {
        queue->state = QUEUE_CLOSED;
    }

    int result1 = pthread_cond_broadcast(&queue->not_empty);
    int result2 = pthread_cond_broadcast(&queue->not_full);
    int result3 = pthread_mutex_unlock(&queue->mutex);

    if (result1 != 0 || result2 != 0 || result3 != 0) {
        return -1;
    }

    return 0;
}

// function to abort the queue
int queue_abort(Queue *queue)
{
    if (pthread_mutex_lock(&queue->mutex) != 0)
        return -1;

    queue->state = QUEUE_ABORTED;

    int result1 = pthread_cond_broadcast(&queue->not_empty);
    int result2 = pthread_cond_broadcast(&queue->not_full);
    int result3 = pthread_mutex_unlock(&queue->mutex);

    if (result1 != 0 || result2 != 0 || result3 != 0)
        return -1;

    return 0;
}

// function to destroy the queue and free its resources
int queue_destroy(Queue *queue)
{
    int failed = 0;

    if (pthread_cond_destroy(&queue->not_empty) != 0)
        failed = 1;

    if (pthread_cond_destroy(&queue->not_full) != 0)
        failed = 1;

    if (pthread_mutex_destroy(&queue->mutex) != 0)
        failed = 1;

    free(queue->items);

    queue->items = NULL;

    return failed ? -1 : 0;
}
// function to free a Task structure and its associated resources
void free_task(Task *task)
{
    if (task == NULL)
        return;

    for (int i = 0; i < task->count; i++) {
        free(task->lines[i]);
    }

    free(task->lines);
    free(task);
}

// function to free a Result structure and its associated resources
void free_result(Result *result)
{
    if (result == NULL)
        return;

    free(result->alerts);
    free(result);
}

/* Free tasks remaining in the task queue after all threads stop. */
int cleanup_task_queue(Queue *queue)
{
    if (pthread_mutex_lock(&queue->mutex) != 0) {
        return -1;
    }

    while (queue->count > 0) {
        Task *task = (Task *)queue->items[queue->head];

        queue->items[queue->head] = NULL;
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;

        free_task(task);
    }

    int status = pthread_mutex_unlock(&queue->mutex);
    return status == 0 ? 0 : -1;
}

/* Free results remaining in the result queue after all threads stop. */
int cleanup_result_queue(Queue *queue)
{
    if (pthread_mutex_lock(&queue->mutex) != 0) {
        return -1;
    }

    while (queue->count > 0) {
        Result *result = (Result *)queue->items[queue->head];

        queue->items[queue->head] = NULL;
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;

        free_result(result);
    }

    int status = pthread_mutex_unlock(&queue->mutex);
    return status == 0 ? 0 : -1;
}


//task processing function
Result *process_task(Task *task)
{
    Result *result = calloc(1, sizeof(Result));

    if (result == NULL)
        return NULL;

    result->task_id = task->task_id;

    result->alerts = calloc(task->count, sizeof(Event));

    if (result->alerts == NULL) {
        free(result);
        return NULL;
    }

    for (int i = 0; i < task->count; i++) {

        Event event;

        int expected_id = task->first_line + i;

        if (!parse_event(task->lines[i],
                         expected_id,
                         &event)) {
            free_result(result);
            return NULL;
        }

        result->records++;
        result->amount_sum += event.amount;

        if (event.level == INFO) {
            result->info++;
        }
        else if (event.level == WARN) {
            result->warn++;
        }
        else if (event.level == ERROR) {

            result->error++;

            result->alerts[result->alert_count] = event;
            result->alert_count++;
        }
    }

    return result;
}
//Check Pipeline is abort
int pipeline_is_aborted(Pipeline *pipeline)
{
    return atomic_load(&pipeline->aborted);
}
// pipeline abort function

void pipeline_abort(Pipeline *pipeline, const char *message)
{
    int lock_status =
        pthread_mutex_lock(&pipeline->credit_mutex);

    if (lock_status == 0) {

        /* Atomically mark the pipeline as aborted. */
        bool already_aborted =
            atomic_exchange(&pipeline->aborted, true);

        /* Preserve the first error message. */
        if (!already_aborted) {
            snprintf(
                pipeline->error_message,
                sizeof(pipeline->error_message),
                "%s",
                message
            );
        }

        /* Wake threads waiting for credit while holding the mutex. */
        int broadcast_status =
            pthread_cond_broadcast(&pipeline->credit_cond);

        int unlock_status =
            pthread_mutex_unlock(&pipeline->credit_mutex);

        if (unlock_status != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot unlock credit mutex during abort\n");
            _Exit(EXIT_FAILURE);
        }

        if (broadcast_status != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot broadcast credit condition\n");
        }

    } else {

        /* Best-effort fallback if the credit mutex cannot be locked. */
        bool already_aborted =
            atomic_exchange(&pipeline->aborted, true);

        if (!already_aborted) {
            snprintf(
                pipeline->error_message,
                sizeof(pipeline->error_message),
                "%s",
                message
            );
        }

        fprintf(stderr,
                "Fatal error: Cannot lock credit mutex during abort\n");

        if (pthread_cond_broadcast(&pipeline->credit_cond) != 0) {
            fprintf(stderr,
                    "Fatal error: Cannot broadcast credit condition\n");
        }
    }

    /* Abort both queues and wake their waiting threads. */
    if (queue_abort(&pipeline->task_queue) != 0) {
        fprintf(stderr,
                "Fatal error: Cannot abort task queue\n");
    }

    if (queue_abort(&pipeline->result_queue) != 0) {
        fprintf(stderr,
                "Fatal error: Cannot abort result queue\n");
    }
}


// worker thread function

void *worker(void *arg)
{
    Pipeline *pipeline = (Pipeline *)arg;

    while (1) {

        void *item = NULL;

        int status = queue_pop(
            &pipeline->task_queue,
            &item
        );

        if (status == 0) {
            break;
        }

        if (status == -1) {
            if (!pipeline_is_aborted(pipeline)) {
                pipeline_abort(
                    pipeline,
                    "Cannot receive task from queue"
                );
            }
            break;
        }
        if (status == 2) {
    free_task((Task *)item);

    pipeline_abort(
        pipeline,
        "Cannot signal task queue condition"
    );

    break;
}

        Task *task = (Task *)item;

        Result *result = process_task(task);

        free_task(task);

        if (result == NULL) {
            pipeline_abort(
                pipeline,
                "Invalid record or memory allocation error"
            );
            break;
        }

        int push_status = queue_push(
            &pipeline->result_queue,
            result
        );

        if (push_status != 1) {

    /* Do not free a result already owned by the queue. */
    if (push_status != 2) {
        free_result(result);
    }

    if (!pipeline_is_aborted(pipeline)) {
        pipeline_abort(
            pipeline,
            "Cannot submit result to queue"
        );
    }

    break;
}
    }

    return NULL;
}


// function to wait for available credit in the pipeline
// Function to wait for available credit in the pipeline.
int wait_for_credit(Pipeline *pipeline)
{
    if (pthread_mutex_lock(&pipeline->credit_mutex) != 0) {
        pipeline_abort(
            pipeline,
            "Cannot lock credit mutex"
        );
        return 0;
    }

    while (!pipeline_is_aborted(pipeline) &&
           pipeline->issued - pipeline->committed >= pipeline->window) {

        int wait_status = pthread_cond_wait(
            &pipeline->credit_cond,
            &pipeline->credit_mutex
        );

        if (wait_status != 0) {
            pthread_mutex_unlock(&pipeline->credit_mutex);

            pipeline_abort(
                pipeline,
                "Cannot wait on credit condition"
            );

            return 0;
        }
    }

    int available = !pipeline_is_aborted(pipeline);

    if (pthread_mutex_unlock(&pipeline->credit_mutex) != 0) {
        fprintf(stderr,
                "Fatal error: Cannot unlock credit mutex\n");
        _Exit(EXIT_FAILURE);
    }

    return available;
}

//function to create tasks
Task *create_task(char **lines, int count,int task_id, int first_line) {
    Task *task = malloc(sizeof(Task));

    if (task == NULL) {
        return NULL;
    }

    task->task_id = task_id;
    task->first_line = first_line;
    task->count = count;
    task->lines = lines;

    return task;
}

// functionn to read line by line

char *read_line(FILE *input, size_t *length, int *read_error) {
    size_t capacity = 128;
    size_t count = 0;

    *read_error = 0;

    char *line = malloc(capacity + 1);

    if (line == NULL) {
        *read_error = 1;
        return NULL;
    }

    int ch;

    while ((ch = GETC_FAST(input)) != EOF) {

        if (count == capacity) {
            capacity *= 2;

            char *temp = realloc(line, capacity + 1);

            if (temp == NULL) {
                free(line);
                *read_error = 1;
                return NULL;
            }

            line = temp;
        }

        line[count++] = (char)ch;
        if (count > 129) break;   /* too long; producer will reject it */

        if (ch == '\n') {
            break;
        }
    }

    if (ferror(input)) {
        free(line);
        *read_error = 1;
        return NULL;
    }

    if (count == 0 && feof(input)) {
        free(line);
        return NULL;
    }

    line[count] = '\0';
    *length = count;

    return line;
}

// function to run the producer thread
// Function to produce tasks from the input file
int run_producer(Pipeline *pipeline, const Config *config) {

    FILE *input = fopen(config->input_path, "rb");

    if (input == NULL) {
        pipeline_abort(pipeline, "Cannot open input file");
        return 0;
    }

    char **batch = malloc(sizeof(char *) * config->batch_size);

    if (batch == NULL) {
        fclose(input);
        pipeline_abort(pipeline, "Memory allocation failed");
        return 0;
    }

    int count = 0;
    int task_id = 0;
    int physical_line = 0;
    int first_line = 1;

    char *line = NULL;
    int read_error = 0;
    size_t length = 0;

    while (1) {

        line = read_line(input, &length,&read_error);
        if (read_error) {
    pipeline_abort(pipeline, "Input read or memory allocation error");
    break;
}

if (line == NULL) {
    break;
}

        physical_line++;

        size_t record_length = length;

        if (record_length > 0 &&
            line[record_length - 1] == '\n') {
            record_length--;
        }

        if (record_length > 128 ||
            physical_line > 10000000) {

            free(line);
            line = NULL;

            pipeline_abort(pipeline, "Invalid input record");
            break;
        }

        /* The batch takes over the line buffer: no second copy. */
        batch[count] = line;
        line = NULL;

        count++;

        /* Submit a full batch. */
        if (count == config->batch_size) {

            if (!wait_for_credit(pipeline)) {
                break;
            }

            Task *task = create_task(
                batch,
                count,
                task_id,
                first_line
            );

            if (task == NULL) {
                pipeline_abort(pipeline, "Memory allocation failed");
                break;
            }

            /* Task now owns the batch. */
            batch = NULL;
            

if (pthread_mutex_lock(&pipeline->credit_mutex) != 0) {
    free_task(task);

    pipeline_abort(
        pipeline,
        "Cannot lock credit mutex"
    );

    break;
}

pipeline->issued++;

if (pthread_mutex_unlock(&pipeline->credit_mutex) != 0) {
    fprintf(stderr,
            "Fatal error: Cannot unlock credit mutex\n");
    _Exit(EXIT_FAILURE);
}

int status = queue_push(&pipeline->task_queue, task);

            if (status != 1) {

    /* Status 2 means the queue already owns the task. */
    if (status != 2) {
        free_task(task);
    }

    pipeline_abort(pipeline, "Cannot submit task");
    break;
}
            task_id++;
            first_line = physical_line + 1;
            count = 0;

            /* Allocate the next batch. */
            batch = malloc(sizeof(char *) * config->batch_size);

            if (batch == NULL) {
                pipeline_abort(pipeline, "Memory allocation failed");
                break;
            }
        }
    }

    free(line);

    

    /* Submit the final partial batch. */
        /* Submit the final partial batch. */
    if (!pipeline_is_aborted(pipeline) && count > 0) {

        if (wait_for_credit(pipeline)) {

            Task *task = create_task(batch, count, task_id, first_line);

            if (task == NULL) {
                /* batch is still ours; freed at the end of this function */
                pipeline_abort(pipeline, "Memory allocation failed");
            }
            else {
                batch = NULL;   /* task now owns the batch */

                if (pthread_mutex_lock(&pipeline->credit_mutex) != 0) {
                    free_task(task);
                    pipeline_abort(pipeline, "Cannot lock credit mutex");
                }
                else {
                    pipeline->issued++;

                    if (pthread_mutex_unlock(&pipeline->credit_mutex) != 0) {
                        fprintf(stderr,
                                "Fatal error: Cannot unlock credit mutex\n");
                        _Exit(EXIT_FAILURE);
                    }

                    int status = queue_push(&pipeline->task_queue, task);

                    if (status != 1) {
                        /* status 2 = queue already owns the task */
                        if (status != 2) {
                            free_task(task);
                        }
                        pipeline_abort(pipeline, "Cannot submit final task");
                    }
                }
            }
        }
    }

    /* Free any batch that was not transferred to a task. */
    if (batch != NULL) {

        for (int i = 0; i < count; i++) {
            free(batch[i]);
        }

        free(batch);
    }

    if (fclose(input) != 0 && !pipeline->aborted) {
        pipeline_abort(pipeline, "Error closing input file");
    }

    /* Tell workers that no more tasks will arrive. */
    /* Tell workers that no more tasks will arrive. */
if (!pipeline_is_aborted(pipeline)) {
    if (queue_close(&pipeline->task_queue) != 0) {
        pipeline_abort(
            pipeline,
            "Cannot close task queue"
        );
    }
}

    return !pipeline->aborted;
}
// function to update the summary with the results from a processed task
void update_summary(Summary *summary, Result *result) {
    summary->records += result->records;
    summary->info += result->info;
    summary->warn += result->warn;
    summary->error += result->error;
    summary->amount_sum += result->amount_sum;
    summary->alerts += result->alert_count;
}

// function to write the alerts to the output file
int write_alerts(FILE *output, Result *result) {
    for (int i = 0; i < result->alert_count; i++) {

        if (fprintf(output, "%d,%d\n",
                    result->alerts[i].id,
                    result->alerts[i].amount) < 0) {
            return 0;
        }
    }

    return 1;
}

// function to run the collector thread
void *collector(void *arg) {
    CollectorArgs *args = arg;

    Pipeline *pipeline = args->pipeline;
    FILE *output = args->output;
    Summary *summary = args->summary;

    int next_task = 0;

    int capacity = pipeline->window;

    Result **pending = calloc(capacity, sizeof(Result *));

    if (pending == NULL) {
        pipeline_abort(pipeline, "Memory allocation failed");
        return NULL;
    }

    Result *result;

    while (1) {

        int status = queue_pop(&pipeline->result_queue,
                               (void **)&result);

        if (status == 0) {
            break;
        }

        if (status == -1) {
            break;
        }
        if (status == 2) {
    free_result(result);

    pipeline_abort(
        pipeline,
        "Cannot signal result queue condition"
    );

    break;
}

        int index = result->task_id % capacity;

        if (pending[index] != NULL) {
            free_result(result);
            pipeline_abort(pipeline, "Duplicate task result");
            break;
        }

        pending[index] = result;

        /* Commit results in their original order. */
        while (pending[next_task % capacity] != NULL) {

            Result *current =
                pending[next_task % capacity];

            if (current->task_id != next_task) {
                break;
            }

            if (!write_alerts(output, current)) {
                pipeline_abort(pipeline, "Error writing output");
                free_result(current);
                pending[next_task % capacity] = NULL;
                goto cleanup;
            }

            update_summary(summary, current);

            free_result(current);
            pending[next_task % capacity] = NULL;

            next_task++;

            /* Notify producer that a task was committed. */
            /* Notify producer that a task was committed. */
if (pthread_mutex_lock(&pipeline->credit_mutex) != 0) {
    pipeline_abort(
        pipeline,
        "Cannot lock credit mutex"
    );
    goto cleanup;
}

pipeline->committed++;

int signal_status =
    pthread_cond_broadcast(&pipeline->credit_cond);

int unlock_status =
    pthread_mutex_unlock(&pipeline->credit_mutex);

if (unlock_status != 0) {
    fprintf(stderr,
            "Fatal error: Cannot unlock credit mutex\n");
    _Exit(EXIT_FAILURE);
}

if (signal_status != 0) {
    pipeline_abort(
        pipeline,
        "Cannot notify producer about committed task"
    );
    goto cleanup;
}
        }
    }

cleanup:

    for (int i = 0; i < capacity; i++) {
        if (pending[i] != NULL) {
            free_result(pending[i]);
        }
    }

    free(pending);

    return NULL;
}

//temporary-file helper
char *create_temp_path(const char *output_path) {
    size_t length = strlen(output_path) + 12;

    char *path = malloc(length);

    if (path == NULL) {
        return NULL;
    }

    snprintf(path, length, "%s.tmp.XXXXXX", output_path);

    return path;
}

// function to run the parallel processing pipeline
int run_parallel(const Config *config, Summary *summary,  char **result_path) {

    Pipeline pipeline;
    int success = 0;
    FILE *output = NULL;
    char *temp_path = NULL;

    struct timespec benchmark_start;
    struct timespec benchmark_end;

    memset(&pipeline, 0, sizeof(Pipeline));
    atomic_init(&pipeline.aborted, false); //initialize atomic variabler

    pipeline.window = 2 * config->threads + config->queue_size;

    /* Initialize queues. */
    if (queue_init(&pipeline.task_queue, config->queue_size) != 0) {
         
        return 0;
    }

    if (queue_init(&pipeline.result_queue, config->queue_size) != 0) {
        
        queue_destroy(&pipeline.task_queue);
        return 0;
    }

    /* Initialize synchronization. */
    /* Initialize credit synchronization. */
if (pthread_mutex_init(&pipeline.credit_mutex, NULL) != 0) {
    fprintf(stderr, "Error: Cannot initialize credit mutex\n");
    queue_destroy(&pipeline.task_queue);
    queue_destroy(&pipeline.result_queue);
    return 0;
}

if (pthread_cond_init(&pipeline.credit_cond, NULL) != 0) {
    fprintf(stderr, "Error: Cannot initialize credit condition variable\n");
    pthread_mutex_destroy(&pipeline.credit_mutex);
    queue_destroy(&pipeline.task_queue);
    queue_destroy(&pipeline.result_queue);
    return 0;
}

    /* Create temporary output file. */
    temp_path = create_temp_path(config->output_path);

    if (temp_path == NULL) {
        pipeline_abort(&pipeline, "Memory allocation failed");
        goto cleanup;
    }

    int fd = mkstemp(temp_path);
    

    if (fd == -1) {
        pipeline_abort(&pipeline, "Cannot create temporary output");
        goto cleanup;
    }
    if (chmod(temp_path, 0644) != 0)
    {
    close(fd);
    unlink(temp_path);
    pipeline_abort(&pipeline, "Cannot set output permissions");
    goto cleanup;
    }

   output = fdopen(fd, "w");

    if (output == NULL) {
        close(fd);
        unlink(temp_path);
        pipeline_abort(&pipeline, "Cannot open temporary output");
        goto cleanup;
    }

    /* Write the required CSV header. */
    if (fprintf(output, "id,amount\n") < 0) {
        pipeline_abort(&pipeline, "Cannot write output header");
    }

    //Crreate timerer
    clock_gettime(CLOCK_MONOTONIC, &benchmark_start);
    /* Create worker threads. */
    pthread_t workers[32];
    int workers_started = 0;

    for (int i = 0; i < config->threads; i++) {

        if (pthread_create(&workers[i], NULL,
                           worker, &pipeline) != 0) {

            pipeline_abort(&pipeline, "Cannot create worker thread");
            break;
        }

        workers_started++;
    }

    /* Prepare collector arguments. */
    CollectorArgs collector_args = {
        .pipeline = &pipeline,
        .output = output,
        .summary = summary
    };

    pthread_t collector_thread;
    int collector_started = 0;

    /* Start collector before producer. */
    if (!pipeline_is_aborted(&pipeline)) {

        if (pthread_create(&collector_thread, NULL,
                           collector, &collector_args) == 0) {

            collector_started = 1;

        } else {
            pipeline_abort(&pipeline, "Cannot create collector thread");
        }
    }

    /* Main thread acts as producer. */
    if (!pipeline_is_aborted(&pipeline)) {
        run_producer(&pipeline, config);
    }

    /* Ensure workers stop if an error occurred. */
    if (pipeline_is_aborted(&pipeline)) {
        queue_abort(&pipeline.task_queue);
        queue_abort(&pipeline.result_queue);
    }

    /* Join all workers. */
    for (int i = 0; i < workers_started; i++) {
    int join_status = pthread_join(workers[i], NULL);

    if (join_status != 0) {
        fprintf(stderr,
                "Fatal error: Cannot join worker thread (%d)\n",
                join_status);
        _Exit(EXIT_FAILURE);
    }
}

    /* Close result queue after workers finish. */
   /* Close result queue after workers finish. */
if (!pipeline_is_aborted(&pipeline)) {
    if (queue_close(&pipeline.result_queue) != 0) {
        pipeline_abort(
            &pipeline,
            "Cannot close result queue"
        );
    }
}


    
    /* Join collector. */
    if (collector_started) {
    int join_status = pthread_join(collector_thread, NULL);

    if (join_status != 0) {
        fprintf(stderr,
                "Fatal error: Cannot join collector thread (%d)\n",
                join_status);
        _Exit(EXIT_FAILURE);
    }
}

   success = !pipeline_is_aborted(&pipeline);

/* Flush and close output. */
if (fflush(output) != 0) {
    success = 0;
}

if (fclose(output) != 0) {
    success = 0;
}

output = NULL;

/* Record benchmark time after successful close. */
if (success) {
    clock_gettime(CLOCK_MONOTONIC, &benchmark_end);

    benchmark_seconds =
        (benchmark_end.tv_sec - benchmark_start.tv_sec) +
        (benchmark_end.tv_nsec - benchmark_start.tv_nsec)
        / 1000000000.0;

      
      fprintf(stderr, "BENCHMARK_SECONDS=%.6f\n", benchmark_seconds);
  
}

/* Publish only after successful processing. */
if (success) {
    *result_path = temp_path;
    temp_path = NULL;
} else {
    if (temp_path != NULL) {
        unlink(temp_path);
    }

    if (pipeline.error_message[0] != '\0') {
        fprintf(stderr, "Error: %s\n",
                pipeline.error_message);
    }
}

cleanup:

if (output != NULL) {
    fclose(output);
}

if (temp_path != NULL) {
    unlink(temp_path);
    free(temp_path);
}
cleanup_task_queue(&pipeline.task_queue);
cleanup_result_queue(&pipeline.result_queue);

queue_destroy(&pipeline.task_queue);
queue_destroy(&pipeline.result_queue);

pthread_cond_destroy(&pipeline.credit_cond);
pthread_mutex_destroy(&pipeline.credit_mutex);

return success;
}



// Function to run serial processing
int run_serial(const Config *config, Summary *summary, char **result_path) {

    FILE *input = NULL;
    FILE *output = NULL;

    char *temp_path = NULL;
    char line[130];

    size_t line_number = 0;

    int success = 1;
    int fd = -1;

    *result_path = NULL;

    /* Open input file. */
    input = fopen(config->input_path, "rb");

    if (input == NULL) {
        perror("Cannot open input file");
        return 0;
    }

    /* Create temporary output file. */
    temp_path = create_temp_path(config->output_path);

    if (temp_path == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        success = 0;
        goto cleanup;
    }

    fd = mkstemp(temp_path);
    

    if (fd == -1) {
        perror("Cannot create temporary output");
        success = 0;
        goto cleanup;
    }
    if (chmod(temp_path, 0644) != 0) {
    perror("Cannot set output permissions");
    close(fd);
    fd = -1;
    success = 0;
    goto cleanup;
}

    output = fdopen(fd, "w");

    if (output == NULL) {
        close(fd);
        fd = -1;
        perror("Cannot open temporary output");
        success = 0;
        goto cleanup;
    }

    fd = -1;

    /* Write CSV header. */
    if (fprintf(output, "id,amount\n") < 0) {
        fprintf(stderr, "Cannot write output header.\n");
        success = 0;
        goto cleanup;
    }

    /* Initialize summary. */
    memset(summary, 0, sizeof(*summary));

    /* Read and process records one by one. */
    while (fgets(line, sizeof(line), input) != NULL) {

        line_number++;

        size_t length = strlen(line);

        /* Check whether the line is too long. */
        if (length == 129 && line[length - 1] != '\n' &&
            !feof(input)) {

            fprintf(stderr, "Invalid input at line %zu: line too long.\n",
                    line_number);

            success = 0;
            break;
        }

        Event event;

        /* Parse current event.
           Adjust this call if your parse_event signature differs. */
        if (!parse_event(line, line_number, &event)) {

            fprintf(stderr, "Invalid input at line %zu.\n",
                    line_number);

            success = 0;
            break;
        }

        /* Update summary. */
        summary->records++;

        if (event.level == INFO) {
    summary->info++;
}
else if (event.level == WARN) {
    summary->warn++;
}
else if (event.level == ERROR) {

            summary->error++;
            summary->alerts++;

            /* Write ERROR event to output. */
            if (fprintf(output, "%d,%d\n",
                        event.id, event.amount) < 0) {

                fprintf(stderr, "Error writing output.\n");
                success = 0;
                break;
            }
        }

        summary->amount_sum += event.amount;
    }

    /* Check input errors. */
    if (ferror(input)) {
        fprintf(stderr, "Error reading input file.\n");
        success = 0;
    }

cleanup:

    if (input != NULL) {
        if (fclose(input) != 0) {
            success = 0;
        }
    }

    if (output != NULL) {

        if (fflush(output) != 0) {
            success = 0;
        }

        if (fclose(output) != 0) {
            success = 0;
        }
    }

    if (fd != -1) {
        close(fd);
    }

    /* Keep temporary output only if successful. */
    if (success) {
        *result_path = temp_path;
        return 1;
    }

    if (temp_path != NULL) {
        unlink(temp_path);
        free(temp_path);
    }

    return 0;
}

// function to compare two Summary structures for equality
int summaries_equal(const Summary *a, const Summary *b) {
    return
        a->records == b->records &&
        a->info == b->info &&
        a->warn == b->warn &&
        a->error == b->error &&
        a->amount_sum == b->amount_sum &&
        a->alerts == b->alerts;
}

// function to compare two files for equality
int files_equal(const char *file1, const char *file2) {

    FILE *a = fopen(file1, "rb");
    FILE *b = fopen(file2, "rb");

    if (a == NULL || b == NULL) {
        if (a != NULL) fclose(a);
        if (b != NULL) fclose(b);
        return 0;
    }

    int equal = 1;

    while (1) {
        int char_a = GETC_FAST(a);
        int char_b = GETC_FAST(b);

        if (char_a != char_b) {
            equal = 0;
            break;
        }

        if (char_a == EOF) {
            break;
        }
    }

    if (ferror(a) || ferror(b)) {
        equal = 0;
    }

    fclose(a);
    fclose(b);

    return equal;
}

// function to verify that the results of parallel and serial processing are equal
int verify_results(const Summary *parallel_summary,const Summary *serial_summary,const char *parallel_file,const char *serial_file) {
    if (!summaries_equal(parallel_summary, serial_summary)) {
        return 0;
    }

    if (!files_equal(parallel_file, serial_file)) {
        return 0;
    }

    return 1;
}

// function to print the summary of processing results
void print_summary(const Summary *summary, const char *verify_status) {

    printf(
        "records=%lld info=%lld warn=%lld error=%lld "
        "amount_sum=%lld alerts=%lld verify=%s\n",

        (long long)summary->records,
        (long long)summary->info,
        (long long)summary->warn,
        (long long)summary->error,
        (long long)summary->amount_sum,
        (long long)summary->alerts,
        verify_status
    );
}

//main function
int main(int argc, char *argv[]) {

    Config config;

    /* Parse command-line arguments. */
    if (!parse_arguments(argc, argv, &config)) {
        fprintf(stderr, "Error: Invalid command-line arguments\n");
        return EXIT_FAILURE;
    }

    Summary summary = {0};

    char *parallel_path = NULL;
    char *serial_path = NULL;

    int success = 0;
    const char *verify_status = "SKIP";

    if (strcmp(config.mode, "serial") == 0) {

        success = run_serial(
            &config,
            &summary,
            &serial_path
        );

    } else {

        success = run_parallel(
            &config,
            &summary,
            &parallel_path
        );

        /* Perform independent serial verification. */
        if (success && config.verify) {

            Summary serial_summary = {0};

            int serial_success = run_serial(
                &config,
                &serial_summary,
                &serial_path
            );

            if (serial_success) {

                int verified = verify_results(
                    &summary,
                    &serial_summary,
                    parallel_path,
                    serial_path
                );

                if (verified) {
                    verify_status = "PASS";
                } else {
                    verify_status = "FAIL";
                    success = 0;
                }

            } else {
                verify_status = "FAIL";
                success = 0;
            }
        }
    }

    /* Publish the correct temporary result. */
    if (success) {

        char *result_path =
            strcmp(config.mode, "serial") == 0
            ? serial_path
            : parallel_path;

        if (rename(result_path, config.output_path) != 0) {
            fprintf(stderr, "Error: Cannot publish output file\n");
            success = 0;
        }
    }

    /* Clean up temporary files. */
    if (parallel_path != NULL) {
        if (!success) {
            unlink(parallel_path);
        }
        free(parallel_path);
    }

    if (serial_path != NULL) {
        if (!success || strcmp(config.mode, "parallel") == 0) {
            unlink(serial_path);
        }
        free(serial_path);
    }

    if (!success) {
        if (strcmp(verify_status, "FAIL") == 0) {
            fprintf(stderr, "Error: verification failed\n");
            print_summary(&summary, verify_status);   /* final line says FAIL */
        }
        return EXIT_FAILURE;
    }

    print_summary(&summary, verify_status);

    return EXIT_SUCCESS;
}
