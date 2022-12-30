#include "db.h"

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <stdbool.h>


#define DATABASE_BUFFER_SIZE	(200)

/* micro seconds */
uint64_t one_epoch_time;
//#define ONE_EPOCH_TIME			(1 * 1000)
#define ONE_EPOCH_TIME			(one_epoch_time)
#define RANDOM_NUMBER_PER_EPOCH	(300)

/* seconds */
#define PROGRESS_CHECK_INTERVAL	(3)

char				project_name[300];
char				log_path[] = "LOG";
char				logmsg_path[] = "LOGMSG";

typedef int64_t	  TableId;
typedef int64_t		Key;
typedef int			TransactionId;
typedef int64_t		Epoch;
typedef char		Value[120];
#define EpochInvalid	(-1)
#define PARAMETER_SIZE	(10)
typedef int64_t		Parameters[PARAMETER_SIZE];
typedef int64_t		Parameter;
typedef int64_t		ThreadId;
#define ThreadIdInvalid	(-1)
#define ThreadIdExit	(-2)

TableId	table_id_array[20];

typedef enum {
	TA_NONE,
	TA_OPER_START,
	TA_SLEEP,
	TA_OPER_DONE,
} ThreadActivity;

typedef enum {
	OP_NONE,
	OP_TRX_BEGIN,
	OP_TRX_COMMIT,
	OP_TRX_ABORT,
	OP_FIND,
	OP_UPDATE,
} Operation;

typedef struct {
	ThreadActivity	activity;
	Operation		operation;
	Parameters		parameters;
	bool			done;
} ThreadStateData;

typedef ThreadStateData*	ThreadState;
typedef ThreadState*		ThreadStateTable;

void
thread_state_clear(ThreadState thread_state)
{
	thread_state->activity = TA_NONE;
	thread_state->operation = OP_NONE;
}

ThreadState
thread_state_init()
{
	ThreadState thread_state;

	thread_state = (ThreadState) malloc(sizeof(ThreadStateData));
	thread_state_clear(thread_state);
	thread_state->done = false;

	return thread_state;
}

/* pthread variables for thread scheduling */
pthread_mutex_t*	mutexs;
pthread_cond_t*		conds;
pthread_barrier_t*	barrier;

/* one thread who has a authority to execute one operation for this epoch */
ThreadId			selected_thread;
bool				workload_done;

/* all thread's states */
ThreadStateTable	thread_state_table;

char	output_file_name[1000];
char	result_file_name[1000];
char	dbinfo_file_name[1000];
int		output_fd;
int		result_fd;
int		dbinfo_fd;
pthread_mutex_t output_file_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t result_file_mutex = PTHREAD_MUTEX_INITIALIZER;

/* db state (record data) used in comparing crash before & after */
typedef struct {
	TransactionId	trx_id;
	TableId			table_id;
	Key				key;
	int64_t			value;
} RecordState;

typedef struct {
	RecordState*	array;
	int64_t			len;
} RecordStateListManager;

RecordStateListManager* record_state_list;
pthread_mutex_t record_state_list_mutex = PTHREAD_MUTEX_INITIALIZER;

/* execute workload with fixed scheduling if this variable is true */
bool deterministic_mode;
bool do_insertion;
bool population; /* do only population */
bool check_compatibility;

typedef enum {
	CS_BEFORE_CRASH,
	CS_NORMAL_RECOVERY,
	CS_REDO_CRASH,
	CS_UNDO_CRASH,
} CrashState;
CrashState crash_state;

/* global_epoch_number & global_epoch is only used in non deterministic mode. */
Epoch global_epoch_number;
Epoch global_epoch;

/**
 * random numbers have to be generated before executing workloads
 * because generating random number also must be scheduled forcedly
 * and it is possible srand could be called in the student's lock table module.
 */
typedef uint64_t	RandomNumber;
typedef uint64_t*	RandomNumberArray;
typedef struct {
	RandomNumberArray	array;
	uint64_t			len;
	uint64_t			curr_pos;
} RandomNumberManager;

RandomNumberManager* random_number_manager;

void
random_number_init(uint64_t max_count, int thread_number)
{
	if (deterministic_mode == true) {
		srand(123);
	} else {
		srand(time(NULL));
	}

	random_number_manager = (RandomNumberManager*)
		malloc(sizeof(RandomNumberManager) * thread_number);

	for (int i = 0; i < thread_number; i++) {
		random_number_manager[i].len = max_count;
		random_number_manager[i].curr_pos = 0;

		random_number_manager[i].array = (RandomNumber*)
			malloc(sizeof(RandomNumber) * (max_count));
		for (uint64_t j = 0; j < max_count; j++) {
				random_number_manager[i].array[j] = rand();
		}
	}
}

RandomNumber
get_rand(ThreadId thread_id)
{
	RandomNumber	r;
	uint64_t		index;

	index = __sync_fetch_and_add(&random_number_manager[thread_id].curr_pos, 1);

	assert(index < random_number_manager[thread_id].len);

	r = random_number_manager[thread_id].array[index];

	return r;
}


/* micro-seconds */
typedef uint64_t Timestamp;
Timestamp global_begin_time;

/**
 *  GetTimestamp
 *
 *  Return micro-seconds timestamp.
 */
Timestamp
GetTimestamp(void)
{
	struct      timespec tms;
	Timestamp   micros;

	timespec_get(&tms, TIME_UTC);

	micros = tms.tv_sec * 1000000;
	micros += tms.tv_nsec/1000;

	return micros;
}



/* compare and sort pairs of table id and key */
typedef struct {
	TableId	table_id;
	Key		key;
} TableIdKey;

int compare_tik(const void* first, const void* second)
{
	TableIdKey* left = (TableIdKey*) first;
	TableIdKey* right = (TableIdKey*) second;

    if (left->table_id < right->table_id)
		return -1;
    if (left->table_id > right->table_id)
		return 1;
	/* table id is same */
	if (left->key < right->key)
		return -1;
	if (left->key > right->key)
		return 1;
	return 0;
}

void
sorted_tik(TableIdKey tik[], int count)
{
	qsort(tik, count, sizeof(TableIdKey), compare_tik);
}




int
output_file_write(const char str[])
{
	int ret;
	pthread_mutex_lock(&output_file_mutex);
	ret = write(output_fd, str, strlen(str));
	//fsync(output_fd);
	pthread_mutex_unlock(&output_file_mutex);
	return ret;
}

int
result_file_write(const char str[])
{
	int ret;
	pthread_mutex_lock(&result_file_mutex);
	ret = write(result_fd, str, strlen(str));
	//fsync(result_fd);
	pthread_mutex_unlock(&result_file_mutex);
	return ret;
}

int
dbinfo_file_write(const char str[])
{
	int ret;
	ret = write(dbinfo_fd, str, strlen(str));
	return ret;
}

void
print_output(const char str[])
{
	if (false) {
		/* stdout */
		printf("%s", str);
	}
	if (true) {
		/* file write */
		output_file_write(str);
	}
}

void
print_line(Epoch curr_epoch, Epoch max_epoch, int thread_number)
{
	ThreadState state;
	char		str[400];

	for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {

		state = thread_state_table[thread_id];
		if (state->activity != TA_OPER_DONE && thread_id != selected_thread) {
			continue;
		}

		sprintf(str, "Epoch %6ld / %6ld, ", curr_epoch, max_epoch);
		print_output(str);
		sprintf(str, "thread id: %3ld, ", thread_id);
		print_output(str);

		if (state->activity != TA_SLEEP
				&& !(state->activity == TA_NONE && state->done == true)) {
			bool oper_start = state->activity == TA_OPER_START;
			if (state->operation == OP_TRX_BEGIN) {
				if (!oper_start)
					sprintf(str, "%-17s "
							"%s: %3ld",
							"trx_begin,",
							"trx id", state->parameters[0]
							);
				else
					sprintf(str, "%-17s ",
							"trx_begin Zzz,"
							);
				print_output(str);
			}
			if (state->operation == OP_TRX_COMMIT) {
				sprintf(str, "%-17s "
						"%s: %3ld",
						!oper_start ? "trx_commit," : "trx_commit Zzz,",
						"trx id", state->parameters[0]
						);
				print_output(str);
			}
			if (state->operation == OP_TRX_ABORT) {
				sprintf(str, "%-17s "
						"%s: %3ld",
						!oper_start ? "trx_abort," : "trx_abort Zzz,",
						"trx id", state->parameters[0]
						);
				print_output(str);
			}
			if (state->operation == OP_FIND) {
				if (!oper_start)
					sprintf(str, "%-17s "
							"%s: %3ld, %s: %ld, %s: %3ld, %s: %3ld, %s",
							"db_find,",
							"trx id", state->parameters[2],
							"table id", state->parameters[0],
							"key", state->parameters[1],
							"value", state->parameters[3],
							state->parameters[4] == 0 ? "success" : "abort"
							);
				else
					sprintf(str, "%-17s "
							"%s: %3ld, %s: %ld, %s: %3ld",
							"db_find Zzz,",
							"trx id", state->parameters[2],
							"table id", state->parameters[0],
							"key", state->parameters[1]
							);
				print_output(str);
			}
			if (state->operation == OP_UPDATE) {
				if (!oper_start)
					sprintf(str, "%-17s "
							"%s: %3ld, %s: %ld, %s: %3ld, %s: %3ld, %s",
							"db_update,",
							"trx id", state->parameters[2],
							"table id", state->parameters[0],
							"key", state->parameters[1],
							"value", state->parameters[3],
							state->parameters[4] == 0 ? "success" : "abort"
							);
				else
					sprintf(str, "%-17s "
							"%s: %3ld, %s: %ld, %s: %3ld, %s: %3ld",
							"db_update Zzz,",
							"trx id", state->parameters[2],
							"table id", state->parameters[0],
							"key", state->parameters[1],
							"value", state->parameters[3]
							);
				print_output(str);
			}
		} else {
			sprintf(str, "%s", "Zzz..");
			print_output(str);
		}
		sprintf(str, "\n");
		print_output(str);
	}

	print_output("\n");
}


void
scenario_init(Epoch epoch_number, int thread_number)
{
	random_number_init(epoch_number * RANDOM_NUMBER_PER_EPOCH, thread_number + 1);

	selected_thread = ThreadIdInvalid;
	workload_done = false;

	mutexs = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * thread_number);
	conds = (pthread_cond_t*) malloc(sizeof(pthread_cond_t) * thread_number);
	barrier = (pthread_barrier_t*) malloc(sizeof(pthread_barrier_t));
	for (int i = 0; i < thread_number; i++) {
		pthread_mutex_init(&mutexs[i], NULL);
		pthread_cond_init(&conds[i], NULL);
	}
	pthread_barrier_init(barrier, NULL, thread_number + 1);

	thread_state_table = (ThreadStateTable)
		malloc(sizeof(ThreadState) * thread_number);

	for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
		thread_state_table[thread_id] = thread_state_init();
	}
}

void
wait_myturn(ThreadId thread_id)
{
	pthread_mutex_t*	mutex;
	pthread_cond_t*		cond;
	ThreadState			thread_state;

	if (deterministic_mode == false) {
		__sync_fetch_and_add(&global_epoch, 1);
		return;
	}

	mutex = &mutexs[thread_id];
	cond = &conds[thread_id];
	thread_state = thread_state_table[thread_id];

	for (;;) {
		pthread_mutex_lock(mutex);
		pthread_cond_wait(cond, mutex);
		pthread_mutex_unlock(mutex);

		thread_state->activity = TA_NONE;

		if (selected_thread == thread_id)
			break;
		if (selected_thread == ThreadIdExit)
			break;
	}
}

void
wakeup_all(int thread_number)
{
	pthread_mutex_t*	mutex;
	pthread_cond_t*		cond;

	for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
		mutex = &mutexs[thread_id];
		cond = &conds[thread_id];

		pthread_mutex_lock(mutex);
		pthread_cond_signal(cond);
		pthread_mutex_unlock(mutex);
	}
}

TransactionId
OP_trx_begin(ThreadId thread_id)
{
	ThreadState		thread_state;
	TransactionId	transaction_id;

	thread_state = thread_state_table[thread_id];

	wait_myturn(thread_id);

	thread_state->operation = OP_TRX_BEGIN;
	thread_state->activity = TA_OPER_START;

	transaction_id = trx_begin();

	thread_state->activity = TA_OPER_DONE;
	thread_state->parameters[0] = transaction_id;

	return transaction_id;
}

int
OP_trx_commit(ThreadId thread_id, TransactionId transaction_id)
{
	ThreadState		thread_state;
	int				ret;

	thread_state = thread_state_table[thread_id];

	wait_myturn(thread_id);

	thread_state->operation = OP_TRX_COMMIT;
	thread_state->activity = TA_OPER_START;
	thread_state->parameters[0] = transaction_id;

	ret = trx_commit(transaction_id);

	thread_state->activity = TA_OPER_DONE;

	return ret;
}

int
OP_trx_abort(ThreadId thread_id, TransactionId transaction_id)
{
	ThreadState		thread_state;
	int				ret;

	thread_state = thread_state_table[thread_id];

	wait_myturn(thread_id);

	thread_state->operation = OP_TRX_ABORT;
	thread_state->activity = TA_OPER_START;
	thread_state->parameters[0] = transaction_id;

	ret = trx_abort(transaction_id);

	thread_state->activity = TA_OPER_DONE;

	return ret;
}

int
OP_db_update(ThreadId thread_id, TableId table_id, Key key, Value value,
		TransactionId transaction_id)
{
	ThreadState thread_state;
	int			ret;
  uint16_t new_value = 60;
  uint16_t old_value;

	thread_state = thread_state_table[thread_id];

	wait_myturn(thread_id);

	thread_state->operation = OP_UPDATE;
	thread_state->activity = TA_OPER_START;
	thread_state->parameters[0] = table_id;
	thread_state->parameters[1] = key;
	thread_state->parameters[2] = transaction_id;
	thread_state->parameters[3] = atoi(value);

	ret = db_update(table_id_array[table_id], key, value, new_value, &old_value, transaction_id);

	thread_state->activity = TA_OPER_DONE;
	thread_state->parameters[0] = table_id;
	thread_state->parameters[1] = key;
	thread_state->parameters[2] = transaction_id;
	thread_state->parameters[3] = atoi(value);
	thread_state->parameters[4] = ret;

	return ret;
}

int
OP_db_find(ThreadId thread_id, TableId table_id, Key key, Value value,
		TransactionId transaction_id)
{
	ThreadState thread_state;
	int			ret;
  uint16_t val_size;

	thread_state = thread_state_table[thread_id];

	wait_myturn(thread_id);

	thread_state->operation = OP_FIND;
	thread_state->activity = TA_OPER_START;
	thread_state->parameters[0] = table_id;
	thread_state->parameters[1] = key;
	thread_state->parameters[2] = transaction_id;

	ret = db_find(table_id_array[table_id], key, value, &val_size, transaction_id);

	thread_state->activity = TA_OPER_DONE;
	thread_state->parameters[0] = table_id;
	thread_state->parameters[1] = key;
	thread_state->parameters[2] = transaction_id;
	thread_state->parameters[3] = atoi(value);
	thread_state->parameters[4] = ret;

	return ret;
}





void
scenario(int thread_number, void* (*funcs[])(void*), int epoch_number)
{
	pthread_t*	threads;
	Epoch		epoch;

	threads = (pthread_t*) malloc(sizeof(pthread_t) * thread_number);

	scenario_init(epoch_number, thread_number);

	/* thread create */
	for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
		pthread_create(&threads[thread_id], 0, funcs[thread_id],
				(void*) thread_id);
	}

	if (deterministic_mode == true) {
		/* Wait until all thread ready. */
		pthread_barrier_wait(barrier);

		usleep(100 * ONE_EPOCH_TIME);

		global_begin_time = GetTimestamp();

		for (epoch = 0; epoch < epoch_number; epoch++) {
			/* give a chance to one thread to run during each epoch. */
			selected_thread = get_rand(thread_number) % thread_number;

			if (thread_state_table[selected_thread]->activity != TA_SLEEP) {
				wakeup_all(thread_number);
				usleep(ONE_EPOCH_TIME);
			}

			print_line(epoch, epoch_number, thread_number);
			int sleep_count = 0;
			for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
				if (thread_state_table[thread_id]->activity == TA_SLEEP)
					sleep_count++;
			}
			if (sleep_count == thread_number) {
				char str[300];
				sprintf(str, "INCORRECT: all sleep epoch %ld\n", epoch);
				result_file_write(str);
			}
			for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
				if (thread_state_table[thread_id]->activity == TA_OPER_START)
					thread_state_table[thread_id]->activity = TA_SLEEP;
				if (thread_state_table[thread_id]->activity == TA_OPER_DONE)
					thread_state_table[thread_id]->activity = TA_NONE;
			}
		}

		/* terminate workload */

		for (;;) {
			/* give a chance to one thread to run during each epoch. */
			workload_done = true;
			selected_thread = get_rand(thread_number) % thread_number;
			if (thread_state_table[selected_thread]->activity != TA_SLEEP
					&& thread_state_table[selected_thread]->done != true) {
				wakeup_all(thread_number);
				usleep(ONE_EPOCH_TIME);
			}
			
			print_line(epoch, epoch_number, thread_number);
			int sleep_count = 0;
			int done_count = 0;
			for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
				if (thread_state_table[thread_id]->done == true) {
					done_count++;
				} else if (thread_state_table[thread_id]->activity == TA_SLEEP) {
					sleep_count++;
				}
			}
			if (done_count == thread_number) break;
			if (sleep_count + done_count == thread_number) {
				char str[300];
				sprintf(str, "INCORRECT: all sleep in terminate phase\n");
				result_file_write(str);
				break;
			}
			for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
				if (thread_state_table[thread_id]->activity == TA_OPER_START)
					thread_state_table[thread_id]->activity = TA_SLEEP;
				if (thread_state_table[thread_id]->activity == TA_OPER_DONE)
					thread_state_table[thread_id]->activity = TA_NONE;
			}
			epoch++;
		}
	} else {
		/* non deterministic mode */
		global_epoch_number = epoch_number;
		global_epoch = 0;

		/* Wait until all thread ready. */
		pthread_barrier_wait(barrier);

		global_begin_time = GetTimestamp();

		for (;;) {
			Epoch before_epoch;
			Epoch current_epoch;

			before_epoch = __sync_fetch_and_add(&global_epoch, 0);
			sleep(PROGRESS_CHECK_INTERVAL);
			current_epoch = __sync_fetch_and_add(&global_epoch, 0);

			if (current_epoch >= global_epoch_number) break;
			if (before_epoch == current_epoch) {
				char str[300];
				sprintf(str, "INCORRECT: all sleep epoch %ld\n", current_epoch);
				result_file_write(str);
				break;
			}
		}
	}

	/* join thread */
	for (ThreadId thread_id = 0; thread_id < thread_number; thread_id++) {
		pthread_join(threads[thread_id], NULL);
	}

	result_file_write("workload done\n");
}




/******************************************************************************
 * single thread test (STT)
 */

#define STT_CODE				"single_thread_test"
#define STT_FUNC()				single_thread_test()

#define STT_EPOCH_NUMBER		((Epoch) 1000)
#define STT_ND_EPOCH_NUMBER		((Epoch) 1000)
#define STT_TABLE_NUMBER		(1)
#define STT_TABLE_SIZE			(100)
#define STT_THREAD_NUMBER		(1)

void*
STT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key1, key2;
	Value			value;
	Value			ret_val;
	TransactionId	transaction_id;
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		table_id = get_rand(thread_id) % STT_TABLE_NUMBER;
		key1 = get_rand(thread_id) % STT_TABLE_SIZE;
		key2 = get_rand(thread_id) % STT_TABLE_SIZE;
		sprintf(value, "%ld", key2);

		if (key1 == key2) continue;

		transaction_id = OP_trx_begin(thread_id);

		ret = OP_db_find(thread_id, table_id, key1, ret_val, transaction_id);
		if (ret != 0) {
			result_file_write("INCORRECT: fail to db_find()\n");
			return NULL;
		}
		if (atoi(ret_val) != 0 && atoi(ret_val) != key1) {
			result_file_write("INCORRECT: value is wrong\n");
			return NULL;
		}

		ret = OP_db_update(thread_id, table_id, key2, value, transaction_id);
		if (ret != 0) {
			result_file_write("INCORRECT: fail to db_update()\n");
			return NULL;
		}

		OP_trx_commit(thread_id, transaction_id);
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
single_thread_test()
{
	/* Set thread functions. */
	void* (*funcs[STT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < STT_THREAD_NUMBER; i++) {
		funcs[i] = STT_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < STT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < STT_TABLE_SIZE; key++) {
				Value value;
				sprintf(value, "%d", 0);
				db_insert(table_id, key, value, val_size);
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = STT_ND_EPOCH_NUMBER;
	} else {
		epoch_number = STT_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(STT_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /* 
	for (int i = 0; i < STT_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}


/******************************************************************************
 * s-lock test (SLT)
 * s-lock only test
 */

#define SLT_CODE				"s_lock_test"
#define SLT_FUNC()				s_lock_test()

#define SLT_EPOCH_NUMBER		((Epoch) 2000)
#define SLT_ND_EPOCH_NUMBER		((Epoch) 500000)
#define SLT_TABLE_NUMBER		(1)
#define SLT_TABLE_SIZE			(50)
#define SLT_THREAD_NUMBER		(10)

#define SLT_FIND_NUMBER			(10)


void*
SLT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			ret_val;
	TransactionId	transaction_id;
	TableIdKey		tik[SLT_FIND_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		for (int i = 0; i < SLT_FIND_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % SLT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % SLT_TABLE_SIZE;
		}

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < SLT_FIND_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;

			/* avoid accessing same record twice */
			bool twice_access = false;
			for (int j = 0; j < i; j++) {
				if (tik[i].table_id == tik[j].table_id && tik[i].key == tik[j].key)
					twice_access = true;
			}
			if (twice_access == true)
				continue;

			ret = OP_db_find(thread_id, table_id, key, ret_val, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to db_find()\n");
				return NULL;
			}
			if (atoi(ret_val) != 0 && atoi(ret_val) != key) {
				result_file_write("INCORRECT: value is wrong\n");
				return NULL;
			}

		}

		ret = OP_trx_commit(thread_id, transaction_id);
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
s_lock_test()
{
	/* Set thread functions. */
	void* (*funcs[SLT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < SLT_THREAD_NUMBER; i++) {
		funcs[i] = SLT_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < SLT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < SLT_TABLE_SIZE; key++) {
				Value value;
				sprintf(value, "%ld", key);
				db_insert(table_id, key, value, val_size);
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = SLT_ND_EPOCH_NUMBER;
	} else {
		epoch_number = SLT_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(SLT_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < SLT_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}


/******************************************************************************
 * x-lock test (XLT)
 * x-lock only test without deadlock
 */

#define XLT_CODE				"x_lock_test"
#define XLT_FUNC()				x_lock_test()

#define XLT_EPOCH_NUMBER		((Epoch) 3000)
#define XLT_ND_EPOCH_NUMBER		((Epoch) 300000)
#define XLT_TABLE_NUMBER		(1)
#define XLT_TABLE_SIZE			(50)
#define XLT_THREAD_NUMBER		(10)

#define XLT_UPDATE_NUMBER		(10)

void*
XLT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[XLT_UPDATE_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}
		bool	abort = false;

		for (int i = 0; i < XLT_UPDATE_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % XLT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % XLT_TABLE_SIZE;
		}

		sorted_tik(tik, XLT_UPDATE_NUMBER);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < XLT_UPDATE_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%ld", key);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				char str[200];
				sprintf(str, "INCORRECT: fail to db_update()\n"
						"%ld, %ld, %d\n", table_id, key, transaction_id);
				result_file_write(str);
				abort = true;
				break;
			}
		}

		if (abort == false) {
			OP_trx_commit(thread_id, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to trx_commit()\n");
				break;
			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
x_lock_test()
{
	/* Set thread functions. */
	void* (*funcs[XLT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < XLT_THREAD_NUMBER; i++) {
		funcs[i] = XLT_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < XLT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < XLT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
				}
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = XLT_ND_EPOCH_NUMBER;
	} else {
		epoch_number = XLT_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(XLT_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < XLT_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}



/******************************************************************************
 * mix-lock test (MLT)
 * mix-lock test without deadlock
 */

#define MLT_CODE				"m_lock_test"
#define MLT_FUNC()				m_lock_test()

#define MLT_EPOCH_NUMBER		((Epoch) 3000)
#define MLT_ND_EPOCH_NUMBER		((Epoch) 300000)
#define MLT_TABLE_NUMBER		(1)
#define MLT_TABLE_SIZE			(50)
#define MLT_THREAD_NUMBER		(10)

#define MLT_OPERATION_NUMBER	(10)

void*
MLT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	Value			ret_val;
	TransactionId	transaction_id;
	TableIdKey		tik[MLT_OPERATION_NUMBER];
	bool			coin[MLT_OPERATION_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}
		bool	abort = false;

		for (int i = 0; i < MLT_OPERATION_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % MLT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % MLT_TABLE_SIZE;
			coin[i] = (get_rand(thread_id) % 2 == 0);
		}

		sorted_tik(tik, MLT_OPERATION_NUMBER);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < MLT_OPERATION_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%ld", key);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			if (coin[i] == true) {
				/* db_find */
				ret = OP_db_find(thread_id, table_id, key, ret_val, transaction_id);
				if (ret != 0) {
					char str[200];
					sprintf(str, "INCORRECT: fail to db_find()\n"
							"%ld, %ld, %d\n", table_id, key, transaction_id);
					result_file_write(str);
					abort = true;
					break;
				}
			} else {
				/* db_update */
				ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
				if (ret != 0) {
					char str[200];
					sprintf(str, "INCORRECT: fail to db_update()\n"
							"%ld, %ld, %d\n", table_id, key, transaction_id);
					result_file_write(str);
					abort = true;
					break;
				}
			}
		}

		if (abort == false) {
			OP_trx_commit(thread_id, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to trx_commit()\n");
				break;
			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
m_lock_test()
{
	/* Set thread functions. */
	void* (*funcs[MLT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < MLT_THREAD_NUMBER; i++) {
		funcs[i] = MLT_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < MLT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < MLT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
				}
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = MLT_ND_EPOCH_NUMBER;
	} else {
		epoch_number = MLT_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(MLT_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < MLT_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}


/******************************************************************************
 * deadlock test (DLT)
 */

#define DLT_CODE				"deadlock_test"
#define DLT_FUNC()				deadlock_test()

#define DLT_EPOCH_NUMBER		((Epoch) 3000)
#define DLT_ND_EPOCH_NUMBER		((Epoch) 300000)
#define DLT_TABLE_NUMBER		(1)
#define DLT_TABLE_SIZE			(50)
#define DLT_THREAD_NUMBER		(5)

#define DLT_OPERATION_NUMBER	(10)

void*
DLT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	Value			ret_val;
	TransactionId	transaction_id;
	TableIdKey		tik[DLT_OPERATION_NUMBER];
	bool			coin[DLT_OPERATION_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}
		bool	abort = false;

		for (int i = 0; i < DLT_OPERATION_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % DLT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % DLT_TABLE_SIZE;
			coin[i] = (get_rand(thread_id) % 2 == 0);
		}

		/* avoid accessing same record twice */
		bool flag = false;
		for (int i = 0; i < DLT_OPERATION_NUMBER; i++) {
			for (int j = 0; j < i; j++) {
				if (tik[i].table_id == tik[j].table_id && tik[i].key == tik[j].key)
					flag = true;
			}
		}
		if (flag == true)
			continue;

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < DLT_OPERATION_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%ld", key);

			if (coin[i] == true) {
				/* db_find */
				ret = OP_db_find(thread_id, table_id, key, ret_val, transaction_id);
				if (ret != 0) {
					/* aborted */
					abort = true;
					break;
				}
			} else {
				/* db_update */
				ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
				if (ret != 0) {
					/* aborted */
					abort = true;
					break;
				}
			}
		}

		if (abort == false) {
			OP_trx_commit(thread_id, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to trx_commit()\n");
				break;
			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
deadlock_test()
{
	/* Set thread functions. */
	void* (*funcs[DLT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < DLT_THREAD_NUMBER; i++) {
		funcs[i] = DLT_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < DLT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < DLT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
				}
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = DLT_ND_EPOCH_NUMBER;
	} else {
		epoch_number = DLT_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(DLT_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < DLT_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}


/******************************************************************************
 * stress test 1 (ST1)
 * many thread without deadlock
 */

#define ST1_CODE				"stress_test_1"
#define ST1_FUNC()				stress_test_1()

#define ST1_EPOCH_NUMBER		((Epoch) 5000)
#define ST1_ND_EPOCH_NUMBER		((Epoch) 300000)
#define ST1_TABLE_NUMBER		(1)
#define ST1_TABLE_SIZE			(40)
#define ST1_THREAD_NUMBER		(30)

#define ST1_OPERATION_NUMBER	(10)

void*
ST1_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	Value			ret_val;
	TransactionId	transaction_id;
	TableIdKey		tik[ST1_OPERATION_NUMBER];
	bool			coin[ST1_OPERATION_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}
		bool	abort = false;

		for (int i = 0; i < ST1_OPERATION_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % ST1_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % ST1_TABLE_SIZE;
			coin[i] = (get_rand(thread_id) % 2 == 0);
		}

		sorted_tik(tik, ST1_OPERATION_NUMBER);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < ST1_OPERATION_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%ld", key);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			if (coin[i] == true) {
				/* db_find */
				ret = OP_db_find(thread_id, table_id, key, ret_val, transaction_id);
				if (ret != 0) {
					char str[200];
					sprintf(str, "INCORRECT: fail to db_find()\n"
							"%ld, %ld, %d\n", table_id, key, transaction_id);
					result_file_write(str);
					abort = true;
					break;
				}
			} else {
				/* db_update */
				ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
				if (ret != 0) {
					char str[200];
					sprintf(str, "INCORRECT: fail to db_update()\n"
							"%ld, %ld, %d\n", table_id, key, transaction_id);
					result_file_write(str);
					abort = true;
					break;
				}
			}
		}

		if (abort == false) {
			OP_trx_commit(thread_id, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to trx_commit()\n");
				break;
			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
stress_test_1()
{
	/* Set thread functions. */
	void* (*funcs[ST1_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < ST1_THREAD_NUMBER; i++) {
		funcs[i] = ST1_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < ST1_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < ST1_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
				}
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = ST1_ND_EPOCH_NUMBER;
	} else {
		epoch_number = ST1_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(ST1_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < ST1_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}



/******************************************************************************
 * stress test 2 (ST2)
 * enourmous deadlock
 */

#define ST2_CODE				"stress_test_2"
#define ST2_FUNC()				stress_test_2()

#define ST2_EPOCH_NUMBER		((Epoch) 5000)
#define ST2_ND_EPOCH_NUMBER		((Epoch) 100000)
#define ST2_TABLE_NUMBER		(1)
#define ST2_TABLE_SIZE			(40)
#define ST2_THREAD_NUMBER		(20)

#define ST2_OPERATION_NUMBER	(20)

void*
ST2_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	Value			ret_val;
	TransactionId	transaction_id;
	TableIdKey		tik[ST2_OPERATION_NUMBER];
	bool			coin[ST2_OPERATION_NUMBER];
	int				ret;

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}
		bool	abort = false;

		for (int i = 0; i < ST2_OPERATION_NUMBER; i++) {
			tik[i].table_id = get_rand(thread_id) % ST2_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % ST2_TABLE_SIZE;
			coin[i] = (get_rand(thread_id) % 2 == 0);
		}

		/* avoid accessing same record twice */
		bool flag = false;
		for (int i = 0; i < DLT_OPERATION_NUMBER; i++) {
			for (int j = 0; j < i; j++) {
				if (tik[i].table_id == tik[j].table_id && tik[i].key == tik[j].key)
					flag = true;
			}
		}
		if (flag == true)
			continue;

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < ST2_OPERATION_NUMBER; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%ld", key);

			if (coin[i] == true) {
				/* db_find */
				ret = OP_db_find(thread_id, table_id, key, ret_val, transaction_id);
				if (ret != 0) {
					/* aborted */
					abort = true;
					break;
				}
			} else {
				/* db_update */
				ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
				if (ret != 0) {
					/* aborted */
					abort = true;
					break;
				}
			}
		}

		if (abort == false) {
			OP_trx_commit(thread_id, transaction_id);
			if (ret != 0) {
				result_file_write("INCORRECT: fail to trx_commit()\n");
				break;
			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
stress_test_2()
{
	/* Set thread functions. */
	void* (*funcs[ST2_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;

	for (int i = 0; i < ST2_THREAD_NUMBER; i++) {
		funcs[i] = ST2_func;
	}

	/* Initiate database. */
	init_db(DATABASE_BUFFER_SIZE, 0, 0, log_path, logmsg_path);

	/* open table */
	for (int i = 0; i < ST2_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < ST2_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
				}
			}
		}
	}

	result_file_write("database init\n");

	if (deterministic_mode == false) {
		epoch_number = ST2_ND_EPOCH_NUMBER;
	} else {
		epoch_number = ST2_EPOCH_NUMBER;
	}

	/* run scenario */
	scenario(ST2_THREAD_NUMBER, funcs, epoch_number);

	/* close table */
  /*
	for (int i = 0; i < ST2_TABLE_NUMBER; i++) {
		TableId table_id;
		table_id = table_id_array[i];
		close_table(table_id);
	}
  */
	/* shutdown db */
	shutdown_db();
}











/******************************************************************************
 * LOGGING PROJECT
 * logging_simple_test (LST)
 *
 */

#define LST_CODE				"logging_simple_test"
#define LST_FUNC()				logging_simple_test()

#define LST_EPOCH_NUMBER		((Epoch) 5000)
#define LST_ND_EPOCH_NUMBER		((Epoch) 100)
#define LST_BUFFER_SIZE			(50)
#define LST_TABLE_NUMBER		(1)
#define LST_TABLE_SIZE			(1000)
#define LST_THREAD_NUMBER		(1)

#define LST_UPDATE_NUMBER		(1)

void*
LST_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LST_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = LST_UPDATE_NUMBER;
		commit = (get_rand(thread_id) % 5 != 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LST_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % LST_TABLE_SIZE;
		}

		/* sorting for avoiding deadlock */
		sorted_tik(tik, update_number);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				sprintf(str, "INCORRECT: fail to db_update(%d)\n", transaction_id);
				result_file_write(str);
				commit = false;
				break;
			}
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				if (i != 0 && tik[i].table_id == tik[i-1].table_id
						&& tik[i].key == tik[i-1].key) continue;
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_simple_test()
{
	/* Set thread functions. */
	void* (*funcs[LST_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LST_THREAD_NUMBER; i++) {
		funcs[i] = LST_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LST_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LST_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LST_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LST_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LST_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LST_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LST_TABLE_NUMBER, LST_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LST_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LST_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LST_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LST_EPOCH_NUMBER;
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LST_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LST_TABLE_NUMBER, LST_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LST_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}


/******************************************************************************
 * LOGGING PROJECT
 * logging_short_transaction_nodeadlock_test (LSTNT)
 *
 * commit or abort with a few update
 */

#define LSTNT_CODE				"logging_short_transaction_nodeadlock_test"
#define LSTNT_FUNC()			logging_short_transaction_nodeadlock_test()

#define LSTNT_EPOCH_NUMBER		((Epoch) 5000)
#define LSTNT_ND_EPOCH_NUMBER	((Epoch) 1000)
#define LSTNT_BUFFER_SIZE		(50)
#define LSTNT_TABLE_NUMBER		(1)
#define LSTNT_TABLE_SIZE		(1000)
#define LSTNT_THREAD_NUMBER		(10)

#define LSTNT_UPDATE_NUMBER		(4)

void*
LSTNT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LSTNT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LSTNT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LSTNT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % LSTNT_TABLE_SIZE;
		}

		/* sorting for avoiding deadlock */
		sorted_tik(tik, update_number);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				sprintf(str, "INCORRECT: fail to db_update(%d)\n", transaction_id);
				result_file_write(str);
				commit = false;
				break;
			}
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				if (i != 0 && tik[i].table_id == tik[i-1].table_id
						&& tik[i].key == tik[i-1].key) continue;
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_short_transaction_nodeadlock_test()
{
	/* Set thread functions. */
	void* (*funcs[LSTNT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LSTNT_THREAD_NUMBER; i++) {
		funcs[i] = LSTNT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LSTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LSTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LSTNT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LSTNT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LSTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LSTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LSTNT_TABLE_NUMBER, LSTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LSTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LSTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LSTNT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LSTNT_EPOCH_NUMBER;
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LSTNT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LSTNT_TABLE_NUMBER, LSTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LSTNT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}


/******************************************************************************
 * LOGGING PROJECT
 * logging_short_transaction_test (LSTT)
 *
 * commit or abort with a few update
 */

#define LSTT_CODE				"logging_short_transaction_test"
#define LSTT_FUNC()				logging_short_transaction_test()

#define LSTT_EPOCH_NUMBER		((Epoch) 5000)
#define LSTT_ND_EPOCH_NUMBER	((Epoch) 1000)
#define LSTT_BUFFER_SIZE		(50)
#define LSTT_TABLE_NUMBER		(1)
#define LSTT_TABLE_SIZE			(1000)
#define LSTT_THREAD_NUMBER		(10)

#define LSTT_UPDATE_NUMBER		(3)

void*
LSTT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LSTT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LSTT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LSTT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % LSTT_TABLE_SIZE;
		}

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				commit = false;
				break;
			}
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_short_transaction_test()
{
	/* Set thread functions. */
	void* (*funcs[LSTT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LSTT_THREAD_NUMBER; i++) {
		funcs[i] = LSTT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LSTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LSTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LSTT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LSTT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LSTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LSTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LSTT_TABLE_NUMBER, LSTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LSTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LSTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LSTT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LSTT_EPOCH_NUMBER;
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LSTT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LSTT_TABLE_NUMBER, LSTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LSTT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}


/******************************************************************************
 * LOGGING PROJECT
 * logging_long_transaction_nodeadlock_test (LLTNT)
 *
 */

#define LLTNT_CODE				"logging_long_transaction_nodeadlock_test"
#define LLTNT_FUNC()			logging_long_transaction_nodeadlock_test()

#define LLTNT_EPOCH_NUMBER		((Epoch) 5000)
#define LLTNT_ND_EPOCH_NUMBER	((Epoch) 1000)
#define LLTNT_BUFFER_SIZE		(50)
#define LLTNT_TABLE_NUMBER		(1)
#define LLTNT_TABLE_SIZE		(1000)
#define LLTNT_THREAD_NUMBER		(10)

#define LLTNT_UPDATE_NUMBER		(50)

void*
LLTNT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LLTNT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LLTNT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LLTNT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % LLTNT_TABLE_SIZE;
		}

		/* sorting for avoiding deadlock */
		sorted_tik(tik, update_number);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				commit = false;
				break;
			}
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				if (i != 0 && tik[i].table_id == tik[i-1].table_id
						&& tik[i].key == tik[i-1].key) continue;
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_long_transaction_nodeadlock_test()
{
	/* Set thread functions. */
	void* (*funcs[LLTNT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LLTNT_THREAD_NUMBER; i++) {
		funcs[i] = LLTNT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LLTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LLTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LLTNT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LLTNT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LLTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLTNT_TABLE_NUMBER, LLTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LLTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LLTNT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LLTNT_EPOCH_NUMBER;
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LLTNT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLTNT_TABLE_NUMBER, LLTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LLTNT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}


/******************************************************************************
 * LOGGING PROJECT
 * logging_long_transaction_test (LLTT)
 *
 * commit or abort with many update
 */

#define LLTT_CODE				"logging_long_transaction_test"
#define LLTT_FUNC()				logging_long_transaction_test()

#define LLTT_EPOCH_NUMBER		((Epoch) 5000)
#define LLTT_ND_EPOCH_NUMBER	((Epoch) 1000)
#define LLTT_BUFFER_SIZE		(50)
#define LLTT_TABLE_NUMBER		(1)
#define LLTT_TABLE_SIZE			(1000)
#define LLTT_THREAD_NUMBER		(10)

#define LLTT_UPDATE_NUMBER		(50)

void*
LLTT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LLTT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LLTT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LLTT_TABLE_NUMBER;
			tik[i].key = get_rand(thread_id) % LLTT_TABLE_SIZE;
		}

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				commit = false;
				break;
			}
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_long_transaction_test()
{
	/* Set thread functions. */
	void* (*funcs[LLTT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LLTT_THREAD_NUMBER; i++) {
		funcs[i] = LLTT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LLTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LLTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LLTT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LLTT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LLTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLTT_TABLE_NUMBER, LLTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LLTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LLTT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LLTT_EPOCH_NUMBER;
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LLTT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLTT_TABLE_NUMBER, LLTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LLTT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}


/******************************************************************************
 * LOGGING PROJECT
 * logging_live_transaction_nodeadlock_test (LLiveTNT)
 *
 */

#define LLiveTNT_CODE				"logging_live_transaction_nodeadlock_test"
#define LLiveTNT_FUNC()				logging_live_transaction_nodeadlock_test()

#define LLiveTNT_EPOCH_NUMBER		((Epoch) 5000)
#define LLiveTNT_ND_EPOCH_NUMBER		((Epoch) 1500)
#define LLiveTNT_BUFFER_SIZE			(50)
#define LLiveTNT_TABLE_NUMBER		(1)
#define LLiveTNT_TABLE_SIZE			(4000)
#define LLiveTNT_THREAD_NUMBER		(10)

#define LLiveTNT_UPDATE_NUMBER		(20)

Key*		LLiveTNT_acquired_keys;
int64_t		LLiveTNT_len;
int64_t		LLiveTNT_curr;

void*
LLiveTNT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LLiveTNT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	bool			live;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LLiveTNT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		live = (get_rand(thread_id) % 3 == 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LLiveTNT_TABLE_NUMBER;
			if (live == true) {
retry:
				int64_t curr;
				curr = __sync_fetch_and_add(&LLiveTNT_curr, 1);
				if (curr >= LLiveTNT_len) {
					if (i == 0) {
						live = false;
						tik[i].key = (get_rand(thread_id) % (LLiveTNT_TABLE_SIZE / 2)) * 2 + 1;
						continue;
					} else {
						update_number = i;
						break;
					}
				}
				for (int j = 0; j < curr; j++) {
					if (LLiveTNT_acquired_keys[curr] == LLiveTNT_acquired_keys[j])
						goto retry;
				}
				tik[i].key = LLiveTNT_acquired_keys[curr] * 2;
			} else {
				tik[i].key = (get_rand(thread_id) % (LLiveTNT_TABLE_SIZE / 2)) * 2 + 1;
			}
		}

		/* sorting for avoiding deadlock */
		sorted_tik(tik, update_number);

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			if (i != 0 && table_id == tik[i-1].table_id && key == tik[i-1].key)
				/* Avoid accessing same record twice. */
				continue;

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				commit = false;
				break;
			}
		}

		if (live == true) {
			continue;
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				if (i != 0 && tik[i].table_id == tik[i-1].table_id
						&& tik[i].key == tik[i-1].key) continue;
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_live_transaction_nodeadlock_test()
{
	/* Set thread functions. */
	void* (*funcs[LLiveTNT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LLiveTNT_THREAD_NUMBER; i++) {
		funcs[i] = LLiveTNT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LLiveTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LLiveTNT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LLiveTNT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LLiveTNT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LLiveTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLiveTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLiveTNT_TABLE_NUMBER, LLiveTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LLiveTNT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLiveTNT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LLiveTNT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LLiveTNT_EPOCH_NUMBER;
		}

		LLiveTNT_len = epoch_number;
		LLiveTNT_acquired_keys = (Key*) malloc(sizeof(Key) * LLiveTNT_len);
		LLiveTNT_curr = 0;
		for (int i = 0; i < LLiveTNT_len; i++) {
			LLiveTNT_acquired_keys[i] = rand() % (LLiveTNT_TABLE_SIZE / 2);
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LLiveTNT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLiveTNT_TABLE_NUMBER, LLiveTNT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LLiveTNT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}



/******************************************************************************
 * LOGGING PROJECT
 * logging_live_transaction_test (LLiveTT)
 *
 * commit or abort with many update
 */

#define LLiveTT_CODE				"logging_live_transaction_test"
#define LLiveTT_FUNC()				logging_live_transaction_test()

#define LLiveTT_EPOCH_NUMBER		((Epoch) 5000)
#define LLiveTT_ND_EPOCH_NUMBER		((Epoch) 1500)
#define LLiveTT_BUFFER_SIZE			(50)
#define LLiveTT_TABLE_NUMBER		(1)
#define LLiveTT_TABLE_SIZE			(4000)
#define LLiveTT_THREAD_NUMBER		(10)

#define LLiveTT_UPDATE_NUMBER		(20)

Key*		LLiveTT_acquired_keys;
int64_t		LLiveTT_len;
int64_t		LLiveTT_curr;

void*
LLiveTT_func(void* args)
{
	ThreadId		thread_id;
	TableId			table_id;
	Key				key;
	Value			value;
	TransactionId	transaction_id;
	TableIdKey		tik[LLiveTT_UPDATE_NUMBER];
	int				ret;
	int				update_number;
	bool			commit;
	bool			live;
	char			str[500];

	thread_id = (ThreadId) args;

	/* Wait until all thread ready. */
	pthread_barrier_wait(barrier);

	for (;;) {
		if (selected_thread == ThreadIdExit) break;
		if (workload_done == true) break;
		if (deterministic_mode == false &&
				__sync_fetch_and_add(&global_epoch, 0) >= global_epoch_number) {
			break;
		}

		update_number = get_rand(thread_id) % LLiveTT_UPDATE_NUMBER + 1;
		commit = (get_rand(thread_id) % 5 != 0);
		live = (get_rand(thread_id) % 3 == 0);
		for (int i = 0; i < update_number; i++) {
			tik[i].table_id = get_rand(thread_id) % LLiveTT_TABLE_NUMBER;
			if (live == true) {
retry:
				int64_t curr;
				curr = __sync_fetch_and_add(&LLiveTT_curr, 1);
				if (curr >= LLiveTNT_len) {
					if (i == 0) {
						live = false;
						tik[i].key = (get_rand(thread_id) % (LLiveTNT_TABLE_SIZE / 2)) * 2 + 1;
						continue;
					} else {
						update_number = i;
						break;
					}
				}
				for (int j = 0; j < curr; j++) {
					if (LLiveTT_acquired_keys[curr] == LLiveTT_acquired_keys[j])
						goto retry;
				}
				tik[i].key = LLiveTT_acquired_keys[curr] * 2;
			} else {
				tik[i].key = (get_rand(thread_id) % (LLiveTT_TABLE_SIZE / 2)) * 2 + 1;
			}
		}

		transaction_id = OP_trx_begin(thread_id);

		for (int i = 0; i < update_number; i++) {
			table_id = tik[i].table_id;
			key = tik[i].key;
			sprintf(value, "%d", transaction_id);

			/* db_update */
			ret = OP_db_update(thread_id, table_id, key, value, transaction_id);
			if (ret != 0) {
				/* aborted */
				commit = false;
				break;
			}
		}

		if (live == true) {
			continue;
		}

		if (commit == true) {
			if (deterministic_mode == false)
				pthread_mutex_lock(&record_state_list_mutex);
			ret = OP_trx_commit(thread_id, transaction_id);
			if (ret != transaction_id) {
				sprintf(str, "INCORRECT: fail to trx_commit(%d)\n", transaction_id);
				result_file_write(str);
				if (deterministic_mode == false)
					pthread_mutex_unlock(&record_state_list_mutex);
				break;
			}
			for (int i = 0; i < update_number; i++) {
				int64_t index = record_state_list->len;
				record_state_list->array[index].trx_id = transaction_id;
				record_state_list->array[index].table_id = tik[i].table_id;
				record_state_list->array[index].key = tik[i].key;
				record_state_list->array[index].value = transaction_id;
				record_state_list->len++;
			}
			if (deterministic_mode == false)
				pthread_mutex_unlock(&record_state_list_mutex);
		} else {
			ret = OP_trx_abort(thread_id, transaction_id);
//			if (ret != transaction_id) {
//				sprintf(str, "INCORRECT: fail to trx_abort(%d)\n", transaction_id);
//				result_file_write(str);
//				break;
//			}
		}
	}

	thread_state_table[thread_id]->done = true;

	return NULL;
}

void
logging_live_transaction_test()
{
	/* Set thread functions. */
	void* (*funcs[LLiveTT_THREAD_NUMBER])(void*);
	Epoch epoch_number;
  uint16_t val_size = 60;
  uint16_t ret_val_size;

	for (int i = 0; i < LLiveTT_THREAD_NUMBER; i++) {
		funcs[i] = LLiveTT_func;
	}

	/* Initiate database. */
	if (crash_state == CS_BEFORE_CRASH) {
		init_db(LLiveTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}
	if (crash_state == CS_NORMAL_RECOVERY) {
		init_db(LLiveTT_BUFFER_SIZE, 0, 0, log_path, logmsg_path);
	}

	/* open table */
	for (int i = 0; i < LLiveTT_TABLE_NUMBER; i++) {
		char* str = (char*) malloc(sizeof(char) * 100);
		TableId table_id;
		sprintf(str, "DATA%d", i + 1);
		table_id = open_table(str);
		table_id_array[i] = table_id;

		if (do_insertion == true) {
			/* insertion */
			for (Key key = 0; key < LLiveTT_TABLE_SIZE; key++) {
				Value value;
				int ret;
				sprintf(value, "%ld", key);
				ret = db_insert(table_id, key, value, val_size);
				if (ret != 0) {
					printf("insert error: %ld, %ld\n", table_id, key);
					result_file_write("INCORRECT: fail to db_insert()\n");
				}
			}
		}
	}

	if (check_compatibility == true) {
		TransactionId trx_id = trx_begin();
		for (int i = 0; i < LLiveTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLiveTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret != 0) {
					result_file_write("INCORRECT: compatibility failed\n");
				}
				if (atoi(ret_val) != key) {
					result_file_write("INCORRECT: compatibility failed2\n");
				}
			}
		}
		trx_commit(trx_id);
	}

	if (crash_state == CS_NORMAL_RECOVERY) {
		TransactionId trx_id = trx_begin();
		char str[300];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLiveTT_TABLE_NUMBER, LLiveTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < LLiveTT_TABLE_NUMBER; i++) {
			TableId table_id = table_id_array[i];

			for (Key key = 0; key < LLiveTT_TABLE_SIZE; key++) {
				Value ret_val;
				int ret;
				ret = db_find(table_id, key, ret_val, &ret_val_size, trx_id);
				if (ret == 0) {
					sprintf(str, "trxid: %d, tableid: %d, key: %ld, value: %d\n",
							0, i, key, atoi(ret_val));
					dbinfo_file_write(str);
				}
			}
		}
		trx_commit(trx_id);
	}

	if (check_compatibility == false
			&& population == false
			&& crash_state == CS_BEFORE_CRASH) {

		result_file_write("database init\n");

		if (deterministic_mode == false) {
			epoch_number = LLiveTT_ND_EPOCH_NUMBER;
		} else {
			epoch_number = LLiveTT_EPOCH_NUMBER;
		}

		LLiveTT_len = epoch_number;
		LLiveTT_acquired_keys = (Key*) malloc(sizeof(Key) * LLiveTT_len);
		LLiveTT_curr = 0;
		for (int i = 0; i < LLiveTT_len; i++) {
			LLiveTT_acquired_keys[i] = rand() % (LLiveTT_TABLE_SIZE / 2);
		}

		/* db state manager init */
		record_state_list = (RecordStateListManager*)
			malloc(sizeof(RecordStateListManager));
		record_state_list->array = (RecordState*)
			malloc(sizeof(RecordState) * (epoch_number * 3 + 1000));
		record_state_list->len = 0;

		/* run scenario */
		scenario(LLiveTT_THREAD_NUMBER, funcs, epoch_number);

		/* store db state */
		char str[400];
		sprintf(str, "table_num: %d, table_size: %d\n",
				LLiveTT_TABLE_NUMBER, LLiveTT_TABLE_SIZE);
		dbinfo_file_write(str);
		for (int i = 0; i < record_state_list->len; i++) {
			TransactionId	trx_id		= record_state_list->array[i].trx_id;
			TableId			table_id	= record_state_list->array[i].table_id;
			Key				key			= record_state_list->array[i].key;
			int64_t			value		= record_state_list->array[i].value;
			sprintf(str, "trxid: %d, tableid: %ld, key: %ld, value: %ld\n",
					trx_id, table_id, key, value);
			dbinfo_file_write(str);
		}
	}

	if (crash_state == CS_NORMAL_RECOVERY
			|| population == true
			|| check_compatibility == true) {
		/* close table */
    /*
		for (int i = 0; i < LLiveTT_TABLE_NUMBER; i++) {
			TableId table_id;
			table_id = table_id_array[i];
			close_table(table_id);
		}
    */
		/* shutdown db */
		shutdown_db();
	}
}

#define PRINT_TIME

int
main(int argc, char* argv[])
{
#ifdef PRINT_TIME
	Timestamp start;
	Timestamp end;
	Timestamp diff;
#endif

#ifdef PRINT_TIME
		start = GetTimestamp();
#endif

	if (argc < 2) {
		return 0;
	}

	if (strcmp("logging", argv[1]) == 0) {

		if (!(argc == 10)) {
			return 0;
		}

		sprintf(project_name, "logging");

		sprintf(output_file_name, "%s", argv[3]);
		sprintf(result_file_name, "%s", argv[4]);
		sprintf(dbinfo_file_name, "%s", argv[5]);

		output_fd = open(output_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
		result_fd = open(result_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
		dbinfo_fd = open(dbinfo_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);


		if (strcmp("non_deterministic_mode", argv[6]) == 0) {
			deterministic_mode = false;
		} else {
			return 0;
		}

		if (strcmp("do_insertion", argv[7]) == 0) {
			do_insertion = true;
			population = false;
			check_compatibility = false;
		} else if (strcmp("no_insertion", argv[7]) == 0) {
			do_insertion = false;
			population = false;
			check_compatibility = false;
		} else if (strcmp("population", argv[7]) == 0) {
			do_insertion = true;
			population = true;
			check_compatibility = false;
		} else if (strcmp("compatibility", argv[7]) == 0) {
			do_insertion = false;
			population = false;
			check_compatibility = true;
		} else {
			return 0;
		}

		if (strcmp("before_crash", argv[8]) == 0) {
			crash_state = CS_BEFORE_CRASH;
		} else if (strcmp("normal_recovery", argv[8]) == 0) {
			crash_state = CS_NORMAL_RECOVERY;
		} else if (strcmp("redo_crash", argv[8]) == 0) {
			crash_state = CS_REDO_CRASH;
		} else if (strcmp("undo_crash", argv[8]) == 0) {
			crash_state = CS_UNDO_CRASH;
		} else {
			return 0;
		}

		one_epoch_time = atoi(argv[9]);

		if (strcmp(LST_CODE, argv[2]) == 0) {
			LST_FUNC();
		}
		if (strcmp(LSTNT_CODE, argv[2]) == 0) {
			LSTNT_FUNC();
		}
		if (strcmp(LSTT_CODE, argv[2]) == 0) {
			LSTT_FUNC();
		}
		if (strcmp(LLTNT_CODE, argv[2]) == 0) {
			LLTNT_FUNC();
		}
		if (strcmp(LLTT_CODE, argv[2]) == 0) {
			LLTT_FUNC();
		}
		if (strcmp(LLiveTNT_CODE, argv[2]) == 0) {
			LLiveTNT_FUNC();
		}
		if (strcmp(LLiveTT_CODE, argv[2]) == 0) {
			LLiveTT_FUNC();
		}
		close(output_fd);
		close(result_fd);
		close(dbinfo_fd);
	}

#ifdef PRINT_TIME
	end = GetTimestamp();
	diff = end - start;

	printf("running time: %3lu.%03lu s\n", diff / 1000000, diff % 1000000 / 1000);
#endif

	return 0;
}

