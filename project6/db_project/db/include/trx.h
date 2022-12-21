#ifndef __TRX_H__
#define __TRX_H__

#include <set>
#include <stack>

#include "buffer.h"

// LOCK MODES.

#define SHARED (0)
#define EXCLUSIVE (1)

// TYPES.

struct lock_t;
struct page_hash_t;

struct trx_t {
  int trx_id;
  lock_t* lock;
  int64_t last_lsn;
};

struct lock_table_entry_t {
  int64_t table_id;
  pagenum_t page_id;
  lock_t* tail;
  lock_t* head;
};

struct lock_t {
  lock_t* prev;
  lock_t* next;
  lock_table_entry_t* sentinel;
  pthread_cond_t cond_var;
  int64_t record_id;
  int lock_mode;
  lock_t* trx_next;
  int trx_id;
};

// GLOBALS.

extern int g_trx_id;

extern std::unordered_map<int, trx_t*> trx_table;
extern pthread_mutex_t trx_table_latch;

extern std::unordered_map<page_hash_t, lock_table_entry_t*> lock_table;
extern pthread_mutex_t lock_table_latch;

// APIs.

int trx_begin();
int trx_commit(int trx_id);

int init_lock_table();
lock_t* lock_acquire(int64_t table_id,
                     pagenum_t page_id,
                     int64_t key,
                     int trx_id,
                     int lock_mode);
int lock_release(lock_t* lock_obj);

int trx_shutdown_db();

int trx_abort(int trx_id);

void trx_resurrect(int trx_id, int64_t last_lsn);

// Utilities.

void trx_add_lock(int trx_id, lock_t* lock);
int lock_need_to_wait(lock_t* lock);
void lock_wake_up_all_behind(lock_t* lock);
int trx_detect_deadlock(lock_t* lock);
std::set<int> lock_waiting_list(lock_t* lock);

#endif /* __TRX_H__ */
