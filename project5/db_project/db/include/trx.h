#ifndef __TRX_H__
#define __TRX_H__

#include <set>
#include <stack>

#include "buffer.h"

// LOCK MODES.

#define SHARED (0)
#define EXCLUSIVE (1)

// TYPES.

struct trx_t;
struct trx_undo_log_t;
struct lock_t;
struct lock_table_entry_t;

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

void trx_abort(int trx_id);

// Utilities.

void trx_add_lock(int trx_id, lock_t* lock);
int lock_need_to_wait(lock_t* lock);
void lock_wake_up_all_behind(lock_t* lock);
int trx_detect_deadlock(lock_t* lock);
std::set<int> lock_waiting_list(lock_t* lock);
void trx_undo_update(trx_undo_log_t log);
void trx_log_undo(int trx_id,
                  int64_t table_id,
                  pagenum_t page_id,
                  char* old_val,
                  uint16_t val_size,
                  uint16_t offset);

#endif /* __TRX_H__ */
