#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unordered_map>

// TYPES.

struct lock_t;
struct lock_table_entry_t;
struct lock_hash_t;

// GLOBALS.

extern std::unordered_map<lock_hash_t, lock_table_entry_t*> lock_table;
extern pthread_mutex_t lock_table_latch;

// OPERATORS.

bool operator==(const lock_hash_t& l1, const lock_hash_t& l2);

template <>
struct std::hash<lock_hash_t> {
  std::size_t operator()(lock_hash_t const& l) const noexcept;
};

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int64_t table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
