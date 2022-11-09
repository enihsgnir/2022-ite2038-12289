#include "lock_table.h"

// TYPES.

struct lock_t {
  /* GOOD LOCK :) */
  lock_t* prev;
  lock_t* next;
  lock_table_entry_t* sentinel;
  pthread_cond_t cond_var;
};

struct lock_table_entry_t {
  int64_t table_id;
  int64_t key;
  lock_t* tail;
  lock_t* head;
};

struct lock_hash_t {
  int64_t table_id;
  int64_t key;
};

// GLOBALS.

std::unordered_map<lock_hash_t, lock_table_entry_t*> lock_table;
pthread_mutex_t lock_table_latch;

// OPERATORS.

bool operator==(const lock_hash_t& l1, const lock_hash_t& l2) {
  return l1.table_id == l2.table_id && l1.key == l2.key;
}

std::size_t std::hash<lock_hash_t>::operator()(
    lock_hash_t const& l) const noexcept {
  std::size_t h1 = std::hash<int64_t>{}(l.table_id);
  std::size_t h2 = std::hash<int64_t>{}(l.key);
  return h1 ^ (h2 << 1);
}

/* APIs for lock table */

int init_lock_table() {
  lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
  return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key) {
  pthread_mutex_lock(&lock_table_latch);

  lock_table_entry_t* entry = lock_table[{table_id, key}];
  if (entry == NULL) {
    entry = (lock_table_entry_t*)malloc(sizeof(lock_table_entry_t));
    if (entry == NULL) {
      return NULL;
    }
    entry->table_id = table_id;
    entry->key = key;
    entry->tail = NULL;
    entry->head = NULL;

    lock_table[{table_id, key}] = entry;
  }

  lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
  if (lock == NULL) {
    return NULL;
  }
  lock->prev = entry->tail;
  lock->next = NULL;
  lock->sentinel = entry;
  lock->cond_var = PTHREAD_COND_INITIALIZER;

  if (entry->tail == NULL) {
    entry->head = lock;
  } else {
    entry->tail->next = lock;
  }
  entry->tail = lock;

  while (entry->head != lock) {
    pthread_cond_wait(&lock->cond_var, &lock_table_latch);
  }
  pthread_mutex_unlock(&lock_table_latch);
  return lock;
};

int lock_release(lock_t* lock_obj) {
  pthread_mutex_lock(&lock_table_latch);

  lock_table_entry_t* entry = lock_obj->sentinel;
  if (lock_obj != entry->head) {
    return -1;
  }

  if (lock_obj->next == NULL) {
    lock_table.erase({entry->table_id, entry->key});
    free(entry);
  } else {
    entry->head = lock_obj->next;
    entry->head->prev = NULL;
    pthread_cond_signal(&lock_obj->next->cond_var);
  }

  free(lock_obj);

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
