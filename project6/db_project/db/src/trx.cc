#include "trx.h"

// GLOBALS.

int g_trx_id = 1;

std::unordered_map<int, trx_t*> trx_table;
pthread_mutex_t trx_table_latch;

std::unordered_map<page_hash_t, lock_table_entry_t*> lock_table;
pthread_mutex_t lock_table_latch;

// APIs.

int trx_begin() {
  pthread_mutex_lock(&trx_table_latch);

  trx_t* trx = new trx_t;

  int trx_id = g_trx_id++;
  trx->trx_id = trx_id;
  trx->lock = NULL;
  trx->last_lsn = -1;
  trx_table[trx->trx_id] = trx;

  log_t* log = log_make_base_log(trx_id, BEGIN);
  log_add(log);

  pthread_mutex_unlock(&trx_table_latch);
  return trx_id;
}

int trx_commit(int trx_id) {
  pthread_mutex_lock(&trx_table_latch);

  trx_t* trx = trx_table[trx_id];
  if (trx != NULL) {
    lock_t* temp = trx->lock;
    while (temp != NULL) {
      lock_t* next = temp->trx_next;
      lock_release(temp);
      temp = next;
    }

    log_t* log = log_make_base_log(trx_id, COMMIT);
    log_add_and_flush(log);

    trx_table.erase(trx_id);
    delete trx;
  }

  pthread_mutex_unlock(&trx_table_latch);
  return trx_id;
}

int init_lock_table() {
  trx_table_latch = PTHREAD_MUTEX_INITIALIZER;
  lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
  return 0;
}

lock_t* lock_acquire(int64_t table_id,
                     pagenum_t page_id,
                     int64_t key,
                     int trx_id,
                     int lock_mode) {
  pthread_mutex_lock(&trx_table_latch);

  if (trx_table[trx_id] == NULL) {
    pthread_mutex_unlock(&trx_table_latch);
    return NULL;
  }

  pthread_mutex_lock(&lock_table_latch);

  lock_table_entry_t* entry = lock_table[{table_id, page_id}];
  if (entry == NULL) {
    entry = new lock_table_entry_t;
    entry->table_id = table_id;
    entry->page_id = page_id;
    entry->tail = NULL;
    entry->head = NULL;

    lock_table[{table_id, page_id}] = entry;
  }

  lock_t* lock = new lock_t;
  lock->prev = entry->tail;
  lock->next = NULL;
  lock->sentinel = entry;
  lock->cond_var = PTHREAD_COND_INITIALIZER;
  lock->record_id = key;
  lock->lock_mode = lock_mode;
  lock->trx_next = NULL;
  lock->trx_id = trx_id;

  if (entry->tail == NULL) {
    entry->head = lock;
  } else {
    entry->tail->next = lock;
  }
  entry->tail = lock;

  pthread_mutex_unlock(&lock_table_latch);

  trx_add_lock(trx_id, lock);

  if (trx_detect_deadlock(lock)) {
    pthread_mutex_unlock(&trx_table_latch);
    return NULL;
  }

  while (lock_need_to_wait(lock)) {
    pthread_cond_wait(&lock->cond_var, &trx_table_latch);
  }
  pthread_mutex_unlock(&trx_table_latch);
  return lock;
};

int lock_release(lock_t* lock_obj) {
  pthread_mutex_lock(&lock_table_latch);

  lock_wake_up_all_behind(lock_obj);

  lock_table_entry_t* entry = lock_obj->sentinel;
  if (entry->tail == lock_obj) {
    entry->tail = lock_obj->prev;
  }
  if (entry->head == lock_obj) {
    entry->head = lock_obj->next;
  }
  if (lock_obj->prev != NULL) {
    lock_obj->prev->next = lock_obj->next;
  }
  if (lock_obj->next != NULL) {
    lock_obj->next->prev = lock_obj->prev;
  }

  delete lock_obj;

  if (entry->tail == NULL) {
    lock_table.erase({entry->table_id, entry->page_id});
    delete entry;
  }

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}

int trx_shutdown_db() {
  std::vector<int> trx_ids;
  for (auto i : trx_table) {
    trx_ids.push_back(i.first);
  }
  for (int trx_id : trx_ids) {
    trx_abort(trx_id);
  }
  trx_table.clear();

  for (auto i : lock_table) {
    lock_table_entry_t* entry = i.second;
    lock_t* temp = entry->head;
    while (temp != NULL) {
      lock_t* next = temp->next;
      delete temp;
      temp = next;
    }
    delete entry;
  }
  lock_table.clear();

  return 0;
}

int trx_abort(int trx_id) {
  pthread_mutex_lock(&trx_table_latch);

  trx_t* trx = trx_table[trx_id];
  if (trx != NULL) {
    std::vector<log_t*> logs = log_trace(trx->last_lsn);
    for (log_t* log : logs) {
      if (log_get_type(log) == UPDATE) {
        log_undo(log);
      }
      delete[] log->data;
      delete log;
    }

    lock_t* temp = trx->lock;
    while (temp != NULL) {
      lock_t* next = temp->trx_next;
      lock_release(temp);
      temp = next;
    }

    log_t* log = log_make_base_log(trx_id, ROLLBACK);
    log_add_and_flush(log);

    trx_table.erase(trx_id);
    delete trx;
  }

  pthread_mutex_unlock(&trx_table_latch);
  return trx_id;
}

void trx_resurrect(int trx_id, int64_t last_lsn) {
  pthread_mutex_lock(&trx_table_latch);

  trx_t* trx = new trx_t;
  trx->last_lsn = last_lsn;
  trx_table[trx_id] = trx;

  pthread_mutex_unlock(&trx_table_latch);
}

// Utilities.

void trx_add_lock(int trx_id, lock_t* lock) {
  trx_t* trx = trx_table[trx_id];
  if (trx != NULL) {
    lock_t* temp = trx->lock;
    if (temp == NULL) {
      trx->lock = lock;
    } else {
      while (temp->trx_next != NULL) {
        temp = temp->trx_next;
      }
      temp->trx_next = lock;
    }
  }
}

int lock_need_to_wait(lock_t* lock) {
  lock_t* temp = lock->prev;
  while (temp != NULL) {
    if (temp->record_id == lock->record_id && temp->trx_id != lock->trx_id) {
      if (lock->lock_mode == EXCLUSIVE || temp->lock_mode == EXCLUSIVE) {
        return 1;
      }
    }
    temp = temp->prev;
  }
  return 0;
}

void lock_wake_up_all_behind(lock_t* lock) {
  lock_t* temp = lock->next;
  while (temp != NULL) {
    if (temp->record_id == lock->record_id) {
      pthread_cond_signal(&temp->cond_var);
      if (temp->lock_mode == EXCLUSIVE) {
        return;
      }
    }
    temp = temp->next;
  }
}

int trx_detect_deadlock(lock_t* lock) {
  std::set<int> checked;
  std::stack<int> st;

  for (int id : lock_waiting_list(lock)) {
    st.push(id);
  }
  while (st.size() > 0) {
    int trx_id = st.top();
    st.pop();

    if (checked.find(trx_id) != checked.end()) {
      continue;
    }

    if (trx_id == lock->trx_id) {
      return 1;
    }

    trx_t* trx = trx_table[trx_id];
    if (trx != NULL) {
      lock_t* temp = trx->lock;
      while (temp != NULL) {
        for (int id : lock_waiting_list(temp)) {
          st.push(id);
        }
        temp = temp->trx_next;
      }
    }

    checked.insert(trx_id);
  }

  return 0;
}

std::set<int> lock_waiting_list(lock_t* lock) {
  std::set<int> trx_ids;

  lock_t* temp = lock->prev;
  while (temp != NULL) {
    if (temp->record_id == lock->record_id && temp->trx_id != lock->trx_id) {
      if (lock->lock_mode == EXCLUSIVE || temp->lock_mode == EXCLUSIVE) {
        trx_ids.insert(temp->trx_id);
      }
    }
    temp = temp->prev;
  }
  return trx_ids;
}
