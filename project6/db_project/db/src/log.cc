#include "log.h"

#include <iostream>

// GLOBALS.

int log_fd;

int64_t g_lsn = 0;

std::vector<log_t*> log_buffer;
pthread_mutex_t log_buffer_latch;

// Getters and setters.

// Default.

void log_get_data(void* dest, const log_t* src, int size, int offset) {
  memcpy(dest, src->data + offset, size);
}

void log_set_data(log_t* dest, const void* src, int size, int offset) {
  memcpy(dest->data + offset, src, size);
}

// BEGIN/COMMIT/ROLLBACK

uint32_t log_get_log_size(const log_t* log) {
  uint32_t size;
  log_get_data(&size, log, 4, 0);
  return size;
}

void log_set_log_size(log_t* log, const uint32_t size) {
  log_set_data(log, &size, 4, 0);
}

int64_t log_get_lsn(const log_t* log) {
  int64_t lsn;
  log_get_data(&lsn, log, 8, 4);
  return lsn;
}

void log_set_lsn(log_t* log, const int64_t lsn) {
  log_set_data(log, &lsn, 8, 4);
}

int64_t log_get_prev_lsn(const log_t* log) {
  int64_t prev_lsn;
  log_get_data(&prev_lsn, log, 8, 12);
  return prev_lsn;
}

void log_set_prev_lsn(log_t* log, const int64_t prev_lsn) {
  log_set_data(log, &prev_lsn, 8, 12);
}

int32_t log_get_trx_id(const log_t* log) {
  int32_t trx_id;
  log_get_data(&trx_id, log, 4, 20);
  return trx_id;
}

void log_set_trx_id(log_t* log, int32_t trx_id) {
  log_set_data(log, &trx_id, 4, 20);
}

int32_t log_get_type(const log_t* log) {
  int32_t type;
  log_get_data(&type, log, 4, 24);
  return type;
}

void log_set_type(log_t* log, int32_t type) {
  log_set_data(log, &type, 4, 24);
}

// UPDATE

int64_t log_get_table_id(const log_t* log) {
  int64_t table_id;
  log_get_data(&table_id, log, 8, 28);
  return table_id;
}

void log_set_table_id(log_t* log, int64_t table_id) {
  log_set_data(log, &table_id, 8, 28);
}

pagenum_t log_get_page_num(const log_t* log) {
  pagenum_t page_num;
  log_get_data(&page_num, log, 8, 36);
  return page_num;
}

void log_set_page_num(log_t* log, pagenum_t page_num) {
  log_set_data(log, &page_num, 8, 36);
}

uint16_t log_get_offset(const log_t* log) {
  uint16_t offset;
  log_get_data(&offset, log, 2, 44);
  return offset;
}

void log_set_offset(log_t* log, uint16_t offset) {
  log_set_data(log, &offset, 2, 44);
}

uint16_t log_get_data_length(const log_t* log) {
  uint16_t length;
  log_get_data(&length, log, 2, 46);
  return length;
}

void log_set_data_length(log_t* log, uint16_t length) {
  log_set_data(log, &length, 2, 46);
}

void log_get_old_image(const log_t* log, char* old_val, uint16_t length) {
  log_get_data(old_val, log, length, 48);
}

void log_set_old_image(log_t* log, const char* old_val, uint16_t length) {
  log_set_data(log, old_val, length, 48);
}

void log_get_new_image(const log_t* log, char* new_val, uint16_t length) {
  log_get_data(new_val, log, length, 48 + length);
}

void log_set_new_image(log_t* log, const char* new_val, uint16_t length) {
  log_set_data(log, new_val, length, 48 + length);
}

// COMPENSATE

int64_t log_get_next_undo_lsn(const log_t* log) {
  uint16_t length = log_get_data_length(log);

  int64_t next_undo_lsn;
  log_get_data(&next_undo_lsn, log, 8, 48 + 2 * length);
  return next_undo_lsn;
}

void log_set_next_undo_lsn(log_t* log, int64_t next_undo_lsn) {
  uint16_t length = log_get_data_length(log);
  log_set_data(log, &next_undo_lsn, 8, 48 + 2 * length);
}

// Buffer.

int64_t log_get_page_lsn(const page_t* page) {
  int64_t page_lsn;
  memcpy(&page_lsn, page->data + 32, 8);
  return page_lsn;
}

void log_set_page_lsn(page_t* page, int64_t page_lsn) {
  memcpy(page->data + 32, &page_lsn, 8);
}

log_t* log_make_base_log(int32_t trx_id, int32_t type, uint32_t size) {
  log_t* log = new log_t;
  log->data = new uint8_t[size];
  log_set_log_size(log, size);
  log_set_lsn(log, -1);
  log_set_prev_lsn(log, -1);
  log_set_trx_id(log, trx_id);
  log_set_type(log, type);
  return log;
}

log_t* log_make_update_log(int32_t trx_id,
                           int64_t table_id,
                           pagenum_t page_num,
                           uint16_t offset,
                           uint16_t length,
                           char* old_val,
                           char* new_val) {
  log_t* log = log_make_base_log(trx_id, UPDATE, 48 + 2 * length);
  log_set_table_id(log, table_id);
  log_set_page_num(log, page_num);
  log_set_offset(log, offset);
  log_set_data_length(log, length);
  log_set_old_image(log, old_val, length);
  log_set_new_image(log, new_val, length);
  return log;
}

log_t* log_make_compensate_log(log_t* update_log) {
  int32_t trx_id = log_get_trx_id(update_log);
  uint16_t length = log_get_data_length(update_log);
  log_t* log = log_make_base_log(trx_id, COMPENSATE, 48 + 2 * length + 8);

  int64_t table_id = log_get_table_id(update_log);
  log_set_table_id(log, table_id);

  pagenum_t page_num = log_get_page_num(update_log);
  log_set_page_num(log, page_num);

  uint16_t offset = log_get_offset(update_log);
  log_set_offset(log, offset);

  log_set_data_length(log, length);

  char* val = new char[length];
  log_get_old_image(update_log, val, length);
  log_set_new_image(log, val, length);
  log_get_new_image(update_log, val, length);
  log_set_old_image(log, val, length);

  int64_t prev_lsn = log_get_prev_lsn(update_log);
  log_set_next_undo_lsn(log, prev_lsn);

  delete[] val;

  return log;
}

static void _add(log_t* log) {
  int64_t lsn = g_lsn;
  log_set_lsn(log, lsn);
  g_lsn += log_get_log_size(log);

  int32_t trx_id = log_get_trx_id(log);
  trx_t* trx = trx_table[trx_id];
  if (trx != NULL) {
    int64_t prev_lsn = trx->last_lsn;
    log_set_prev_lsn(log, prev_lsn);
    trx->last_lsn = lsn;
  }

  log_buffer.push_back(log);
}

void log_add(log_t* log) {
  pthread_mutex_lock(&log_buffer_latch);

  _add(log);

  pthread_mutex_unlock(&log_buffer_latch);
}

static void _flush() {
  for (log_t* log : log_buffer) {
    uint32_t size = log_get_log_size(log);
    int64_t lsn = log_get_lsn(log);
    pwrite(log_fd, log->data, size, lsn);

    delete[] log->data;
    delete log;
  }
  fsync(log_fd);

  log_buffer.clear();
}

void log_flush() {
  pthread_mutex_lock(&log_buffer_latch);

  _flush();

  pthread_mutex_unlock(&log_buffer_latch);
}

void log_add_and_flush(log_t* log) {
  pthread_mutex_lock(&log_buffer_latch);

  _add(log);
  _flush();

  pthread_mutex_unlock(&log_buffer_latch);
}

int log_init_db(char* log_path) {
  log_buffer_latch = PTHREAD_MUTEX_INITIALIZER;

  log_fd = open(log_path, O_RDWR);
  if (log_fd < 0) {
    log_fd = open(log_path, O_RDWR | O_CREAT | O_TRUNC, DEFAULT_FILE_MODE);
    if (log_fd < 0) {
      return -1;
    }
  }
  return 0;
}

int log_shutdown_db() {
  close(log_fd);
  g_lsn = 0;
  log_buffer.clear();
  return 0;
}

log_t* log_read(uint32_t size, int64_t lsn) {
  log_t* log = new log_t;
  log->data = new uint8_t[size];
  pread(log_fd, log->data, size, lsn);

  return log;
}

log_t* log_copy(log_t* log) {
  uint32_t size = log_get_log_size(log);

  log_t* copy = new log_t;
  copy->data = new uint8_t[size];
  memcpy(copy->data, log->data, size);

  return copy;
}

void log_read_all(std::vector<log_t*>& redo_logs) {
  uint32_t size;
  while (pread(log_fd, &size, 4, g_lsn) > 0) {
    log_t* log = log_read(size, g_lsn);
    redo_logs.push_back(log);

    g_lsn += size;
  }
}

void log_get_losers(std::set<int32_t>& winners,
                    std::set<int32_t>& losers,
                    std::vector<log_t*>& redo_logs,
                    std::vector<log_t*>& undo_logs) {
  for (log_t* log : redo_logs) {
    int32_t type = log_get_type(log);
    int32_t trx_id = log_get_trx_id(log);

    if (type == BEGIN) {
      losers.insert(trx_id);
    } else if (type == COMMIT || type == ROLLBACK) {
      winners.insert(trx_id);
      losers.erase(trx_id);
    }
  }

  for (log_t* log : redo_logs) {
    int32_t trx_id = log_get_trx_id(log);
    if (losers.find(trx_id) != losers.end()) {
      log_t* copy = log_copy(log);
      undo_logs.push_back(copy);
    }
  }

  for (int32_t trx_id : losers) {
    auto is_same_trx = [=](log_t* log) {
      return log_get_trx_id(log) == trx_id;
    };
    auto last = std::find_if(undo_logs.rbegin(), undo_logs.rend(), is_same_trx);
    if (trx_table[trx_id] == NULL) {
      trx_resurrect(trx_id, log_get_lsn(*last));
    }

    if (log_get_type(*last) != COMPENSATE) {
      continue;
    }

    int64_t next_undo_lsn = log_get_next_undo_lsn(*last);
    auto is_newer_than_next_undo = [=](log_t* log) {
      return is_same_trx(log) && log_get_lsn(log) > next_undo_lsn;
    };
    undo_logs.erase(std::remove_if(undo_logs.begin(), undo_logs.end(),
                                   is_newer_than_next_undo),
                    undo_logs.end());
  }
  std::reverse(undo_logs.begin(), undo_logs.end());
}

int log_redo(log_t* log) {
  int64_t table_id = log_get_table_id(log);
  pagenum_t page_num = log_get_page_num(log);
  control_block_t* block = buf_read_page(table_id, page_num);

  int64_t lsn = log_get_lsn(log);
  if (lsn <= log_get_page_lsn(block->frame)) {
    buf_unpin_block(block, 0);
    return 0;
  }

  uint16_t offset = log_get_offset(log);
  uint16_t length = log_get_data_length(log);

  char* new_val = new char[length];
  log_get_new_image(log, new_val, length);

  memcpy(block->frame->data + offset, new_val, length);

  log_set_page_lsn(block->frame, lsn);

  buf_unpin_block(block, 1);

  delete[] new_val;

  return 1;
}

void log_undo(log_t* log) {
  int64_t table_id = log_get_table_id(log);
  pagenum_t page_num = log_get_page_num(log);
  control_block_t* block = buf_read_page(table_id, page_num);

  uint16_t offset = log_get_offset(log);
  uint16_t length = log_get_data_length(log);

  char* old_val = new char[length];
  log_get_old_image(log, old_val, length);

  memcpy(block->frame->data + offset, old_val, length);

  log_t* compensate_log = log_make_compensate_log(log);
  int64_t lsn = log_get_lsn(compensate_log);
  log_add(compensate_log);

  log_set_page_lsn(block->frame, lsn);

  buf_unpin_block(block, 1);

  delete[] old_val;
}

std::vector<log_t*> log_trace(int64_t last_lsn) {
  pthread_mutex_lock(&log_buffer_latch);

  std::vector<log_t*> logs;

  int64_t temp = last_lsn;
  for (auto it = log_buffer.rbegin(); it != log_buffer.rend(); it++) {
    if (temp < 0) {
      break;
    }

    if (log_get_lsn(*it) == temp) {
      log_t* copy = log_copy(*it);
      logs.push_back(copy);
      temp = log_get_prev_lsn(copy);
    }
  }
  while (temp >= 0) {
    uint32_t size;
    pread(log_fd, &size, 4, temp);

    log_t* log = log_read(size, temp);
    logs.push_back(log);
    temp = log_get_prev_lsn(log);
  }

  pthread_mutex_unlock(&log_buffer_latch);
  return logs;
}

int log_recover(int flag, int log_num, char* logmsg_path) {
  FILE* logmsg_fp = fopen(logmsg_path, "w");

  // ANALYZE

  fprintf(logmsg_fp, "[ANALYSIS] Analysis pass start\n");

  std::vector<log_t*> redo_logs;
  log_read_all(redo_logs);

  std::set<int32_t> winners, losers;
  std::vector<log_t*> undo_logs;
  log_get_losers(winners, losers, redo_logs, undo_logs);

  fprintf(logmsg_fp, "[ANALYSIS] Analysis success. Winner:");
  for (int32_t trx_id : winners) {
    fprintf(logmsg_fp, " %d", trx_id);
  }
  fprintf(logmsg_fp, ", Loser:");
  for (int32_t trx_id : losers) {
    fprintf(logmsg_fp, " %d", trx_id);
  }
  fprintf(logmsg_fp, "\n");

  // REDO

  fprintf(logmsg_fp, "[REDO] Redo pass start\n");

  int i = 0;
  while ((flag != REDO_CRASH || i < log_num) && i < redo_logs.size()) {
    log_t* log = redo_logs[i++];
    int32_t type = log_get_type(log);

    int64_t lsn = log_get_lsn(log);
    int32_t trx_id = log_get_trx_id(log);

    if (type == BEGIN) {
      fprintf(logmsg_fp, "LSN %lu [BEGIN] Transaction id %d\n", lsn, trx_id);
    } else if (type == COMMIT) {
      fprintf(logmsg_fp, "LSN %lu [COMMIT] Transaction id %d\n", lsn, trx_id);
    } else if (type == ROLLBACK) {
      fprintf(logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %d\n", lsn, trx_id);
    } else {
      if (log_redo(log)) {
        if (type == UPDATE) {
          fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n",
                  lsn, trx_id);
        } else {
          int64_t next_undo_lsn = log_get_next_undo_lsn(log);
          fprintf(logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n", lsn,
                  next_undo_lsn);
        }
      } else {
        fprintf(logmsg_fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", lsn,
                trx_id);
      }
    }
  }
  if (flag == REDO_CRASH) {
    return 0;
  }

  fprintf(logmsg_fp, "[REDO] Redo pass end\n");

  // UNDO

  fprintf(logmsg_fp, "[UNDO] Undo pass start\n");

  i = 0;
  while ((flag != UNDO_CRASH || i < log_num) && i < undo_logs.size()) {
    log_t* log = undo_logs[i++];
    int32_t type = log_get_type(log);

    int64_t lsn = log_get_lsn(log);
    int32_t trx_id = log_get_trx_id(log);

    if (type == BEGIN) {
      log_t* rollback_log = log_make_base_log(trx_id, ROLLBACK);
      log_add_and_flush(rollback_log);

      delete trx_table[trx_id];
      trx_table.erase(trx_id);
    } else if (type == UPDATE) {
      log_undo(log);
      fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", lsn,
              trx_id);
    }
  }
  if (flag == UNDO_CRASH) {
    return 0;
  }

  fprintf(logmsg_fp, "[UNDO] Undo pass end\n");

  fclose(logmsg_fp);

  log_flush();

  control_block_t* temp = head_block;
  while (temp != NULL) {
    if (temp->is_dirty) {
      file_write_page(temp->table_id, temp->page_num, temp->frame);
      temp->is_dirty = 0;
    }
    temp = temp->next;
  }

  for (log_t* log : redo_logs) {
    delete[] log->data;
    delete log;
  }
  for (log_t* log : undo_logs) {
    delete[] log->data;
    delete log;
  }

  return 0;
}
