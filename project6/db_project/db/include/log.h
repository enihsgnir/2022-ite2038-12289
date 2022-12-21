#ifndef __LOG_H__
#define __LOG_H__

#include <algorithm>
#include <vector>

#include "trx.h"

// LOG TYPES.

#define BEGIN (0)
#define UPDATE (1)
#define COMMIT (2)
#define ROLLBACK (3)
#define COMPENSATE (4)

// RECOVERY FLAGS.

#define NORMAL_RECOVERY (0)
#define REDO_CRASH (1)
#define UNDO_CRASH (2)

struct log_t {
  uint8_t* data;
};

// GLOBALS.

extern int log_fd;

extern int64_t g_lsn;

extern std::vector<log_t*> log_buffer;
extern pthread_mutex_t log_buffer_latch;

// Getters and setters.

// Default.

void log_get_data(void* dest, const log_t* src, int size, int offset);
void log_set_data(log_t* dest, const void* src, int size, int offset);

// BEGIN/COMMIT/ROLLBACK

uint32_t log_get_log_size(const log_t* log);
void log_set_log_size(log_t* log, const uint32_t size);

int64_t log_get_lsn(const log_t* log);
void log_set_lsn(log_t* log, const int64_t lsn);

int64_t log_get_prev_lsn(const log_t* log);
void log_set_prev_lsn(log_t* log, const int64_t prev_lsn);

int32_t log_get_trx_id(const log_t* log);
void log_set_trx_id(log_t* log, int32_t trx_id);

int32_t log_get_type(const log_t* log);
void log_set_type(log_t* log, int32_t type);

// UPDATE

int64_t log_get_table_id(const log_t* log);
void log_set_table_id(log_t* log, int64_t table_id);

pagenum_t log_get_page_num(const log_t* log);
void log_set_page_num(log_t* log, pagenum_t page_num);

uint16_t log_get_offset(const log_t* log);
void log_set_offset(log_t* log, uint16_t offset);

uint16_t log_get_data_length(const log_t* log);
void log_set_data_length(log_t* log, uint16_t length);

void log_get_old_image(const log_t* log, char* old_val, uint16_t length);
void log_set_old_image(log_t* log, const char* old_val, uint16_t length);

void log_get_new_image(const log_t* log, char* new_val, uint16_t length);
void log_set_new_image(log_t* log, const char* new_val, uint16_t length);

// COMPENSATE

int64_t log_get_next_undo_lsn(const log_t* log);
void log_set_next_undo_lsn(log_t* log, int64_t next_undo_lsn);

// Buffer.

int64_t log_get_page_lsn(const page_t* page);
void log_set_page_lsn(page_t* page, int64_t page_lsn);

// Utilities.

log_t* log_make_base_log(int32_t trx_id, int32_t type, uint32_t size = 28);
log_t* log_make_update_log(int32_t trx_id,
                           int64_t table_id,
                           pagenum_t page_num,
                           uint16_t offset,
                           uint16_t length,
                           char* old_val,
                           char* new_val);
log_t* log_make_compensate_log(log_t* update_log);

static void _add(log_t* log);
void log_add(log_t* log);
static void _flush();
void log_flush();
void log_add_and_flush(log_t* log);

int log_init_db(char* log_path);
int log_shutdown_db();

log_t* log_read(uint32_t size, int64_t lsn);
log_t* log_copy(log_t* log);
void log_read_all(std::vector<log_t*>& redo_logs);
void log_get_losers(std::set<int32_t>& winners,
                    std::set<int32_t>& losers,
                    std::vector<log_t*>& redo_logs,
                    std::vector<log_t*>& undo_logs);
int log_redo(log_t* log);
void log_undo(log_t* update_log);
std::vector<log_t*> log_trace(int64_t last_lsn);
int log_recover(int flag, int log_num, char* logmsg_path);

#endif /* __LOG_H__ */
