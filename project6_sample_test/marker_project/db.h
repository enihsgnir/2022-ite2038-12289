#ifndef DB_DB_H_
#define DB_DB_H_

#include <cstdint>
#include <vector>

// % Index manager APIs

int trx_begin(void);
int trx_commit(int trx_id);
int trx_abort(int trx_id);

// Open an existing database file or create one if not exist.
int64_t open_table(const char *pathname);

// Insert a record to the given table.
int db_insert(int64_t table_id, int64_t key, const char *value, uint16_t val_size);

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size, int trx_id);

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key);

int db_update(int64_t table_id, int64_t key, char *value, uint16_t new_val_size,
			  uint16_t *old_val_size, int trx_id);

// Find records with a key betwen the range: begin_key <= key <= end_key
int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
			std::vector<int64_t> *keys, std::vector<char*> *values,
			std::vector<uint16_t> *val_sizes);

// Initialize the database system.
int init_db(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path);

// Shutdown the databasee system.
int shutdown_db();

#endif  // DB_DB_H_
