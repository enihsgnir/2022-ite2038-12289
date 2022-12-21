#include "db.h"

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>

int64_t table_id;
const char* pathname = "DATA1";
char log_path[] = "logfile.data";
char logmsg_path[] = "logmsg.txt";

TEST(DbBasic, IsInitialRootNull) {
  init_db(1, 0, 0, log_path, logmsg_path);

  table_id = open_table(pathname);

  control_block_t* header_block = buf_read_page(table_id, 0);
  EXPECT_EQ(db_get_root_page_number(header_block->frame), 0);

  shutdown_db();
  remove(pathname);
  remove(log_path);
  remove(logmsg_path);
}

int64_t n = 1000;
int num_buf = n / 25;
int max_num_length = std::to_string(n - 1).length();

std::string fixed_size_value(int i, char c = 'a') {
  std::string num = std::to_string(i);
  std::string value = std::string(MIN_VAL_SIZE - max_num_length, c) +
                      std::string(max_num_length - num.length(), '0') + num;
  return value;
}

TEST(DbTest_FixedSizeLinearOrder, Init) {
  ASSERT_EQ(init_db(num_buf, 0, 0, log_path, logmsg_path), 0);
  ASSERT_GE((table_id = open_table(pathname)), 0);
}

TEST(DbTest_FixedSizeLinearOrder, Insertion) {
  for (int64_t i = 0; i < n; i++) {
    std::string value = fixed_size_value(i);
    ASSERT_EQ(db_insert(table_id, i, value.c_str(), MIN_VAL_SIZE), 0);
  }
}

TEST(DbTest_FixedSizeLinearOrder, CheckInsertion) {
  for (int64_t i = 0; i < n; i++) {
    char ret_val[MAX_VAL_SIZE];
    uint16_t val_size;

    ASSERT_EQ(db_find(table_id, i, ret_val, &val_size), 0);
    EXPECT_EQ(val_size, MIN_VAL_SIZE);
    EXPECT_EQ(strncmp(ret_val, fixed_size_value(i).c_str(), MIN_VAL_SIZE), 0);
  }
}

TEST(DbTest_FixedSizeLinearOrder, Scan) {
  std::vector<int64_t> keys;
  std::vector<char*> values;
  std::vector<uint16_t> val_sizes;
  ASSERT_EQ(db_scan(table_id, n / 4, n / 2, &keys, &values, &val_sizes), 0);
  EXPECT_EQ(values.size(), n / 2 - n / 4 + 1);
  for (int64_t i = n / 4; i <= n / 2; i++) {
    EXPECT_EQ(keys[i - n / 4], i);
    EXPECT_EQ(val_sizes[i - n / 4], MIN_VAL_SIZE);
    EXPECT_EQ(
        strncmp(values[i - n / 4], fixed_size_value(i).c_str(), MIN_VAL_SIZE),
        0);
  }
}

TEST(DbTest_FixedSizeLinearOrder, Deletion) {
  for (int64_t i = 0; i < n; i++) {
    EXPECT_EQ(db_delete(table_id, i), 0);
  }
}

TEST(DbTest_FixedSizeLinearOrder, CheckDeletion) {
  for (int64_t i = 0; i < n; i++) {
    EXPECT_NE(db_find(table_id, i, NULL, NULL), 0);
  }
}

TEST(DbTest_FixedSizeLinearOrder, Shutdown) {
  ASSERT_EQ(shutdown_db(), 0);
  ASSERT_EQ(remove(pathname), 0);
  ASSERT_EQ(remove(log_path), 0);
  ASSERT_EQ(remove(logmsg_path), 0);
}

std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<uint16_t> dist(MIN_VAL_SIZE, MAX_VAL_SIZE);

std::string random_size_value(int i) {
  uint16_t size = dist(gen);
  std::string num = std::to_string(i);
  std::string value = std::string(size - max_num_length, 'a') +
                      std::string(max_num_length - num.length(), '0') + num;
  return value;
}

std::map<int64_t, std::string> map;

std::vector<int64_t> v;

TEST(DbTest_RandomSizeRandomOrder, Init) {
  ASSERT_EQ(init_db(num_buf, 0, 0, log_path, logmsg_path), 0);
  ASSERT_GE((table_id = open_table(pathname)), 0);
}

TEST(DbTest_RandomSizeRandomOrder, Insertion) {
  v.clear();
  for (int64_t i = 0; i < n; i++) {
    v.push_back(i);
  }
  std::shuffle(v.begin(), v.end(), gen);

  for (int64_t i : v) {
    std::string value = random_size_value(i);
    ASSERT_EQ(db_insert(table_id, i, value.c_str(), value.length()), 0);
    map.insert({i, value});
  }
}

TEST(DbTest_RandomSizeRandomOrder, CheckInsertion) {
  v.clear();
  for (int64_t i = 0; i < n; i++) {
    v.push_back(i);
  }
  std::shuffle(v.begin(), v.end(), gen);

  for (int64_t i : v) {
    char ret_val[MAX_VAL_SIZE];
    uint16_t val_size;

    ASSERT_EQ(db_find(table_id, i, ret_val, &val_size), 0);

    std::string value = map[i];
    EXPECT_NE(value, "");
    EXPECT_EQ(val_size, value.length());
    EXPECT_EQ(strncmp(ret_val, value.c_str(), val_size), 0);
  }
}

TEST(DbTest_RandomSizeRandomOrder, Scan) {
  std::vector<int64_t> keys;
  std::vector<char*> values;
  std::vector<uint16_t> val_sizes;
  ASSERT_EQ(db_scan(table_id, n / 4, n / 2, &keys, &values, &val_sizes), 0);
  EXPECT_EQ(values.size(), n / 2 - n / 4 + 1);
  for (int64_t i = n / 4; i <= n / 2; i++) {
    EXPECT_EQ(keys[i - n / 4], i);

    std::string value = map[i];
    EXPECT_NE(value, "");
    EXPECT_EQ(val_sizes[i - n / 4], value.length());
    EXPECT_EQ(strncmp(values[i - n / 4], value.c_str(), val_sizes[i - n / 4]),
              0);
  }
}

TEST(DbTest_RandomSizeRandomOrder, Deletion) {
  v.clear();
  for (int64_t i = 0; i < n; i++) {
    v.push_back(i);
  }
  std::shuffle(v.begin(), v.end(), gen);

  for (int64_t i : v) {
    EXPECT_EQ(db_delete(table_id, i), 0);
  }
}

TEST(DbTest_RandomSizeRandomOrder, CheckDeletion) {
  for (int64_t i = 0; i < n; i++) {
    EXPECT_NE(db_find(table_id, i, NULL, NULL), 0);
  }
}

TEST(DbTest_RandomSizeRandomOrder, Shutdown) {
  ASSERT_EQ(shutdown_db(), 0);
  ASSERT_EQ(remove(pathname), 0);
  ASSERT_EQ(remove(log_path), 0);
  ASSERT_EQ(remove(logmsg_path), 0);
}

#define THREAD_NUM (100)

void* trx_test(void* arg) {
  int trx_id;
  EXPECT_GT((trx_id = trx_begin()), 0);

  std::vector<std::pair<int64_t, int>> v;
  for (int64_t i = 0; i < n; i++) {
    v.push_back({i, SHARED});
    v.push_back({i, SHARED});
    v.push_back({i, EXCLUSIVE});
  }
  std::shuffle(v.begin(), v.end(), gen);

  for (auto e : v) {
    if (e.second == SHARED) {
      if (db_find(table_id, e.first, NULL, NULL, trx_id)) {
        break;
      }
    } else {
      std::string value = fixed_size_value(e.first, 'a' + trx_id % 26);
      if (db_update(table_id, e.first, (char*)value.c_str(), MIN_VAL_SIZE, NULL,
                    trx_id)) {
        break;
      }
    }
  }

  EXPECT_EQ(trx_commit(trx_id), trx_id);

  return NULL;
}

TEST(TrxTest, Init) {
  ASSERT_EQ(init_db(num_buf, 0, 0, log_path, logmsg_path), 0);
  ASSERT_GE((table_id = open_table(pathname)), 0);
}

TEST(TrxTest, Population) {
  for (int64_t i = 0; i < n; i++) {
    std::string value = fixed_size_value(i);
    ASSERT_EQ(db_insert(table_id, i, value.c_str(), MIN_VAL_SIZE), 0);
  }
}

TEST(TrxTest, Join) {
  pthread_t threads[THREAD_NUM];
  for (int i = 0; i < THREAD_NUM; i++) {
    pthread_create(&threads[i], NULL, trx_test, NULL);
  }

  for (int i = 0; i < THREAD_NUM; i++) {
    pthread_join(threads[i], NULL);
  }
}

TEST(TrxTest, Shutdown) {
  ASSERT_EQ(shutdown_db(), 0);
  ASSERT_EQ(remove(logmsg_path), 0);
}

TEST(LogTest, Recover) {
  EXPECT_EQ(init_db(num_buf, 0, 0, log_path, logmsg_path), 0);

  ASSERT_EQ(shutdown_db(), 0);
  ASSERT_EQ(remove(pathname), 0);
  ASSERT_EQ(remove(log_path), 0);
  ASSERT_EQ(remove(logmsg_path), 0);
}
