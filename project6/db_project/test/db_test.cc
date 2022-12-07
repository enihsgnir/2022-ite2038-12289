#include "db.h"

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>

TEST(DbBasic, IsInitialRootNull) {
  init_db(1);

  std::string pathname = "db_basic.db";
  int64_t table_id = open_table(pathname.c_str());

  control_block_t* header_block = buf_read_page(table_id, 0);
  EXPECT_EQ(db_get_root_page_number(header_block->frame), 0);

  shutdown_db();
  remove(pathname.c_str());
}

int64_t table_id;
std::string pathname = "db_test.db";

int64_t n = 100000;
int num_buf = n / 25;
int max_num_length = std::to_string(n - 1).length();

std::string fixed_size_value(int i) {
  std::string num = std::to_string(i);
  std::string value = std::string(MIN_VAL_SIZE - max_num_length, 'a') +
                      std::string(max_num_length - num.length(), '0') + num;
  return value;
}

TEST(DbTest_FixedSizeLinearOrder, Init) {
  ASSERT_EQ(init_db(num_buf), 0);
  ASSERT_GE((table_id = open_table(pathname.c_str())), 0);
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
  ASSERT_EQ(remove(pathname.c_str()), 0);
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
  ASSERT_EQ(init_db(num_buf), 0);
  ASSERT_GE((table_id = open_table(pathname.c_str())), 0);
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
  ASSERT_EQ(remove(pathname.c_str()), 0);
}
