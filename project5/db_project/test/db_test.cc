#include "db.h"

#include <gtest/gtest.h>

#include <string>

TEST(DbTest, Shutdown) {
  init_db(1);

  std::string pathname = "db_shutdown_test.db";
  open_table(pathname.c_str());

  ASSERT_EQ(table_ids.size(), 1);

  shutdown_db();

  ASSERT_EQ(table_ids.size(), 0);

  remove(pathname.c_str());
}

TEST(DbTest, OpenSameFile) {
  init_db(1);

  std::string pathname = "db_test_same.db";

  int64_t table_id_1 = open_table(pathname.c_str());
  int64_t table_id_2 = open_table(pathname.c_str());

  EXPECT_TRUE(table_id_1 >= 0);
  EXPECT_TRUE(table_id_2 >= 0);

  EXPECT_NE(table_id_1, table_id_2);

  EXPECT_EQ(table_ids.size(), 2);

  shutdown_db();
  remove(pathname.c_str());
}

TEST(DbTest, TableNumberLimit) {
  init_db(1);

  auto pathname_at = [](int i) {
    std::string n1 = std::to_string(i / 10);
    std::string n2 = std::to_string(i % 10);
    return "db_test_" + n1 + n2 + ".db";
  };

  int length = 25;

  std::string pathname;
  for (int i = 0; i < length; i++) {
    std::string pathname = pathname_at(i);
    open_table(pathname.c_str());
  }

  EXPECT_EQ(table_ids.size(), 20);

  shutdown_db();
  for (int i = 0; i < length; i++) {
    std::string pathname = pathname_at(i);
    remove(pathname.c_str());
  }
}

class DbBasic : public ::testing::Test {
 protected:
  int num_buf;
  std::string pathname;
  int64_t table_id;

  DbBasic() {
    num_buf = 1000;
    init_db(num_buf);

    pathname = "db_test.db";
    table_id = open_table(pathname.c_str());
  }

  ~DbBasic() {
    shutdown_db();
    remove(pathname.c_str());
  }
};

TEST_F(DbBasic, GetterAndSetter) {
  pagenum_t page_number = file_alloc_page(table_id);
  page_t page;
  file_read_page(table_id, page_number, &page);

  int size = 10, offset = 50;

  char src[10];
  memset(src, 'a', size);
  db_set_data(&page, src, size, offset);

  char dest[10];
  db_get_data(dest, &page, size, offset);

  EXPECT_EQ(strncmp(src, dest, size), 0);
}

TEST_F(DbBasic, IsInitialRootNull) {
  page_t header_page;
  file_read_page(table_id, 0, &header_page);

  EXPECT_EQ(db_get_root_page_number(&header_page), 0);
}

class DbInsertAndDelete : public DbBasic {
 protected:
  int size;
  char* expected;
  char* actual;
  char* expected_b;
  char* expected_c;

  DbInsertAndDelete() : DbBasic() {
    size = 50;
    expected = (char*)malloc(sizeof(char) * size);
    if (expected == NULL) {
      exit(EXIT_FAILURE);
    }
    memset(expected, 'a', size);
    actual = (char*)malloc(sizeof(char) * size);
    if (actual == NULL) {
      exit(EXIT_FAILURE);
    }

    expected_b = (char*)malloc(sizeof(char) * size);
    if (expected_b == NULL) {
      exit(EXIT_FAILURE);
    }
    memset(expected_b, 'b', size);
    expected_c = (char*)malloc(sizeof(char) * size);
    if (expected_c == NULL) {
      exit(EXIT_FAILURE);
    }
    memset(expected_c, 'c', size);
  }

  ~DbInsertAndDelete() {
    free(expected);
    free(actual);

    free(expected_b);
    free(expected_c);
  }
};

TEST_F(DbInsertAndDelete, InsertOneAndDelete) {
  ASSERT_EQ(db_insert(table_id, 1, expected, size), 0);

  EXPECT_EQ(db_find(table_id, 1, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected, size), 0);

  EXPECT_NE(db_find(table_id, 2, actual, size), 0);

  EXPECT_EQ(db_delete(table_id, 1), 0);
  EXPECT_NE(db_find(table_id, 1, actual, size), 0);
}

TEST_F(DbInsertAndDelete, InsertTwo) {
  ASSERT_EQ(db_insert(table_id, 1, expected, size), 0);
  ASSERT_EQ(db_insert(table_id, 2, expected_b, size), 0);

  EXPECT_EQ(db_find(table_id, 1, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected, size), 0);

  EXPECT_EQ(db_find(table_id, 2, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_b, size), 0);
}

TEST_F(DbInsertAndDelete, InsertAndDeleteSameKey) {
  ASSERT_EQ(db_insert(table_id, 1, expected, size), 0);
  EXPECT_NE(db_insert(table_id, 1, expected, size), 0);

  ASSERT_EQ(db_delete(table_id, 1), 0);
  EXPECT_NE(db_delete(table_id, 1), 0);
}

// 128 + 64 * (12 + 50) == PAGE_SIZE
TEST_F(DbInsertAndDelete, Insert64AndDelete) {
  for (int i = 1; i <= 64; i++) {
    if (i == 32) {
      EXPECT_EQ(db_insert(table_id, i, expected_b, size), 0);
    } else {
      EXPECT_EQ(db_insert(table_id, i, expected, size), 0);
    }
  }

  EXPECT_EQ(db_find(table_id, 32, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_b, size), 0);

  EXPECT_EQ(db_delete(table_id, 32), 0);
  EXPECT_NE(db_find(table_id, 32, actual, size), 0);
}

// 32 * (12 + 50) >= MIDDLE_OF_PAGE
TEST_F(DbInsertAndDelete, Insert65AndDelete) {
  for (int i = 1; i <= 65; i++) {
    if (i == 31) {
      EXPECT_EQ(db_insert(table_id, i, expected_b, size), 0);
    } else if (i == 32) {
      EXPECT_EQ(db_insert(table_id, i, expected_c, size), 0);
    } else {
      EXPECT_EQ(db_insert(table_id, i, expected, size), 0);
    }
  }

  EXPECT_EQ(db_find(table_id, 31, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_b, size), 0);

  EXPECT_EQ(db_find(table_id, 32, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_c, size), 0);

  EXPECT_EQ(db_delete(table_id, 32), 0);
  EXPECT_NE(db_find(table_id, 32, actual, size), 0);
}

TEST_F(DbInsertAndDelete, InsertManyAndDelete) {
  for (int i = 1; i <= 512; i++) {
    if (i == 256) {
      EXPECT_EQ(db_insert(table_id, i, expected_b, size), 0);
    } else if (i == 512) {
      EXPECT_EQ(db_insert(table_id, i, expected_c, size), 0);
    } else {
      EXPECT_EQ(db_insert(table_id, i, expected, size), 0);
    }
  }

  EXPECT_EQ(db_find(table_id, 256, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_b, size), 0);

  EXPECT_EQ(db_find(table_id, 512, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_c, size), 0);

  for (int i = 128; i < 384; i++) {
    EXPECT_EQ(db_delete(table_id, i), 0);
  }
  for (int i = 128; i < 384; i++) {
    EXPECT_NE(db_find(table_id, i, actual, size), 0);
  }
}

TEST_F(DbInsertAndDelete, Scan) {
  for (int i = 1; i <= 65; i++) {
    EXPECT_EQ(db_insert(table_id, i, expected, size), 0);
  }

  std::vector<int64_t> keys;
  std::vector<char*> values;
  std::vector<uint16_t> val_sizes;

  EXPECT_EQ(db_scan(table_id, 16, 48, &keys, &values, &val_sizes), 0);
  for (int i = 0; i < 33; i++) {
    EXPECT_EQ(keys[i], i + 16);
  }
}

// 32 * order < 8192
TEST_F(DbInsertAndDelete, InsertTooManyAndDelete) {
  for (int i = 1; i <= 8192; i++) {
    if (i == 2048) {
      EXPECT_EQ(db_insert(table_id, i, expected_b, size), 0);
    } else if (i == 4096) {
      EXPECT_EQ(db_insert(table_id, i, expected_c, size), 0);
    } else {
      EXPECT_EQ(db_insert(table_id, i, expected, size), 0);
    }
  }

  EXPECT_EQ(db_find(table_id, 2048, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_b, size), 0);

  EXPECT_EQ(db_find(table_id, 4096, actual, size), 0);
  EXPECT_EQ(strncmp(actual, expected_c, size), 0);

  for (int i = 2048; i < 6144; i++) {
    EXPECT_EQ(db_delete(table_id, i), 0);
  }
  for (int i = 2048; i < 6144; i++) {
    EXPECT_NE(db_find(table_id, i, actual, size), 0);
  }
}
