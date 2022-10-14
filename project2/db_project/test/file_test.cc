#include "file.h"

#include <gtest/gtest.h>

#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileInitTest, HandlesInitialization) {
  int64_t fd;                             // file descriptor
  std::string pathname = "init_test.db";  // customize it to your test file

  // Open a database file
  fd = file_open_table_file(pathname.c_str());

  // Check if the file is opened
  ASSERT_TRUE(fd >= 0);  // change the condition to your design's behavior

  // Check the size of the initial file
  int num_pages = /* fetch the number of pages from the header page */ 2560;
  EXPECT_EQ(num_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE)
      << "The initial number of pages does not match the requirement: "
      << num_pages;

  // Close all database files
  file_close_table_files();

  // Remove the db file
  int is_removed = remove(pathname.c_str());

  ASSERT_EQ(is_removed, 0);
}

/*
 * TestFixture for page allocation/deallocation tests
 */
class FileTest : public ::testing::Test {
 protected:
  /*
   * NOTE: You can also use constructor/destructor instead of SetUp() and
   * TearDown(). The official document says that the former is actually
   * perferred due to some reasons. Checkout the document for the difference
   */
  FileTest() {
    pathname = "file_test.db";
    fd = file_open_table_file(pathname.c_str());
  }

  ~FileTest() {
    if (fd >= 0) {
      file_close_table_files();
    }
  }

  int64_t fd;            // file descriptor
  std::string pathname;  // path for the file
};

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST_F(FileTest, HandlesPageAllocation) {
  pagenum_t allocated_page, freed_page;

  // Allocate the pages
  allocated_page = file_alloc_page(fd);
  freed_page = file_alloc_page(fd);

  // Free one page
  file_free_page(fd, freed_page);

  // Traverse the free page list and check the existence of the freed/allocated
  // pages. You might need to open a few APIs soley for testing.

  bool allocated_page_exists = false;
  bool freed_page_exists = false;

  pagenum_t next;
  pread(fd, &next, 8, 8);
  while (next > 0) {
    if (next == allocated_page) {
      allocated_page_exists = true;
    }
    if (next == freed_page) {
      freed_page_exists = true;
    }

    pagenum_t temp = next;
    pread(fd, &next, 8, temp * PAGE_SIZE);
  }

  ASSERT_FALSE(allocated_page_exists);
  ASSERT_TRUE(freed_page_exists);
}

/*
 * Tests page read/write operations
 * 1. Write/Read a page with some random content and check if the data matches
 */
TEST_F(FileTest, CheckReadWriteOperation) {
  struct page_t* src = (struct page_t*)malloc(PAGE_SIZE);
  memset(src, 'a', PAGE_SIZE);

  pagenum_t pagenum = file_alloc_page(fd);

  file_write_page(fd, pagenum, src);

  struct page_t* dest = (struct page_t*)malloc(PAGE_SIZE);
  file_read_page(fd, pagenum, dest);

  EXPECT_EQ(memcmp(src, dest, PAGE_SIZE), 0);

  free(src);
  free(dest);
}
