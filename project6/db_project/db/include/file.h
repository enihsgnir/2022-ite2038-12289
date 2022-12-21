#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <string>
#include <unordered_map>

// These definitions are not requirements.
// You may build your own way to handle the constants.
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB

#define FILE_PREFIX ("DATA")
#define MAX_NUM_TABLE (20)
#define DEFAULT_FILE_MODE (00644)
#define MAGIC_NUM (2022)

typedef uint64_t pagenum_t;

struct page_t {
  // in-memory page structure
  uint8_t data[PAGE_SIZE];
};

extern std::unordered_map<int64_t, int> fd_table;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id,
                     pagenum_t pagenum,
                     const struct page_t* src);

// Close the database file
void file_close_table_files();

int file_find_fd(int64_t table_id);

int64_t file_read_magic_number(int fd);
void file_write_magic_number(int fd, const int64_t magic_number);

pagenum_t file_read_first_free_page_number(int fd);
void file_write_first_free_page_number(int fd, const pagenum_t first);

uint64_t file_read_number_of_pages(int fd);
void file_write_number_of_pages(int fd, const uint64_t number_of_pages);

pagenum_t file_read_next_free_page_number(int fd, pagenum_t page_num);
void file_write_next_free_page_number(int fd,
                                      const pagenum_t next,
                                      pagenum_t page_num);

void file_extend_to_end(int fd, pagenum_t last);

#endif  // DB_FILE_H_
