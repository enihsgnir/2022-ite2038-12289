#include "file.h"

std::vector<int64_t> table_ids;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname) {
  int64_t table_id = open(pathname, O_RDWR);

  if (table_id < 0) {
    table_id = open(pathname, O_RDWR | O_CREAT | O_TRUNC, 00644);
    if (table_id < 0) {
      return table_id;
    }

    uint64_t magic_number = 2022;
    pwrite(table_id, &magic_number, 8, 0);

    uint64_t number_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    pagenum_t free_page_number = number_of_pages - 1;
    pwrite(table_id, &free_page_number, 8, 8);
    pwrite(table_id, &number_of_pages, 8, 16);

    for (pagenum_t i = free_page_number; i > 0; i--) {
      pagenum_t next = i - 1;
      pwrite(table_id, &next, 8, i * PAGE_SIZE);
    }

    fsync(table_id);
  }

  uint64_t magic;
  pread(table_id, &magic, 8, 0);
  if (magic != 2022) {
    return -1;
  }

  table_ids.push_back(table_id);
  return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
  pagenum_t free_page_number;
  pread(table_id, &free_page_number, 8, 8);

  pagenum_t next;
  if (free_page_number == 0) {
    uint64_t number_of_pages;
    pread(table_id, &number_of_pages, 8, 16);

    pagenum_t new_size = 2 * number_of_pages;
    pwrite(table_id, &new_size, 8, 16);

    free_page_number = new_size - 1;

    for (pagenum_t i = free_page_number; i > number_of_pages; i--) {
      next = i - 1;
      pwrite(table_id, &next, 8, i * PAGE_SIZE);
    }
    next = 0;
    pwrite(table_id, &next, 8, number_of_pages * PAGE_SIZE);

    fsync(table_id);
  }

  pread(table_id, &next, 8, free_page_number * PAGE_SIZE);
  pwrite(table_id, &next, 8, 8);
  fsync(table_id);

  return free_page_number;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
  pagenum_t free_page_number;
  pread(table_id, &free_page_number, 8, 8);

  pwrite(table_id, &free_page_number, 8, pagenum * PAGE_SIZE);
  pwrite(table_id, &pagenum, 8, 8);
  fsync(table_id);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest) {
  pread(table_id, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id,
                     pagenum_t pagenum,
                     const struct page_t* src) {
  pwrite(table_id, src, PAGE_SIZE, pagenum * PAGE_SIZE);
  fsync(table_id);
}

// Close the database file
void file_close_table_files() {
  for (int64_t i = 0; i < table_ids.size(); i++) {
    close(table_ids[i]);
  }
  table_ids.clear();
}
