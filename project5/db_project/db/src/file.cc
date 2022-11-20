#include "file.h"

std::vector<int64_t> table_ids;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname) {
  if (table_ids.size() >= MAX_NUM_TABLE) {
    return -1;
  }

  int64_t table_id = open(pathname, O_RDWR);
  if (table_id < 0) {
    table_id = open(pathname, O_RDWR | O_CREAT | O_TRUNC, DEFAULT_FILE_MODE);
    if (table_id < 0) {
      return table_id;
    }

    file_write_magic_number(table_id, MAGIC_NUM);

    uint64_t number_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    file_write_number_of_pages(table_id, number_of_pages);

    pagenum_t first = number_of_pages - 1;
    file_write_first_free_page_number(table_id, first);

    for (pagenum_t i = first; i > 0; i--) {
      file_write_next_free_page_number(table_id, i - 1, i);
    }

    fsync(table_id);
  }

  if (file_read_magic_number(table_id) != MAGIC_NUM) {
    return -1;
  }

  table_ids.push_back(table_id);

  return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
  pagenum_t first = file_read_first_free_page_number(table_id);
  if (first == 0) {
    uint64_t number_of_pages = file_read_number_of_pages(table_id);

    pagenum_t new_size = 2 * number_of_pages;
    file_write_number_of_pages(table_id, new_size);

    first = new_size - 1;
    for (pagenum_t i = first; i > number_of_pages; i--) {
      file_write_next_free_page_number(table_id, i - 1, i);
    }
    file_write_next_free_page_number(table_id, 0, number_of_pages);

    fsync(table_id);
  }

  pagenum_t next = file_read_next_free_page_number(table_id, first);
  file_write_first_free_page_number(table_id, next);

  fsync(table_id);

  return first;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
  pagenum_t first = file_read_first_free_page_number(table_id);
  file_write_next_free_page_number(table_id, first, pagenum);
  file_write_first_free_page_number(table_id, pagenum);
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
  for (int64_t table_id : table_ids) {
    close(table_id);
  }
  table_ids.clear();
}

int64_t file_read_magic_number(int64_t table_id) {
  int64_t magic_number;
  pread(table_id, &magic_number, 8, 0);
  return magic_number;
}

void file_write_magic_number(int64_t table_id, const int64_t magic_number) {
  pwrite(table_id, &magic_number, 8, 0);
}

pagenum_t file_read_first_free_page_number(int64_t table_id) {
  pagenum_t first;
  pread(table_id, &first, 8, 8);
  return first;
}

void file_write_first_free_page_number(int64_t table_id,
                                       const pagenum_t first) {
  pwrite(table_id, &first, 8, 8);
}

uint64_t file_read_number_of_pages(int64_t table_id) {
  uint64_t number_of_pages;
  pread(table_id, &number_of_pages, 8, 16);
  return number_of_pages;
}

void file_write_number_of_pages(int64_t table_id,
                                const uint64_t number_of_pages) {
  pwrite(table_id, &number_of_pages, 8, 16);
}

pagenum_t file_read_next_free_page_number(int64_t table_id, uint64_t index) {
  pagenum_t next;
  pread(table_id, &next, 8, index * PAGE_SIZE);
  return next;
}

void file_write_next_free_page_number(int64_t table_id,
                                      const pagenum_t next,
                                      uint64_t index) {
  pwrite(table_id, &next, 8, index * PAGE_SIZE);
}
