#include "file.h"

std::unordered_map<int64_t, int> fd_table;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname) {
  int64_t table_id = std::stoll(std::string(pathname + 4));
  if (fd_table[table_id] != 0) {
    return table_id;
  }

  if (fd_table.size() >= MAX_NUM_TABLE) {
    return -1;
  }

  int fd = open(pathname, O_RDWR);
  if (fd < 0) {
    fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC, DEFAULT_FILE_MODE);
    if (fd < 0) {
      return fd;
    }

    file_write_magic_number(fd, MAGIC_NUM);

    uint64_t number_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    file_write_number_of_pages(fd, number_of_pages);

    pagenum_t first = number_of_pages - 1;
    file_write_first_free_page_number(fd, first);

    for (pagenum_t i = first; i > 0; i--) {
      file_write_next_free_page_number(fd, i - 1, i);
    }
    file_extend_to_end(fd, first);

    fsync(fd);
  }

  if (file_read_magic_number(fd) != MAGIC_NUM) {
    return -1;
  }

  fd_table[table_id] = fd;

  return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
  int fd = file_find_fd(table_id);

  pagenum_t first = file_read_first_free_page_number(fd);
  if (first == 0) {
    uint64_t number_of_pages = file_read_number_of_pages(fd);

    pagenum_t new_size = 2 * number_of_pages;
    file_write_number_of_pages(fd, new_size);

    first = new_size - 1;
    for (pagenum_t i = first; i > number_of_pages; i--) {
      file_write_next_free_page_number(fd, i - 1, i);
    }
    file_write_next_free_page_number(fd, 0, number_of_pages);
    file_extend_to_end(fd, first);

    fsync(fd);
  }

  pagenum_t next = file_read_next_free_page_number(fd, first);
  file_write_first_free_page_number(fd, next);

  fsync(fd);

  return first;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
  int fd = file_find_fd(table_id);

  pagenum_t first = file_read_first_free_page_number(fd);
  file_write_next_free_page_number(fd, first, pagenum);
  file_write_first_free_page_number(fd, pagenum);

  fsync(fd);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest) {
  int fd = file_find_fd(table_id);
  pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id,
                     pagenum_t pagenum,
                     const struct page_t* src) {
  int fd = file_find_fd(table_id);
  pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
  fsync(fd);
}

// Close the database file
void file_close_table_files() {
  for (auto i : fd_table) {
    close(i.second);
  }
  fd_table.clear();
}

int file_find_fd(int64_t table_id) {
  int fd = fd_table[table_id];
  if (fd == 0) {
    std::string pathname = FILE_PREFIX + std::to_string(table_id);
    if (file_open_table_file(pathname.c_str()) < 0) {
      return -1;
    }

    fd = fd_table[table_id];
  }
  return fd;
}

int64_t file_read_magic_number(int fd) {
  int64_t magic_number;
  pread(fd, &magic_number, 8, 0);
  return magic_number;
}

void file_write_magic_number(int fd, const int64_t magic_number) {
  pwrite(fd, &magic_number, 8, 0);
}

pagenum_t file_read_first_free_page_number(int fd) {
  pagenum_t first;
  pread(fd, &first, 8, 8);
  return first;
}

void file_write_first_free_page_number(int fd, const pagenum_t first) {
  pwrite(fd, &first, 8, 8);
}

uint64_t file_read_number_of_pages(int fd) {
  uint64_t number_of_pages;
  pread(fd, &number_of_pages, 8, 16);
  return number_of_pages;
}

void file_write_number_of_pages(int fd, const uint64_t number_of_pages) {
  pwrite(fd, &number_of_pages, 8, 16);
}

pagenum_t file_read_next_free_page_number(int fd, pagenum_t page_num) {
  pagenum_t next;
  pread(fd, &next, 8, page_num * PAGE_SIZE);
  return next;
}

void file_write_next_free_page_number(int fd,
                                      const pagenum_t next,
                                      pagenum_t page_num) {
  pwrite(fd, &next, 8, page_num * PAGE_SIZE);
}

void file_extend_to_end(int fd, pagenum_t last) {
  pwrite(fd, "\0", 1, (last + 1) * PAGE_SIZE - 1);
}
