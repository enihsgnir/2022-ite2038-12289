#include "file.h"

std::vector<int> fds;

// Open existing database file or create one if it doesn't exist
int file_open_database_file(const char* pathname) {
  int fd = open(pathname, O_RDWR);

  if (fd < 0) {
    fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC, 00644);
    if (fd < 0) {
      return fd;
    }

    uint64_t magic_number = 2022;
    pwrite(fd, &magic_number, 8, 0);

    pagenum_t number_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    pagenum_t free_page_number = number_of_pages - 1;
    pwrite(fd, &free_page_number, 8, 8);
    pwrite(fd, &number_of_pages, 8, 16);

    for (pagenum_t i = free_page_number; i > 0; i--) {
      struct page_t page;
      page.next = i - 1;
      pwrite(fd, &page, PAGE_SIZE, i * PAGE_SIZE);
    }

    fsync(fd);
  }

  uint64_t magic;
  pread(fd, &magic, 8, 0);
  if (magic != 2022) {
    return -1;
  }

  fds.push_back(fd);
  return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
  pagenum_t free_page_number;
  pread(fd, &free_page_number, 8, 8);

  if (free_page_number == 0) {
    pagenum_t number_of_pages;
    pread(fd, &number_of_pages, 8, 16);

    pagenum_t new_size = 2 * number_of_pages;
    pwrite(fd, &new_size, 8, 16);

    free_page_number = new_size - 1;

    for (pagenum_t i = free_page_number; i > number_of_pages; i--) {
      struct page_t page;
      page.next = i - 1;
      pwrite(fd, &page, PAGE_SIZE, i * PAGE_SIZE);
    }
    struct page_t last_page;
    last_page.next = 0;
    pwrite(fd, &last_page, PAGE_SIZE, number_of_pages * PAGE_SIZE);

    fsync(fd);
  }

  pagenum_t next;
  pread(fd, &next, 8, free_page_number * PAGE_SIZE);
  pwrite(fd, &next, 8, 8);
  fsync(fd);

  return free_page_number;
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
  pagenum_t free_page_number;
  pread(fd, &free_page_number, 8, 8);

  pwrite(fd, &free_page_number, 8, pagenum * PAGE_SIZE);
  pwrite(fd, &pagenum, 8, 8);
  fsync(fd);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, struct page_t* dest) {
  pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const struct page_t* src) {
  pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
  fsync(fd);
}

// Close the database file
void file_close_database_file() {
  for (int i = 0; i < fds.size(); i++) {
    close(fds[i]);
  }
  fds.clear();
}
