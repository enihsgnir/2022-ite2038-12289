#ifndef BUFFER_H_
#define BUFFER_H_

#include <string.h>
#include <unordered_map>

#include "file.h"

// TYPES.

struct control_block_t {
  page_t* frame;
  int64_t table_id;
  pagenum_t page_num;
  int is_dirty;
  int is_pinned;
  control_block_t* next;
  control_block_t* prev;
};

struct page_hash_t {
  int64_t table_id;
  pagenum_t page_num;
};

// GLOBALS.

extern std::unordered_map<page_hash_t, control_block_t*> control_block_table;

extern control_block_t* head_block;
extern control_block_t* tail_block;

// OPERATORS.

bool operator==(const page_hash_t& p1, const page_hash_t& p2);

template <>
struct std::hash<page_hash_t> {
  std::size_t operator()(page_hash_t const& p) const noexcept;
};

// FUNCTION PROTOTYPES.

// APIs.

int64_t buf_open_table_file(const char* pathname);
int buf_init_db(int num_buf);
int buf_shutdown_db();
pagenum_t buf_alloc_page(int64_t table_id);
void buf_free_page(int64_t table_id, pagenum_t page_num);
control_block_t* buf_read_page(int64_t table_id, pagenum_t page_num);
void buf_unpin_block(control_block_t* block, int is_dirty);

// Utilities.

control_block_t* buf_find_block(int64_t table_id, pagenum_t page_num);
control_block_t* buf_find_victim();
void buf_refer_block(control_block_t* block);
void buf_make_block_empty(control_block_t* block);

// Getters and Setters.

pagenum_t buf_get_first_free_page_number(const page_t* header);
void buf_set_first_free_page_number(page_t* header, const pagenum_t first);

pagenum_t buf_get_next_free_page_number(const page_t* page);
void buf_set_next_free_page_number(page_t* page, const pagenum_t next);

#endif  // BUFFER_H_
