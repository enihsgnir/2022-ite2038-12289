#include "buffer.h"

// GLOBALS.

std::unordered_map<page_hash_t, control_block_t*> control_block_table;
pthread_mutex_t buffer_manager_latch;

control_block_t* head_block;
control_block_t* tail_block;

// OPERATORS.

bool operator==(const page_hash_t& p1, const page_hash_t& p2) {
  return p1.table_id == p2.table_id && p1.page_num == p2.page_num;
}

std::size_t std::hash<page_hash_t>::operator()(
    page_hash_t const& p) const noexcept {
  std::size_t h1 = std::hash<int64_t>{}(p.table_id);
  std::size_t h2 = std::hash<pagenum_t>{}(p.page_num);
  return h1 ^ (h2 << 1);
}

// FUNCTION PROTOTYPES.

// APIs.

int64_t buf_open_table_file(const char* pathname) {
  return file_open_table_file(pathname);
}

int buf_init_db(int num_buf) {
  if (num_buf <= 0) {
    return -1;
  }

  buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

  head_block = buf_make_new_block();

  control_block_t* temp = head_block;
  for (int i = 1; i < num_buf; i++) {
    control_block_t* block = buf_make_new_block();
    temp->next = block;
    block->prev = temp;
    temp = block;
  }

  tail_block = temp;

  return 0;
}

int buf_shutdown_db() {
  control_block_table.clear();

  control_block_t* temp = head_block;
  while (temp != NULL) {
    if (temp->is_dirty) {
      file_write_page(temp->table_id, temp->page_num, temp->frame);
    }

    control_block_t* next = temp->next;
    delete temp->frame;
    delete temp;
    temp = next;
  }

  file_close_table_files();

  return 0;
}

pagenum_t buf_alloc_page(int64_t table_id) {
  pthread_mutex_lock(&buffer_manager_latch);

  pagenum_t first;

  control_block_t* header_block = control_block_table[{table_id, 0}];
  if (header_block == NULL) {
    first = file_alloc_page(table_id);
  } else {
    pthread_mutex_lock(&header_block->page_latch);

    if (header_block->is_dirty) {
      file_write_page(table_id, 0, header_block->frame);
      header_block->is_dirty = 0;
    }

    first = file_alloc_page(table_id);
    file_read_page(table_id, 0, header_block->frame);

    buf_refer_block(header_block);

    pthread_mutex_unlock(&header_block->page_latch);
  }

  pthread_mutex_unlock(&buffer_manager_latch);
  return first;
}

void buf_free_page(int64_t table_id, pagenum_t page_num) {
  pthread_mutex_lock(&buffer_manager_latch);

  control_block_t* block = control_block_table[{table_id, page_num}];
  if (block != NULL) {
    control_block_table.erase({table_id, page_num});
    buf_make_block_empty(block);
  }

  control_block_t* header_block = control_block_table[{table_id, 0}];
  if (header_block == NULL) {
    file_free_page(table_id, page_num);
  } else {
    pthread_mutex_lock(&header_block->page_latch);

    if (header_block->is_dirty) {
      file_write_page(table_id, 0, header_block->frame);
      header_block->is_dirty = 0;
    }

    file_free_page(table_id, page_num);
    file_read_page(table_id, 0, header_block->frame);

    buf_refer_block(header_block);

    pthread_mutex_unlock(&header_block->page_latch);
  }

  pthread_mutex_unlock(&buffer_manager_latch);
}

control_block_t* buf_read_page(int64_t table_id, pagenum_t page_num) {
  pthread_mutex_lock(&buffer_manager_latch);

  control_block_t* block = control_block_table[{table_id, page_num}];
  if (block == NULL) {
    block = buf_find_victim();
    if (block == NULL) {
      block = buf_make_new_block();
      head_block->prev = block;
      block->next = head_block;
      head_block = block;
    }

    if (block->is_dirty) {
      file_write_page(block->table_id, block->page_num, block->frame);
      block->is_dirty = 0;
    }

    control_block_table.erase({block->table_id, block->page_num});
    file_read_page(table_id, page_num, block->frame);
    block->table_id = table_id;
    block->page_num = page_num;
    control_block_table[{table_id, page_num}] = block;
  }

  buf_refer_block(block);

  pthread_mutex_lock(&block->page_latch);

  pthread_mutex_unlock(&buffer_manager_latch);
  return block;
}

void buf_unpin_block(control_block_t* block, int is_dirty) {
  block->is_dirty |= is_dirty;
  pthread_mutex_unlock(&block->page_latch);
}

// Utility.

control_block_t* buf_find_victim() {
  control_block_t* temp = tail_block;
  while (temp != NULL) {
    if (pthread_mutex_trylock(&temp->page_latch) == 0) {
      pthread_mutex_unlock(&temp->page_latch);
      return temp;
    }
    temp = temp->prev;
  }
  return temp;
}

void buf_refer_block(control_block_t* block) {
  if (block == head_block) {
    return;
  }

  if (block == tail_block) {
    block->prev->next = NULL;
    tail_block = block->prev;
  } else {
    block->prev->next = block->next;
    block->next->prev = block->prev;
  }

  head_block->prev = block;
  block->next = head_block;
  block->prev = NULL;
  head_block = block;
}

void buf_make_block_empty(control_block_t* block) {
  block->is_dirty = 0;
  pthread_mutex_unlock(&block->page_latch);

  if (block == tail_block) {
    return;
  }

  if (block == head_block) {
    block->next->prev = NULL;
    head_block = block->next;
  } else {
    block->prev->next = block->next;
    block->next->prev = block->prev;
  }

  tail_block->next = block;
  block->prev = tail_block;
  block->next = NULL;
  tail_block = block;
}

control_block_t* buf_make_new_block() {
  control_block_t* block = new control_block_t;
  block->frame = new page_t;
  block->table_id = -1;
  block->page_num = 0;
  block->is_dirty = 0;
  block->page_latch = PTHREAD_MUTEX_INITIALIZER;
  block->next = NULL;
  block->prev = NULL;
  return block;
}
