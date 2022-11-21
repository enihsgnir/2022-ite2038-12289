#include "db.h"

// GLOBALS.

int32_t order = DEFAULT_ORDER;

// APIs.

// Open an existing database file or create one if not exist.
int64_t open_table(const char* pathname) {
  return buf_open_table_file(pathname);
}

// Insert a record to the given table.
int db_insert(int64_t table_id,
              int64_t key,
              const char* value,
              uint16_t val_size) {
  if (val_size < MIN_VAL_SIZE || val_size > MAX_VAL_SIZE) {
    return -1;
  }

  char temp_val[MAX_VAL_SIZE];
  uint16_t temp_size;
  if (db_find(table_id, key, temp_val, &temp_size) == 0) {
    return -1;
  }

  control_block_t* header_block = buf_read_page(table_id, 0);
  pagenum_t root = db_get_root_page_number(header_block->frame);
  buf_unpin_block(header_block, 0);

  if (root == 0) {
    return db_start_new_tree(table_id, key, value, val_size);
  }

  pagenum_t leaf = db_find_leaf(table_id, root, key);
  control_block_t* leaf_block = buf_read_page(table_id, leaf);
  int64_t amount_of_free_space = db_get_amount_of_free_space(leaf_block->frame);
  buf_unpin_block(leaf_block, 0);

  if (amount_of_free_space >= val_size) {
    return db_insert_into_leaf(table_id, leaf, key, value, val_size);
  }
  return db_insert_into_leaf_after_splitting(table_id, leaf, key, value,
                                             val_size);
}

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
  control_block_t* header_block = buf_read_page(table_id, 0);
  pagenum_t root = db_get_root_page_number(header_block->frame);
  buf_unpin_block(header_block, 0);

  pagenum_t leaf = db_find_leaf(table_id, root, key);
  if (leaf == 0) {
    return -1;
  }

  control_block_t* leaf_block = buf_read_page(table_id, leaf);
  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);
  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  int32_t i;
  for (i = 0; i < number_of_keys; i++) {
    if (slots[i].key == key) {
      break;
    }
  }
  if (i == number_of_keys) {
    return -1;
  }

  db_get_value(ret_val, leaf_block->frame, slots[i].size, slots[i].offset);
  *val_size = slots[i].size;

  buf_unpin_block(leaf_block, 0);

  delete[] slots;

  return 0;
}

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key) {
  char temp_val[MAX_VAL_SIZE];
  uint16_t temp_size;
  int find_result = db_find(table_id, key, temp_val, &temp_size);

  control_block_t* header_block = buf_read_page(table_id, 0);
  pagenum_t root = db_get_root_page_number(header_block->frame);
  buf_unpin_block(header_block, 0);

  pagenum_t leaf = db_find_leaf(table_id, root, key);

  if (find_result != 0 || leaf == 0) {
    return -1;
  }
  return db_delete_entry(table_id, root, leaf, key);
}

// Find records with a key between the range: begin_key <= key <= end_key.
int db_scan(int64_t table_id,
            int64_t begin_key,
            int64_t end_key,
            std::vector<int64_t>* keys,
            std::vector<char*>* values,
            std::vector<uint16_t>* val_sizes) {
  control_block_t* header_block = buf_read_page(table_id, 0);
  pagenum_t root = db_get_root_page_number(header_block->frame);
  buf_unpin_block(header_block, 0);

  pagenum_t page_number = db_find_leaf(table_id, root, begin_key);
  if (page_number == 0) {
    return -1;
  }

  control_block_t* block = buf_read_page(table_id, page_number);
  int32_t number_of_keys = db_get_number_of_keys(block->frame);
  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(block->frame, slots, number_of_keys);

  int32_t i = 0, j = 0;
  for (; i < number_of_keys && slots[i].key < begin_key; i++) {
    ;
  }
  if (i == number_of_keys) {
    return -1;
  }
  while (page_number != 0) {
    for (; i < number_of_keys && slots[i].key <= end_key; i++) {
      uint16_t size = slots[i].size;

      keys->push_back(slots[i].key);
      val_sizes->push_back(size);

      char* value = new char[size];
      db_get_value(value, block->frame, size, slots[i].offset);
      values->push_back(value);
    }

    page_number = db_get_right_sibling_page_number(block->frame);
    buf_unpin_block(block, 0);
    block = buf_read_page(table_id, page_number);
    number_of_keys = db_get_number_of_keys(block->frame);
    delete[] slots;
    slots = new slot_t[number_of_keys];
    db_get_slots(block->frame, slots, number_of_keys);
    i = 0;
  }

  buf_unpin_block(block, 0);

  delete[] slots;

  return 0;
}

// Initialize the database system.
int init_db(int num_buf) {
  return buf_init_db(num_buf);
}

// Shutdown the database system.
int shutdown_db() {
  return buf_shutdown_db();
}

// Getters and setters.

// Default.

void db_get_data(void* dest,
                 const page_t* src,
                 uint16_t size,
                 uint16_t offset) {
  memcpy(dest, (uint8_t*)src + offset, size);
}

void db_set_data(page_t* dest,
                 const void* src,
                 uint16_t size,
                 uint16_t offset) {
  memcpy((uint8_t*)dest + offset, src, size);
}

// Header Page.

pagenum_t db_get_root_page_number(const page_t* header) {
  pagenum_t root;
  db_get_data(&root, header, 8, 24);
  return root;
}

void db_set_root_page_number(page_t* header, const pagenum_t root) {
  db_set_data(header, &root, 8, 24);
}

// Leaf/Internal Pages.

pagenum_t db_get_parent_page_number(const page_t* page) {
  pagenum_t parent;
  db_get_data(&parent, page, 8, 0);
  return parent;
}

void db_set_parent_page_number(page_t* page, const pagenum_t parent) {
  db_set_data(page, &parent, 8, 0);
}

int32_t db_get_is_leaf(const page_t* page) {
  int32_t is_leaf;
  db_get_data(&is_leaf, page, 4, 8);
  return is_leaf;
}

void db_set_is_leaf(page_t* page, const int32_t is_leaf) {
  db_set_data(page, &is_leaf, 4, 8);
}

int32_t db_get_number_of_keys(const page_t* page) {
  int32_t number_of_keys;
  db_get_data(&number_of_keys, page, 4, 12);
  return number_of_keys;
}

void db_set_number_of_keys(page_t* page, const int32_t number_of_keys) {
  db_set_data(page, &number_of_keys, 4, 12);
}

int64_t db_get_amount_of_free_space(const page_t* page) {
  int64_t amount_of_free_space;
  db_get_data(&amount_of_free_space, page, 8, 112);
  return amount_of_free_space;
}

void db_set_amount_of_free_space(page_t* page,
                                 const int64_t amount_of_free_space) {
  db_set_data(page, &amount_of_free_space, 8, 112);
}

// Leaf Page.

pagenum_t db_get_right_sibling_page_number(const page_t* leaf) {
  pagenum_t right_sibling_page_number;
  db_get_data(&right_sibling_page_number, leaf, 8, 120);
  return right_sibling_page_number;
}

void db_set_right_sibling_page_number(
    page_t* leaf,
    const pagenum_t right_sibling_page_number) {
  db_set_data(leaf, &right_sibling_page_number, 8, 120);
}

slot_t db_get_slot(const page_t* leaf, int32_t index) {
  slot_t slot;
  db_get_data(&slot.key, leaf, 8, 128 + index * 12);
  db_get_data(&slot.size, leaf, 2, 128 + index * 12 + 8);
  db_get_data(&slot.offset, leaf, 2, 128 + index * 12 + 10);
  return slot;
}

void db_set_slot(page_t* leaf, const slot_t slot, int32_t index) {
  db_set_data(leaf, &slot.key, 8, 128 + index * 12);
  db_set_data(leaf, &slot.size, 2, 128 + index * 12 + 8);
  db_set_data(leaf, &slot.offset, 2, 128 + index * 12 + 10);
}

void db_get_slots(const page_t* leaf, slot_t* slots, int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    slots[i] = db_get_slot(leaf, i);
  }
}

void db_set_slots(page_t* leaf, const slot_t* slots, int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    db_set_slot(leaf, slots[i], i);
  }
}

void db_get_value(char* ret_val,
                  const page_t* leaf,
                  uint16_t size,
                  uint16_t offset) {
  db_get_data(ret_val, leaf, size, offset);
}

void db_set_value(page_t* leaf,
                  const char* value,
                  uint16_t size,
                  uint16_t offset) {
  db_set_data(leaf, value, size, offset);
}

void db_get_values(const page_t* leaf,
                   const slot_t* slots,
                   char** values,
                   int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    uint16_t size = slots[i].size;
    uint16_t offset = slots[i].offset;

    values[i] = new char[size];
    db_get_value(values[i], leaf, size, offset);
  }
}

void db_set_all_values_and_headers(page_t* leaf,
                                   slot_t* slots,
                                   char** values,
                                   int32_t length) {
  uint16_t offset = PAGE_SIZE;

  for (int32_t i = 0; i < length; i++) {
    uint16_t size = slots[i].size;
    offset -= size;
    slots[i].offset = offset;
    db_set_value(leaf, values[i], size, offset);
  }

  db_set_slots(leaf, slots, length);
  db_set_number_of_keys(leaf, length);
  db_set_amount_of_free_space(leaf, offset - (128 + 12 * length));
}

// Internal Page.

int64_t db_get_key(const page_t* internal, int32_t index) {
  int64_t key;
  db_get_data(&key, internal, 8, 128 + index * 16);
  return key;
}

void db_set_key(page_t* internal, const int64_t key, int32_t index) {
  db_set_data(internal, &key, 8, 128 + index * 16);
}

void db_get_keys(const page_t* internal, int64_t* keys, int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    keys[i] = db_get_key(internal, i);
  }
}

void db_set_keys(page_t* internal, const int64_t* keys, int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    db_set_key(internal, keys[i], i);
  }
}

pagenum_t db_get_child_page_number(const page_t* internal, int32_t index) {
  pagenum_t child;
  db_get_data(&child, internal, 8, 120 + index * 16);
  return child;
}

void db_set_child_page_number(page_t* internal,
                              const pagenum_t child,
                              int32_t index) {
  db_set_data(internal, &child, 8, 120 + index * 16);
}

void db_get_children(const page_t* internal,
                     pagenum_t* children,
                     int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    children[i] = db_get_child_page_number(internal, i);
  }
}

void db_set_children(page_t* internal,
                     const pagenum_t* children,
                     int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    db_set_child_page_number(internal, children[i], i);
  }
}

// Output and utility.

pagenum_t db_find_leaf(int64_t table_id, pagenum_t root, int64_t key) {
  if (root == 0) {
    return root;
  }

  pagenum_t page_number = root;
  control_block_t* block = buf_read_page(table_id, page_number);

  int32_t is_leaf = db_get_is_leaf(block->frame);
  while (!is_leaf) {
    int32_t number_of_keys = db_get_number_of_keys(block->frame);

    int32_t i = 0;
    while (i < number_of_keys) {
      if (key >= db_get_key(block->frame, i)) {
        i++;
      } else {
        break;
      }
    }

    page_number = db_get_child_page_number(block->frame, i);
    buf_unpin_block(block, 0);
    block = buf_read_page(table_id, page_number);
    is_leaf = db_get_is_leaf(block->frame);
  }

  buf_unpin_block(block, 0);

  return page_number;
}

int32_t cut(int32_t length) {
  if (length % 2 == 0)
    return length / 2;
  else
    return length / 2 + 1;
}

// Insertion.

uint16_t db_get_next_offset(int64_t table_id,
                            pagenum_t leaf,
                            uint16_t val_size) {
  control_block_t* leaf_block = buf_read_page(table_id, leaf);
  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);
  int64_t amount_of_free_space = db_get_amount_of_free_space(leaf_block->frame);
  buf_unpin_block(leaf_block, 0);

  return 128 + number_of_keys * 12 + amount_of_free_space - val_size;
}

slot_t db_make_slot(int64_t key, uint16_t val_size, uint16_t offset) {
  slot_t slot;
  slot.key = key;
  slot.size = val_size;
  slot.offset = offset;
  return slot;
}

pagenum_t db_make_page(int64_t table_id) {
  pagenum_t page_number = buf_alloc_page(table_id);

  control_block_t* block = buf_read_page(table_id, page_number);

  db_set_is_leaf(block->frame, 0);
  db_set_number_of_keys(block->frame, 0);
  db_set_parent_page_number(block->frame, 0);
  db_set_amount_of_free_space(block->frame, PAGE_SIZE - 120);

  buf_unpin_block(block, 1);

  return page_number;
}

pagenum_t db_make_leaf(int64_t table_id) {
  pagenum_t page_number = db_make_page(table_id);

  control_block_t* block = buf_read_page(table_id, page_number);

  db_set_is_leaf(block->frame, 1);
  db_set_amount_of_free_space(block->frame, PAGE_SIZE - 128);
  db_set_right_sibling_page_number(block->frame, 0);

  buf_unpin_block(block, 1);

  return page_number;
}

int32_t db_get_left_index(int64_t table_id, pagenum_t parent, pagenum_t left) {
  control_block_t* parent_block = buf_read_page(table_id, parent);
  int32_t number_of_keys = db_get_number_of_keys(parent_block->frame);

  int32_t left_index = 0;
  while (left_index <= number_of_keys &&
         db_get_child_page_number(parent_block->frame, left_index) != left) {
    left_index++;
  }

  buf_unpin_block(parent_block, 0);

  return left_index;
}

int db_insert_into_leaf(int64_t table_id,
                        pagenum_t leaf,
                        int64_t key,
                        const char* value,
                        uint16_t val_size) {
  control_block_t* leaf_block = buf_read_page(table_id, leaf);

  uint16_t offset = db_get_next_offset(table_id, leaf, val_size);
  slot_t new_slot = db_make_slot(key, val_size, offset);

  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);
  slot_t* slots = new slot_t[number_of_keys + 1];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  int32_t insertion_point = 0;
  while (insertion_point < number_of_keys && slots[insertion_point].key < key) {
    insertion_point++;
  }

  for (int32_t i = number_of_keys; i > insertion_point; i--) {
    slots[i] = slots[i - 1];
  }
  slots[insertion_point] = new_slot;
  db_set_value(leaf_block->frame, value, val_size, offset);
  db_set_slots(leaf_block->frame, slots, number_of_keys + 1);
  db_set_number_of_keys(leaf_block->frame, number_of_keys + 1);
  db_set_amount_of_free_space(
      leaf_block->frame,
      db_get_amount_of_free_space(leaf_block->frame) - (12 + val_size));

  buf_unpin_block(leaf_block, 1);

  delete[] slots;

  return 0;
}

int db_insert_into_leaf_after_splitting(int64_t table_id,
                                        pagenum_t leaf,
                                        int64_t key,
                                        const char* value,
                                        uint16_t val_size) {
  control_block_t* leaf_block = buf_read_page(table_id, leaf);

  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);
  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  char** values = new char*[number_of_keys];
  db_get_values(leaf_block->frame, slots, values, number_of_keys);

  slot_t* temp_slots = new slot_t[number_of_keys + 1];

  char** temp_values = new char*[number_of_keys + 1];

  int32_t insertion_index = 0;
  while (insertion_index < number_of_keys && slots[insertion_index].key < key) {
    insertion_index++;
  }

  for (int32_t i = 0, j = 0; i < number_of_keys; i++, j++) {
    if (j == insertion_index) {
      j++;
    }
    temp_slots[j] = slots[i];
    temp_values[j] = values[i];
  }
  temp_slots[insertion_index] = db_make_slot(key, val_size, 0);
  temp_values[insertion_index] = (char*)value;

  int32_t split_index = 0;
  uint16_t size_sum = 0;
  while (split_index <= number_of_keys) {
    size_sum += 12 + temp_slots[split_index].size;
    if (size_sum >= MIDDLE_OF_PAGE) {
      break;
    }
    split_index++;
  }

  db_set_all_values_and_headers(leaf_block->frame, slots, values, split_index);

  pagenum_t new_leaf = db_make_leaf(table_id);
  control_block_t* new_leaf_block = buf_read_page(table_id, new_leaf);

  db_set_all_values_and_headers(new_leaf_block->frame, temp_slots + split_index,
                                temp_values + split_index,
                                number_of_keys - split_index + 1);

  pagenum_t parent = db_get_parent_page_number(leaf_block->frame);
  db_set_parent_page_number(new_leaf_block->frame, parent);

  db_set_right_sibling_page_number(leaf_block->frame, new_leaf);

  int64_t new_key = temp_slots[split_index].key;

  buf_unpin_block(leaf_block, 1);
  buf_unpin_block(new_leaf_block, 1);

  delete[] slots;
  for (int32_t i = 0; i < split_index; i++) {
    delete[] values[i];
  }
  delete[] values;
  delete[] temp_slots;
  delete[] temp_values;

  return db_insert_into_parent(table_id, leaf, new_key, new_leaf);
}

int db_insert_into_internal(int64_t table_id,
                            pagenum_t parent,
                            int32_t left_index,
                            int64_t key,
                            pagenum_t right) {
  control_block_t* parent_block = buf_read_page(table_id, parent);

  int32_t number_of_keys = db_get_number_of_keys(parent_block->frame);

  pagenum_t* children = new pagenum_t[number_of_keys + 2];
  db_get_children(parent_block->frame, children, number_of_keys + 1);
  int64_t* keys = new int64_t[number_of_keys + 1];
  db_get_keys(parent_block->frame, keys, number_of_keys);

  for (int32_t i = number_of_keys; i > left_index; i--) {
    children[i + 1] = children[i];
    keys[i] = keys[i - 1];
  }
  children[left_index + 1] = right;
  keys[left_index] = key;

  db_set_children(parent_block->frame, children, number_of_keys + 2);
  db_set_keys(parent_block->frame, keys, number_of_keys + 1);
  db_set_number_of_keys(parent_block->frame, number_of_keys + 1);

  int64_t amount_of_free_space =
      db_get_amount_of_free_space(parent_block->frame);
  db_set_amount_of_free_space(parent_block->frame,
                              amount_of_free_space - 2 * 8);

  buf_unpin_block(parent_block, 1);

  delete[] children;
  delete[] keys;

  return 0;
}

int db_insert_into_internal_after_splitting(int64_t table_id,
                                            pagenum_t old_internal,
                                            int32_t left_index,
                                            int64_t key,
                                            pagenum_t right) {
  pagenum_t* temp_children = new pagenum_t[order + 1];

  int64_t* temp_keys = new int64_t[order];

  control_block_t* old_internal_block = buf_read_page(table_id, old_internal);

  int32_t number_of_keys = db_get_number_of_keys(old_internal_block->frame);

  int32_t i, j;
  for (i = 0, j = 0; i <= number_of_keys; i++, j++) {
    if (j == left_index + 1) {
      j++;
    }
    temp_children[j] = db_get_child_page_number(old_internal_block->frame, i);
  }
  for (i = 0, j = 0; i < number_of_keys; i++, j++) {
    if (j == left_index) {
      j++;
    }
    temp_keys[j] = db_get_key(old_internal_block->frame, i);
  }
  temp_children[left_index + 1] = right;
  temp_keys[left_index] = key;

  int32_t split = cut(order);

  pagenum_t new_internal = db_make_page(table_id);
  control_block_t* new_internal_block = buf_read_page(table_id, new_internal);

  for (i = 0; i < split - 1; i++) {
    db_set_child_page_number(old_internal_block->frame, temp_children[i], i);
    db_set_key(old_internal_block->frame, temp_keys[i], i);
  }
  db_set_child_page_number(old_internal_block->frame, temp_children[i], i);
  db_set_number_of_keys(old_internal_block->frame, split - 1);

  int32_t k_prime = temp_keys[split - 1];

  int32_t new_number_of_keys = order - split;
  for (++i, j = 0; i < order; i++, j++) {
    db_set_child_page_number(new_internal_block->frame, temp_children[i], j);
    db_set_key(new_internal_block->frame, temp_keys[i], j);
  }
  db_set_child_page_number(new_internal_block->frame, temp_children[i], j);
  db_set_number_of_keys(new_internal_block->frame, new_number_of_keys);

  pagenum_t parent = db_get_parent_page_number(old_internal_block->frame);
  db_set_parent_page_number(new_internal_block->frame, parent);

  for (i = 0; i <= new_number_of_keys; i++) {
    pagenum_t child = db_get_child_page_number(new_internal_block->frame, i);
    control_block_t* child_block = buf_read_page(table_id, child);
    db_set_parent_page_number(child_block->frame, new_internal);
    buf_unpin_block(child_block, 1);
  }

  buf_unpin_block(old_internal_block, 1);
  buf_unpin_block(new_internal_block, 1);

  delete[] temp_children;
  delete[] temp_keys;

  return db_insert_into_parent(table_id, old_internal, k_prime, new_internal);
}

int db_insert_into_parent(int64_t table_id,
                          pagenum_t left,
                          int64_t key,
                          pagenum_t right) {
  control_block_t* left_block = buf_read_page(table_id, left);
  pagenum_t parent = db_get_parent_page_number(left_block->frame);
  buf_unpin_block(left_block, 0);

  if (parent == 0) {
    return db_insert_into_new_root(table_id, left, key, right);
  }

  int32_t left_index = db_get_left_index(table_id, parent, left);

  control_block_t* parent_block = buf_read_page(table_id, parent);
  int32_t number_of_keys = db_get_number_of_keys(parent_block->frame);
  buf_unpin_block(parent_block, 0);

  if (number_of_keys < order - 1) {
    return db_insert_into_internal(table_id, parent, left_index, key, right);
  }
  return db_insert_into_internal_after_splitting(table_id, parent, left_index,
                                                 key, right);
}

int db_insert_into_new_root(int64_t table_id,
                            pagenum_t left,
                            int64_t key,
                            pagenum_t right) {
  pagenum_t root = db_make_page(table_id);
  control_block_t* root_block = buf_read_page(table_id, root);

  db_set_key(root_block->frame, key, 0);
  db_set_child_page_number(root_block->frame, left, 0);
  db_set_child_page_number(root_block->frame, right, 1);
  db_set_number_of_keys(root_block->frame, 1);
  db_set_amount_of_free_space(
      root_block->frame,
      db_get_amount_of_free_space(root_block->frame) - 3 * 8);

  control_block_t* header_block = buf_read_page(table_id, 0);
  db_set_root_page_number(header_block->frame, root);

  control_block_t* left_block = buf_read_page(table_id, left);
  db_set_parent_page_number(left_block->frame, root);

  control_block_t* right_block = buf_read_page(table_id, right);
  db_set_parent_page_number(right_block->frame, root);

  buf_unpin_block(root_block, 1);
  buf_unpin_block(header_block, 1);
  buf_unpin_block(left_block, 1);
  buf_unpin_block(right_block, 1);

  return 0;
}

int db_start_new_tree(int64_t table_id,
                      int64_t key,
                      const char* value,
                      uint16_t val_size) {
  pagenum_t root = db_make_leaf(table_id);
  control_block_t* root_block = buf_read_page(table_id, root);

  uint16_t offset = PAGE_SIZE - val_size;
  slot_t slot = db_make_slot(key, val_size, offset);
  db_set_slot(root_block->frame, slot, 0);
  db_set_value(root_block->frame, value, val_size, offset);
  db_set_number_of_keys(root_block->frame, 1);
  db_set_amount_of_free_space(
      root_block->frame,
      db_get_amount_of_free_space(root_block->frame) - (12 + val_size));

  control_block_t* header_block = buf_read_page(table_id, 0);
  db_set_root_page_number(header_block->frame, root);

  buf_unpin_block(root_block, 1);
  buf_unpin_block(header_block, 1);

  return 0;
}

// Deletion.

uint16_t db_get_total_data_size(const page_t* leaf_page) {
  int32_t number_of_keys = db_get_number_of_keys(leaf_page);

  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(leaf_page, slots, number_of_keys);

  uint16_t total_size = 0;
  for (int32_t i = 0; i < number_of_keys; i++) {
    total_size += 12 + slots[i].size;
  }

  delete[] slots;

  return total_size;
}

int32_t db_get_neighbor_index(int64_t table_id, pagenum_t page_number) {
  control_block_t* block = buf_read_page(table_id, page_number);

  pagenum_t parent = db_get_parent_page_number(block->frame);
  control_block_t* parent_block = buf_read_page(table_id, parent);

  int32_t number_of_keys = db_get_number_of_keys(parent_block->frame);
  pagenum_t* children = new pagenum_t[number_of_keys + 1];
  db_get_children(parent_block->frame, children, number_of_keys + 1);

  buf_unpin_block(block, 0);
  buf_unpin_block(parent_block, 0);

  for (int32_t i = 0; i <= number_of_keys; i++) {
    if (children[i] == page_number) {
      return i - 1;
    }
  }

  exit(EXIT_FAILURE);
}

void db_remove_entry_from_leaf(int64_t table_id, pagenum_t leaf, int64_t key) {
  control_block_t* leaf_block = buf_read_page(table_id, leaf);

  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);

  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  char** values = new char*[number_of_keys];
  db_get_values(leaf_block->frame, slots, values, number_of_keys);

  int32_t i = 0;
  while (slots[i].key != key) {
    i++;
  }
  for (++i; i < number_of_keys; i++) {
    slots[i - 1] = slots[i];
    values[i - 1] = values[i];
  }
  db_set_all_values_and_headers(leaf_block->frame, slots, values,
                                number_of_keys - 1);

  buf_unpin_block(leaf_block, 1);

  delete[] slots;
  for (int32_t i = 0; i < number_of_keys - 1; i++) {
    delete[] values[i];
  }
  delete[] values;
}

void db_remove_entry_from_internal(int64_t table_id,
                                   pagenum_t internal,
                                   int64_t key) {
  control_block_t* internal_block = buf_read_page(table_id, internal);

  int32_t number_of_keys = db_get_number_of_keys(internal_block->frame);

  int64_t* keys = new int64_t[number_of_keys];
  db_get_keys(internal_block->frame, keys, number_of_keys);

  pagenum_t* children = new pagenum_t[number_of_keys + 1];
  db_get_children(internal_block->frame, children, number_of_keys + 1);

  int32_t i = 0;
  while (keys[i] != key) {
    i++;
  }
  for (++i; i < number_of_keys; i++) {
    keys[i - 1] = keys[i];
    children[i] = children[i + 1];
  }
  db_set_keys(internal_block->frame, keys, number_of_keys - 1);
  db_set_children(internal_block->frame, children, number_of_keys);
  db_set_number_of_keys(internal_block->frame, number_of_keys - 1);

  buf_unpin_block(internal_block, 1);

  delete[] keys;
  delete[] children;
}

int db_adjust_root(int64_t table_id, pagenum_t root) {
  control_block_t* root_block = buf_read_page(table_id, root);

  int32_t number_of_keys = db_get_number_of_keys(root_block->frame);
  if (number_of_keys > 0) {
    buf_unpin_block(root_block, 0);
    return 0;
  }

  pagenum_t new_root = 0;

  int32_t is_leaf = db_get_is_leaf(root_block->frame);
  if (!is_leaf) {
    new_root = db_get_child_page_number(root_block->frame, 0);
    control_block_t* new_root_block = buf_read_page(table_id, new_root);
    db_set_parent_page_number(new_root_block->frame, 0);
    buf_unpin_block(new_root_block, 1);
  }

  control_block_t* header_block = buf_read_page(table_id, 0);
  db_set_root_page_number(header_block->frame, new_root);
  buf_unpin_block(header_block, 1);

  buf_free_page(table_id, root);

  return 0;
}

int db_coalesce_leafs(int64_t table_id,
                      pagenum_t root,
                      pagenum_t leaf,
                      pagenum_t neighbor,
                      int32_t neighbor_index,
                      int64_t k_prime) {
  if (neighbor_index == -1) {
    pagenum_t temp = leaf;
    leaf = neighbor;
    neighbor = temp;
  }

  control_block_t* leaf_block = buf_read_page(table_id, leaf);

  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);

  slot_t* slots = new slot_t[number_of_keys];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  char** values = new char*[number_of_keys];
  db_get_values(leaf_block->frame, slots, values, number_of_keys);

  control_block_t* neighbor_block = buf_read_page(table_id, neighbor);

  int32_t neighbor_number_of_keys =
      db_get_number_of_keys(neighbor_block->frame);

  slot_t* neighbor_slots = new slot_t[neighbor_number_of_keys + number_of_keys];
  db_get_slots(neighbor_block->frame, neighbor_slots, neighbor_number_of_keys);

  char** neighbor_values = new char*[neighbor_number_of_keys + number_of_keys];
  db_get_values(neighbor_block->frame, neighbor_slots, neighbor_values,
                neighbor_number_of_keys);

  pagenum_t parent = db_get_parent_page_number(leaf_block->frame);

  int32_t neighbor_insertion_index = neighbor_number_of_keys;

  int32_t i, j;
  for (i = neighbor_insertion_index, j = 0; j < number_of_keys; i++, j++) {
    neighbor_slots[i] = slots[j];
    neighbor_values[i] = values[j];
  }
  db_set_all_values_and_headers(neighbor_block->frame, neighbor_slots,
                                neighbor_values,
                                neighbor_number_of_keys + number_of_keys);

  db_set_right_sibling_page_number(
      neighbor_block->frame,
      db_get_right_sibling_page_number(leaf_block->frame));

  buf_unpin_block(neighbor_block, 1);

  buf_free_page(table_id, leaf);

  delete[] slots;
  delete[] values;
  delete[] neighbor_slots;
  for (i = 0; i < neighbor_number_of_keys + number_of_keys; i++) {
    delete[] neighbor_values[i];
  }
  delete[] neighbor_values;

  return db_delete_entry(table_id, root, parent, k_prime);
}

int db_coalesce_internals(int64_t table_id,
                          pagenum_t root,
                          pagenum_t internal,
                          pagenum_t neighbor,
                          int32_t neighbor_index,
                          int64_t k_prime) {
  if (neighbor_index == -1) {
    pagenum_t temp = internal;
    internal = neighbor;
    neighbor = temp;
  }

  control_block_t* internal_block = buf_read_page(table_id, internal);

  int32_t number_of_keys = db_get_number_of_keys(internal_block->frame);

  int64_t* keys = new int64_t[number_of_keys];
  db_get_keys(internal_block->frame, keys, number_of_keys);

  pagenum_t* children = new pagenum_t[number_of_keys + 1];
  db_get_children(internal_block->frame, children, number_of_keys + 1);

  control_block_t* neighbor_block = buf_read_page(table_id, neighbor);

  int32_t neighbor_number_of_keys =
      db_get_number_of_keys(neighbor_block->frame);

  int64_t* neighbor_keys =
      new int64_t[neighbor_number_of_keys + number_of_keys + 1];
  db_get_keys(neighbor_block->frame, neighbor_keys, neighbor_number_of_keys);

  pagenum_t* neighbor_children =
      new pagenum_t[neighbor_number_of_keys + number_of_keys + 2];
  db_get_children(neighbor_block->frame, neighbor_children,
                  neighbor_number_of_keys + 1);

  pagenum_t parent = db_get_parent_page_number(internal_block->frame);

  int32_t neighbor_insertion_index = neighbor_number_of_keys;

  neighbor_keys[neighbor_insertion_index] = k_prime;

  int32_t internal_end = number_of_keys;

  int32_t i, j;
  for (i = neighbor_insertion_index + 1, j = 0; j < internal_end; i++, j++) {
    neighbor_keys[i] = keys[j];
    neighbor_children[i] = children[j];
  }
  neighbor_children[i] = children[j];

  for (i = 0; i <= neighbor_number_of_keys + number_of_keys; i++) {
    pagenum_t temp = neighbor_children[i];
    control_block_t* temp_block = buf_read_page(table_id, temp);
    db_set_parent_page_number(temp_block->frame, neighbor);
    buf_unpin_block(temp_block, 1);
  }

  db_set_keys(neighbor_block->frame, neighbor_keys,
              neighbor_number_of_keys + number_of_keys + 1);
  db_set_children(neighbor_block->frame, neighbor_children,
                  neighbor_number_of_keys + number_of_keys + 2);
  db_set_number_of_keys(neighbor_block->frame,
                        neighbor_number_of_keys + number_of_keys + 1);

  buf_unpin_block(neighbor_block, 1);

  buf_free_page(table_id, internal);

  delete[] keys;
  delete[] children;
  delete[] neighbor_keys;
  delete[] neighbor_children;

  return db_delete_entry(table_id, root, parent, k_prime);
}

int db_redistribute_leafs(int64_t table_id,
                          pagenum_t leaf,
                          pagenum_t neighbor,
                          int32_t neighbor_index,
                          int32_t k_prime_index,
                          int64_t k_prime) {
  control_block_t* leaf_block = buf_read_page(table_id, leaf);

  control_block_t* neighbor_block = buf_read_page(table_id, neighbor);

  int32_t neighbor_number_of_keys =
      db_get_number_of_keys(neighbor_block->frame);

  slot_t* neighbor_slots = new slot_t[neighbor_number_of_keys];
  db_get_slots(neighbor_block->frame, neighbor_slots, neighbor_number_of_keys);

  char** neighbor_values = new char*[neighbor_number_of_keys];
  db_get_values(neighbor_block->frame, neighbor_slots, neighbor_values,
                neighbor_number_of_keys);

  int32_t neighbor_amount_of_free_space =
      db_get_amount_of_free_space(neighbor_block->frame);
  int32_t num_split = 0;
  if (neighbor_index != -1) {
    int32_t split_index = neighbor_number_of_keys - 1;
    uint16_t size_sum = 0;
    while (split_index >= 0) {
      size_sum += 12 + neighbor_slots[split_index].size;
      if (size_sum > neighbor_amount_of_free_space - THRESHOLD) {
        break;
      }
      split_index--;
      num_split++;
    }
  } else {
    int32_t split_index = 0;
    uint16_t size_sum = 0;
    while (split_index < neighbor_number_of_keys) {
      size_sum += 12 + neighbor_slots[split_index].size;
      if (size_sum > neighbor_amount_of_free_space - THRESHOLD) {
        break;
      }
      split_index++;
      num_split++;
    }
  }

  int32_t number_of_keys = db_get_number_of_keys(leaf_block->frame);

  slot_t* slots = new slot_t[number_of_keys + num_split];
  db_get_slots(leaf_block->frame, slots, number_of_keys);

  char** values = new char*[number_of_keys + num_split];
  db_get_values(leaf_block->frame, slots, values, number_of_keys);

  pagenum_t parent = db_get_parent_page_number(leaf_block->frame);
  control_block_t* parent_block = buf_read_page(table_id, parent);

  int32_t parent_number_of_keys = db_get_number_of_keys(parent_block->frame);

  int64_t* parent_keys = new int64_t[parent_number_of_keys];
  db_get_keys(parent_block->frame, parent_keys, parent_number_of_keys);

  int32_t i;
  if (neighbor_index != -1) {
    for (i = number_of_keys + num_split - 1; i >= num_split; i--) {
      slots[i] = slots[i - num_split];
      values[i] = values[i - num_split];
    }
    for (i = 0; i < num_split; i++) {
      values[i] = neighbor_values[neighbor_number_of_keys - num_split + i];
      slots[i] = neighbor_slots[neighbor_number_of_keys - num_split + i];
    }
    parent_keys[k_prime_index] = slots[0].key;
  } else {
    for (i = 0; i < num_split; i++) {
      slots[number_of_keys + i] = neighbor_slots[i];
      values[number_of_keys + i] = neighbor_values[i];
    }
    parent_keys[k_prime_index] = neighbor_slots[num_split + 1].key;
    for (i = 0; i < neighbor_number_of_keys - num_split; i++) {
      neighbor_slots[i] = neighbor_slots[i + num_split];
      neighbor_values[i] = neighbor_values[i + num_split];
    }
  }

  db_set_all_values_and_headers(leaf_block->frame, slots, values,
                                number_of_keys + num_split);
  db_set_all_values_and_headers(neighbor_block->frame, neighbor_slots,
                                neighbor_values,
                                neighbor_number_of_keys - num_split);
  db_set_keys(parent_block->frame, parent_keys, parent_number_of_keys);

  buf_unpin_block(leaf_block, 1);
  buf_unpin_block(neighbor_block, 1);
  buf_unpin_block(parent_block, 1);

  delete[] slots;
  for (i = 0; i < number_of_keys + num_split; i++) {
    delete[] values[i];
  }
  delete[] values;
  delete[] neighbor_slots;
  for (i = 0; i < neighbor_number_of_keys - num_split; i++) {
    delete[] neighbor_values[i];
  }
  delete[] neighbor_values;
  delete[] parent_keys;

  return 0;
}

int db_redistribute_internals(int64_t table_id,
                              pagenum_t internal,
                              pagenum_t neighbor,
                              int32_t neighbor_index,
                              int32_t k_prime_index,
                              int64_t k_prime) {
  control_block_t* internal_block = buf_read_page(table_id, internal);

  int32_t number_of_keys = db_get_number_of_keys(internal_block->frame);

  int64_t* keys = new int64_t[number_of_keys + 1];
  db_get_keys(internal_block->frame, keys, number_of_keys + 1);

  pagenum_t* children = new pagenum_t[number_of_keys + 2];
  db_get_children(internal_block->frame, children, number_of_keys + 2);

  control_block_t* neighbor_block = buf_read_page(table_id, neighbor);

  int32_t neighbor_number_of_keys =
      db_get_number_of_keys(neighbor_block->frame);

  int64_t* neighbor_keys = new int64_t[neighbor_number_of_keys];
  db_get_keys(neighbor_block->frame, neighbor_keys, neighbor_number_of_keys);

  pagenum_t* neighbor_children = new pagenum_t[neighbor_number_of_keys + 1];
  db_get_children(neighbor_block->frame, neighbor_children,
                  neighbor_number_of_keys + 1);

  pagenum_t parent = db_get_parent_page_number(internal_block->frame);
  control_block_t* parent_block = buf_read_page(table_id, parent);

  int32_t parent_number_of_keys = db_get_number_of_keys(parent_block->frame);

  int64_t* parent_keys = new int64_t[parent_number_of_keys];
  db_get_keys(parent_block->frame, parent_keys, parent_number_of_keys);

  int32_t i;
  if (neighbor_index != -1) {
    children[number_of_keys + 1] = children[number_of_keys];
    for (i = number_of_keys; i > 0; i--) {
      keys[i] = keys[i - 1];
      children[i] = children[i - 1];
    }

    children[0] = neighbor_children[neighbor_number_of_keys];
    pagenum_t temp = children[0];
    control_block_t* temp_block = buf_read_page(table_id, temp);
    db_set_parent_page_number(temp_block->frame, internal);
    buf_unpin_block(temp_block, 1);
    keys[0] = k_prime;
    parent_keys[k_prime_index] = neighbor_keys[neighbor_number_of_keys - 1];
  } else {
    keys[number_of_keys] = k_prime;
    children[number_of_keys + 1] = neighbor_children[0];
    pagenum_t temp = children[number_of_keys + 1];
    control_block_t* temp_block = buf_read_page(table_id, temp);
    db_set_parent_page_number(temp_block->frame, internal);
    buf_unpin_block(temp_block, 1);
    parent_keys[k_prime_index] = neighbor_keys[0];

    for (i = 0; i < neighbor_number_of_keys - 1; i++) {
      neighbor_keys[i] = neighbor_keys[i + 1];
      neighbor_children[i] = neighbor_children[i + 1];
    }
    neighbor_children[i] = children[i + 1];
  }

  db_set_number_of_keys(internal_block->frame, number_of_keys + 1);
  db_set_number_of_keys(neighbor_block->frame, neighbor_number_of_keys - 1);

  db_set_keys(internal_block->frame, keys, number_of_keys + 1);
  db_set_children(internal_block->frame, children, number_of_keys + 2);
  db_set_keys(neighbor_block->frame, neighbor_keys,
              neighbor_number_of_keys - 1);
  db_set_children(neighbor_block->frame, neighbor_children,
                  neighbor_number_of_keys);
  db_set_keys(parent_block->frame, parent_keys, parent_number_of_keys);

  buf_unpin_block(internal_block, 1);
  buf_unpin_block(neighbor_block, 1);
  buf_unpin_block(parent_block, 1);

  delete[] keys;
  delete[] children;
  delete[] neighbor_keys;
  delete[] neighbor_children;
  delete[] parent_keys;

  return 0;
}

int db_delete_entry(int64_t table_id,
                    pagenum_t root,
                    pagenum_t page_number,
                    int64_t key) {
  control_block_t* block = buf_read_page(table_id, page_number);

  int32_t is_leaf = db_get_is_leaf(block->frame);

  int64_t amount_of_free_space = db_get_amount_of_free_space(block->frame);
  int32_t number_of_keys = db_get_number_of_keys(block->frame);

  pagenum_t parent = db_get_parent_page_number(block->frame);

  uint16_t total_size = db_get_total_data_size(block->frame);

  buf_unpin_block(block, 0);

  if (is_leaf) {
    db_remove_entry_from_leaf(table_id, page_number, key);
  } else {
    db_remove_entry_from_internal(table_id, page_number, key);
  }

  if (page_number == root) {
    return db_adjust_root(table_id, root);
  }

  if (is_leaf) {
    if (amount_of_free_space < THRESHOLD) {
      return 0;
    }
  } else {
    if (number_of_keys >= cut(order) - 1) {
      return 0;
    }
  }

  control_block_t* parent_block = buf_read_page(table_id, parent);

  int32_t parent_number_of_keys = db_get_number_of_keys(parent_block->frame);

  int64_t* keys = new int64_t[parent_number_of_keys];
  db_get_keys(parent_block->frame, keys, parent_number_of_keys);

  pagenum_t* children = new pagenum_t[parent_number_of_keys + 1];
  db_get_children(parent_block->frame, children, parent_number_of_keys + 1);

  int32_t neighbor_index = db_get_neighbor_index(table_id, page_number);
  int32_t k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
  int64_t k_prime = keys[k_prime_index];
  pagenum_t neighbor =
      neighbor_index == -1 ? children[1] : children[neighbor_index];

  int32_t capacity = is_leaf ? order : order - 1;

  control_block_t* neighbor_block = buf_read_page(table_id, neighbor);

  int32_t neighbor_number_of_keys =
      db_get_number_of_keys(neighbor_block->frame);

  int64_t neighbor_amount_of_free_space =
      db_get_amount_of_free_space(neighbor_block->frame);

  buf_unpin_block(parent_block, 0);
  buf_unpin_block(neighbor_block, 0);

  delete[] keys;
  delete[] children;

  if (is_leaf && neighbor_amount_of_free_space >= total_size) {
    return db_coalesce_leafs(table_id, root, page_number, neighbor,
                             neighbor_index, k_prime);
  } else if (!is_leaf && neighbor_number_of_keys + number_of_keys < capacity) {
    return db_coalesce_internals(table_id, root, page_number, neighbor,
                                 neighbor_index, k_prime);
  }

  if (is_leaf) {
    return db_redistribute_leafs(table_id, page_number, neighbor,
                                 neighbor_index, k_prime_index, k_prime);
  }
  return db_redistribute_internals(table_id, page_number, neighbor,
                                   neighbor_index, k_prime_index, k_prime);
}
