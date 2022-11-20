#ifndef DB_H_
#define DB_H_

#include <stdlib.h>
#include <string.h>
#include <cstdint>
#include <vector>

#include "buffer.h"
#include "file.h"

#define DEFAULT_ORDER (249)
#define MIDDLE_OF_PAGE (1984)
#define MAX_VAL_SIZE (112)
#define THRESHOLD (2500)

// TYPES.

typedef struct slot_t {
  int64_t key;
  uint16_t size;
  uint16_t offset;
} slot_t;

// GLOBALS.

extern int32_t order;

// FUNCTION PROTOTYPES.

// APIs.

// Open an existing database file or create one if not exist.
int64_t open_table(const char* pathname);

// Insert a record to the given table.
int db_insert(int64_t table_id,
              int64_t key,
              const char* value,
              uint16_t val_size);

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key);

// Find records with a key between the range: begin_key <= key <= end_key.
int db_scan(int64_t table_id,
            int64_t begin_key,
            int64_t end_key,
            std::vector<int64_t>* keys,
            std::vector<char*>* values,
            std::vector<uint16_t>* val_sizes);

// Initialize the database system.
int init_db(int num_buf);

// Shutdown the database system.
int shutdown_db();

// Getters and setters.

// Default.

void db_get_data(void* dest, const page_t* src, uint16_t size, uint16_t offset);
void db_set_data(page_t* dest, const void* src, uint16_t size, uint16_t offset);

// Header Page.

pagenum_t db_get_root_page_number(const page_t* header);
void db_set_root_page_number(page_t* header, const pagenum_t root);

// Leaf/Internal Pages.

pagenum_t db_get_parent_page_number(const page_t* page);
void db_set_parent_page_number(page_t* page, const pagenum_t parent);

int32_t db_get_is_leaf(const page_t* page);
void db_set_is_leaf(page_t* page, const int32_t is_leaf);

int32_t db_get_number_of_keys(const page_t* page);
void db_set_number_of_keys(page_t* page, const int32_t number_of_keys);

int64_t db_get_amount_of_free_space(const page_t* page);
void db_set_amount_of_free_space(page_t* page,
                                 const int64_t amount_of_free_space);

// Leaf Page.

pagenum_t db_get_right_sibling_page_number(const page_t* leaf);
void db_set_right_sibling_page_number(
    page_t* leaf,
    const pagenum_t right_sibling_page_number);

slot_t db_get_slot(const page_t* leaf, int32_t index);
void db_set_slot(page_t* leaf, const slot_t slot, int32_t index);

void db_get_slots(const page_t* leaf, slot_t* slots, int32_t length);
void db_set_slots(page_t* leaf, const slot_t* slots, int32_t length);

void db_get_value(char* ret_val,
                  const page_t* leaf,
                  uint16_t size,
                  uint16_t offset);
void db_set_value(page_t* leaf,
                  const char* value,
                  uint16_t size,
                  uint16_t offset);

void db_get_values(const page_t* leaf,
                   const slot_t* slots,
                   char** values,
                   int32_t length);
void db_set_all_values_and_headers(page_t* leaf,
                                   slot_t* slots,
                                   char** values,
                                   int32_t length);

// Internal Page.

int64_t db_get_key(const page_t* internal, int32_t index);
void db_set_key(page_t* internal, const int64_t key, int32_t index);

void db_get_keys(const page_t* internal, int64_t* keys, int32_t length);
void db_set_keys(page_t* internal, const int64_t* keys, int32_t length);

pagenum_t db_get_child_page_number(const page_t* internal, int32_t index);
void db_set_child_page_number(page_t* internal,
                              const pagenum_t child,
                              int32_t index);

void db_get_children(const page_t* internal,
                     pagenum_t* children,
                     int32_t length);
void db_set_children(page_t* internal,
                     const pagenum_t* children,
                     int32_t length);

// Output and utility.

pagenum_t db_find_leaf(int64_t table_id, pagenum_t root, int64_t key);
int32_t cut(int32_t length);

// Insertion.

uint16_t db_get_next_offset(int64_t table_id,
                            pagenum_t leaf,
                            uint16_t val_size);
slot_t db_make_slot(int64_t key, uint16_t val_size, uint16_t offset);
pagenum_t db_make_page(int64_t table_id);
pagenum_t db_make_leaf(int64_t table_id);
int32_t db_get_left_index(int64_t table_id, pagenum_t parent, pagenum_t left);
int db_insert_into_leaf(int64_t table_id,
                        pagenum_t leaf,
                        int64_t key,
                        const char* value,
                        uint16_t val_size);
int db_insert_into_leaf_after_splitting(int64_t table_id,
                                        pagenum_t leaf,
                                        int64_t key,
                                        const char* value,
                                        uint16_t val_size);
int db_insert_into_internal(int64_t table_id,
                            pagenum_t parent,
                            int32_t left_index,
                            int64_t key,
                            pagenum_t right);
int db_insert_into_internal_after_splitting(int64_t table_id,
                                            pagenum_t old_internal,
                                            int32_t left_index,
                                            int64_t key,
                                            pagenum_t right);
int db_insert_into_parent(int64_t table_id,
                          pagenum_t left,
                          int64_t key,
                          pagenum_t right);
int db_insert_into_new_root(int64_t table_id,
                            pagenum_t left,
                            int64_t key,
                            pagenum_t right);
int db_start_new_tree(int64_t table_id,
                      int64_t key,
                      const char* value,
                      uint16_t val_size);

// Deletion.

int32_t db_get_neighbor_index(int64_t table_id, pagenum_t page_number);
void db_remove_entry_from_leaf(int64_t table_id, pagenum_t leaf, int64_t key);
void db_remove_entry_from_internal(int64_t table_id,
                                   pagenum_t internal,
                                   int64_t key);
int db_adjust_root(int64_t table_id, pagenum_t root);
int db_coalesce_leafs(int64_t table_id,
                      pagenum_t root,
                      pagenum_t leaf,
                      pagenum_t neighbor,
                      int32_t neighbor_index,
                      int64_t k_prime);
int db_coalesce_internals(int64_t table_id,
                          pagenum_t root,
                          pagenum_t internal,
                          pagenum_t neighbor,
                          int32_t neighbor_index,
                          int64_t k_prime);
int db_redistribute_leafs(int64_t table_id,
                          pagenum_t leaf,
                          pagenum_t neighbor,
                          int32_t neighbor_index,
                          int32_t k_prime_index,
                          int64_t k_prime);
int db_redistribute_internals(int64_t table_id,
                              pagenum_t internal,
                              pagenum_t neighbor,
                              int32_t neighbor_index,
                              int32_t k_prime_index,
                              int64_t k_prime);
int db_delete_entry(int64_t table_id,
                    pagenum_t root,
                    pagenum_t page_number,
                    int64_t key);

#endif  // DB_H_
