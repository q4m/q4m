/*
 * Copyright (C) 2007,2008 Cybozu Labs, Inc.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#ifndef HA_QUEUE_H
#define HA_QUEUE_H

#include "cac/cac_mutex.h"
#include "cac/cac_rwlock.h"
#include "dllist.h"
#include "queue_cond.h"

// error numbers should be less than HA_ERR_FIRST
#define QUEUE_ERR_RECORD_EXISTS (1)

#define QUEUE_MAX_SOURCES (64)

class queue_share_t;

class queue_row_t {
  /* size is stored in the lower 30 bits, while upper 2 bits are used for
   * attributes.  if type == type_checksum, lower 30 bits adler32 is stored
   * in _size, and size of the checksum is stored in the body in type my_off_t.
   */
  char  _size[4];
  uchar _bytes[1];
public:
  enum {
    type_offset = 3, // location of type info
    type_mask                 = 0xe0000000,
    type_row                  = 0x00000000,
    type_row_received         = 0x20000000,
    type_checksum             = 0x40000000,
    type_row_removed          = 0x80000000,
    type_row_received_removed = 0xa0000000,
    type_num_rows_removed     = 0xc0000000,
    size_mask         = ~type_mask,
    max_size          = ~type_mask
  };
  queue_row_t() {} // build uninitialized
  queue_row_t(unsigned size, unsigned type) {
    assert((size & type_mask) == 0);
    int4store(_size, size | type);
  }
  unsigned size() const {
    // NOTE: does not check if the row isn't checksum
    return uint4korr(_size) & size_mask;
  }
  unsigned checksum() const {
    return size();
  }
  unsigned type() const {
    return uint4korr(_size) & type_mask;
  }
  void set_type(unsigned type) {
    assert((type & size_mask) == 0);
    int4store(_size, (uint4korr(_size) & size_mask) | type);
  }
  uchar *bytes() { return _bytes; }
  static size_t header_size() {
    return my_offsetof(queue_row_t, _bytes[0]);
  }
  static size_t checksum_size() {
    return header_size() + sizeof(my_off_t);
  }
  my_off_t next(my_off_t off) {
    off += header_size();
    switch (type()) {
    case type_checksum:
      off += sizeof(my_off_t);
      break;
    case type_num_rows_removed:
      break;
    default:
      off += size();
      break;
    }
    return off;
  }
  my_off_t validate_checksum(int fd, my_off_t off);
  static void create_checksum(queue_row_t *checksum, my_off_t sz,
			      uint32_t adler);
  // my_free should be used on deallocation
  static queue_row_t *create_checksum(const iovec* iov, int iovcnt);
private:
  queue_row_t(const queue_row_t&);
  queue_row_t& operator=(const queue_row_t&);
};

class queue_file_header_t {
public:
  enum {
    MAGIC_V1      = 0x304d3451, // 'Q4M0' in little endian
    MAGIC_V2      = 0x314d3451, // 'Q4M1' in little endian, w. conditional wait
    attr_is_dirty = 0x1
  };
  enum {
    _HEADER_SIZE = 4096
  };
private:
  char _magic[4];
  char _attr[4];
  char _end[8];
  char _begin[8];
  char _begin_row_id[8];
  char _last_received_offsets[QUEUE_MAX_SOURCES][8];
  char _row_count[8];
  char _bytes_total[8]; // sum of size of type_row(|_received)(|_removed)
  char _bytes_removed[8]; // sum of size of *_removed
  char _padding[_HEADER_SIZE
                - (4 + 4 + 8 + 8 + 8 + QUEUE_MAX_SOURCES * 8 + 8 + 8 + 8)];
public:
  queue_file_header_t();
  unsigned magic() const { return uint4korr(_magic); }
  unsigned attr() const { return uint4korr(_attr); }
  void set_attr(unsigned a) { int4store(_attr, a); }
  my_off_t end() const { return uint8korr(_end); }
  void set_end(my_off_t e) { int8store(_end, e); }
  my_off_t begin() const { return uint8korr(_begin); }
  my_off_t begin_row_id() const { return uint8korr(_begin_row_id); }
  void set_begin(my_off_t b, my_off_t i) {
    int8store(_begin, b);
    int8store(_begin_row_id, i);
  }
  my_off_t last_received_offset(unsigned i) const {
    return uint8korr(_last_received_offsets[i]);
  }
  void set_last_received_offset(unsigned i, my_off_t o) {
    int8store(_last_received_offsets[i], o);
  }
  my_off_t row_count() const { return uint8korr(_row_count); }
  void set_row_count(my_off_t c) { int8store(_row_count, c); }
  my_off_t bytes_total() const { return uint8korr(_bytes_total); }
  void set_bytes_total(my_off_t n) { int8store(_bytes_total, n); }
  my_off_t bytes_removed() const { return uint8korr(_bytes_removed); }
  void set_bytes_removed(my_off_t n) { int8store(_bytes_removed, n); }
  void write(int fd);
};

struct queue_source_t {
  char _offset[8];
  unsigned char _sender;
  unsigned sender() const { return _sender; }
  void set_sender(unsigned s) { _sender = s; }
  my_off_t offset() const { return uint8korr(_offset); }
  void set_offset(my_off_t o) { int8store(_offset, o); }
  queue_source_t(unsigned s, my_off_t o) {
    set_sender(s);
    set_offset(o);
  }
};

class queue_fixed_field_t {
protected:
  char *nam;
  size_t sz;
  size_t null_off;
  uchar null_bit;
public:
  queue_fixed_field_t(const TABLE *t, const Field *f, size_t s)
  : nam(new char [strlen(f->field_name) + 1]), sz(s),
#if MYSQL_VERSION_ID < 50600
    null_off(f->null_ptr != NULL ? f->null_ptr - t->record[0] : 0),
    null_bit(f->null_ptr != NULL ? f->null_bit : 0)
#else
    null_off(f->real_maybe_null() ? f->null_offset() : 0),
    null_bit(f->real_maybe_null() ? f->null_bit : 0)
#endif
  {
    strcpy(nam, f->field_name);
  }
  virtual ~queue_fixed_field_t() { delete [] nam; }
  virtual bool is_convertible() const { return false; }
  virtual queue_cond_t::value_t get_value(const uchar *buf, size_t off) const {
    return queue_cond_t::value_t::null_value();
  }
  bool is_null(const uchar *buf) const {
    return (buf[null_off] & null_bit) != 0;
  }
  const char *name() const { return nam; }
  size_t size() const { return sz; }
};

template<size_t N> class queue_int_field_t : public queue_fixed_field_t {
protected:
  bool is_unsigned;
public:
  queue_int_field_t(const TABLE *t, const Field *f)
  : queue_fixed_field_t(t, f, N),
    is_unsigned(f->key_type() == HA_KEYTYPE_BINARY) {}
  virtual ~queue_int_field_t() {}
  virtual bool is_convertible() const { return true; }
  virtual queue_cond_t::value_t get_value(const uchar *buf, size_t off) const {
    long long v;
    switch (N) {
#define TYPEREAD(sz, rd) \
    case sz: \
      v = rd; \
      if (! is_unsigned && (v & (LLONG_MIN >> (64 - sz * 8))) != 0) \
	v |= LLONG_MIN >> (64 - sz * 8); \
      break
      TYPEREAD(1, buf[off]);
      TYPEREAD(2, uint2korr(buf + off));
      TYPEREAD(3, uint3korr(buf + off));
      TYPEREAD(4, uint4korr(buf + off));
      TYPEREAD(8, uint8korr(buf + off));
    default:
      assert(0);
    }
    return queue_cond_t::value_t::int_value(v);
  }
};

struct queue_connection_t;
struct queue_compact_writer;

class queue_share_t {
  
public:
  
  struct append_t {
    const void *rows;
    size_t rows_size, row_count;
    const queue_source_t *source;
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
    append_t(const void *r, size_t rs, size_t rc, const queue_source_t *s)
    : rows(r), rows_size(rs), row_count(rc), source(s), err(-1) {
    }
  private:
    append_t(const append_t&);
    append_t& operator=(const append_t&);
  };
  typedef std::vector<append_t*> append_list_t;
  
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
  struct remove_t : public dllist<remove_t> {
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
# if Q4M_DELETE_METHOD == Q4M_DELETE_SERIAL_PWRITE
    my_off_t *offsets;
    int cnt;
    remove_t(my_off_t *o, int c) : err(-1), offsets(o), cnt(c) {}
# else
    remove_t() : err(-1) {}
# endif
  };
#endif
  
  struct cond_expr_t : public dllist<cond_expr_t> {
    queue_cond_t::node_t *node;
    char *expr;
    size_t expr_len;
    size_t ref_cnt;
    my_off_t pos;
    my_off_t row_id;
    cond_expr_t(queue_cond_t::node_t *n, const char *e, size_t el, my_off_t p,
		my_off_t i)
    : dllist<cond_expr_t>(), node(n), expr(new char [el + 1]), expr_len(el),
      ref_cnt(1), pos(p), row_id(i)
    {
      std::copy(e, e + el, expr);
      expr[el] = '\0';
    }
    void free(cond_expr_t **list) {
      if (list != NULL) {
	detach(*list);
      }
      delete [] expr;
      expr = NULL;
      delete node;
      node = NULL;
    }
  };
  
  struct listener_t {
    pthread_cond_t cond;
    queue_connection_t *listener;
    int queue_wait_index;
    listener_t(queue_connection_t *t) : listener(t), queue_wait_index(-1) {
      pthread_cond_init(&cond, NULL);
    }
    ~listener_t() {
      pthread_cond_destroy(&cond);
    }
  };
  struct listener_cond_t {
    listener_t *l;
    cond_expr_t *cond;
    int queue_wait_index;
    listener_cond_t(listener_t *_l, cond_expr_t *c, int qwi)
    : l(_l), cond(c), queue_wait_index(qwi) {}
  };
  typedef std::list<listener_cond_t> listener_list_t;
  
  struct info_t {
    queue_file_header_t _header;
    queue_connection_t *rows_owned;
    my_off_t max_owned_row_off;
    pthread_cond_t to_writer_cond;
    append_list_t *append_list;
    pthread_cond_t *append_response_cond;
    pthread_cond_t _append_response_conds[2];
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
    remove_t *remove_list;
    pthread_cond_t *remove_response_cond;
    pthread_cond_t _remove_response_conds[2];
#endif
    pthread_cond_t *do_compact_cond;
    bool is_deleting;
    
    queue_cond_t cond_eval;
    cond_expr_t *active_cond_exprs;
    cond_expr_t *inactive_cond_exprs;
    size_t inactive_cond_expr_cnt;
    bool writer_exit;
    
    size_t null_bytes;
    size_t fields;
    uchar *fixed_buf;
    size_t fixed_buf_size;
    
    my_off_t rows_written;
    my_off_t rows_removed;
    
    info_t() : _header(), rows_owned(NULL), max_owned_row_off(0),
	       append_list(new append_list_t()),
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
 remove_list(NULL),
#endif
               do_compact_cond(NULL), is_deleting(false), cond_eval(),
               active_cond_exprs(NULL), inactive_cond_exprs(NULL),
               inactive_cond_expr_cnt(0), writer_exit(false), null_bytes(0),
               fields(0), fixed_buf(NULL), fixed_buf_size(0), rows_written(0),
	       rows_removed(0)
    {
      pthread_cond_init(&to_writer_cond, NULL);
      pthread_cond_init(_append_response_conds, NULL);
      pthread_cond_init(_append_response_conds + 1, NULL);
      append_response_cond = _append_response_conds;
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
      pthread_cond_init(_remove_response_conds, NULL);
      pthread_cond_init(_remove_response_conds + 1, NULL);
      remove_response_cond = _remove_response_conds;
#endif
    }
    ~info_t() {
      delete [] fixed_buf;
      while (inactive_cond_exprs != NULL) {
	inactive_cond_exprs->free(&inactive_cond_exprs);
      }
      pthread_cond_destroy(_append_response_conds);
      pthread_cond_destroy(_append_response_conds + 1);
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
      pthread_cond_destroy(_remove_response_conds);
      pthread_cond_destroy(_remove_response_conds + 1);
      pthread_cond_destroy(&to_writer_cond);
      assert(remove_list == NULL);
#endif
      delete append_list;
    }
  };
  
  struct cond_expr_reset_pos_t {
    void operator()(struct info_t *info, cond_expr_t& e) const {
      e.pos = 0;
    }
  };
  
  struct stats_t {
    my_off_t wait_immediate_cnt;
    my_off_t wait_delayed_cnt;
    my_off_t wait_timeout_cnt;
    my_off_t abort_cnt;
    my_off_t close_cnt;
    stats_t() : wait_immediate_cnt(0), wait_delayed_cnt(0), wait_timeout_cnt(0), abort_cnt(0), close_cnt(0) {}
  };
  
private:
  uint ref_cnt;
  char *table_name;
  uint table_name_length;
  
  /* mutex:         used for many purposes
     mmap_mutex:    used for blocking remapping
     rwlock:        used to block compaction
     compact_mutex: used to serialize threads trying to obtain wrlock(rwlock)
     lock order is compact_mutex -> rwlock -> mutex -> mmap_mutex
   */
  pthread_mutex_t compact_mutex;
  pthread_rwlock_t rwlock;
  
  THR_LOCK store_lock;
  
#ifdef Q4M_USE_MMAP
  struct mmap_info_t {
    char *ptr;
    size_t len;
    mmap_info_t() : ptr(NULL), len(0) {}
  };
  cac_rwlock_t<mmap_info_t> map;
#endif
  
  int fd;
public:
  cac_mutex_t<info_t> info;
  cac_mutex_t<stats_t> *stats;
private:
  cond_expr_t cond_expr_true;
  
  listener_list_t listener_list; /* access serialized using listener_mutex */
  
  pthread_t writer_thread;
  bool writer_do_wake_listeners;
  
  /* following fields are for V2 type table only */
  queue_fixed_field_t **fixed_fields_;
  
public:
  void recalc_row_count(info_t *info, bool log);
  bool fixup_header(info_t *info);
#ifdef Q4M_USE_MMAP
  int mmap_table(size_t new_size);
#endif
  static uchar *get_share_key(queue_share_t *share, size_t *length,
			      my_bool not_used);
  static queue_share_t *get_share(const char* table_name, bool if_is_open = false);
  void detach();
  void release();
  bool init_fixed_fields();
  void init_fixed_fields(info_t *info, TABLE *_table);
  bool lock_reader(bool from_queue_wait = false);
  void unlock_reader(bool from_queue_wait = false, bool force_compaction = false);
  void register_listener(listener_t *l, cond_expr_t *c, int queue_wait_index) {
    listener_list.push_back(listener_cond_t(l, c, queue_wait_index));
  }
  void unregister_listener(listener_t *l);
  bool wake_listeners(bool from_writer = false);
  
  const char *get_table_name() const { return table_name; }
  THR_LOCK *get_store_lock() { return &store_lock; }
  queue_fixed_field_t * const *get_fixed_fields() const {
    return fixed_fields_;
  }
  my_off_t reset_owner(queue_connection_t *conn);
  int write_rows(const void *rows, size_t rows_size, size_t row_count);
  /* functions below requires lock */
  ssize_t read(void *data, my_off_t off, ssize_t size);
  int overwrite_byte(char byte, my_off_t off);
  int next(my_off_t *off, my_off_t* row_id);
  template <typename Func> void apply_cond_exprs(info_t* info, const Func& f) {
    cond_expr_t *e;
    if ((e = info->active_cond_exprs) != NULL) {
      do {
	f(info, *e);
      } while ((e = e->next()) != info->active_cond_exprs);
    }
    if ((e = info->inactive_cond_exprs) != NULL) {
      do {
	f(info, *e);
      } while ((e = e->next()) != info->inactive_cond_exprs);
    }
    f(info, cond_expr_true);
  }
  int remove_rows(my_off_t *offsets, int cnt);
  void remove_owner(queue_connection_t *conn);
  queue_connection_t *find_owner(info_t *info, my_off_t off);
  my_off_t assign_owner(info_t *info, queue_connection_t *conn,
			cond_expr_t *cond_expr);
  int setup_cond_eval(info_t *info, my_off_t pos);
  cond_expr_t* compile_cond_expr(info_t *info, const char *expr, size_t len);
  void release_cond_expr(cond_expr_t *e);
private:
  my_off_t check_cond_and_wake(info_t *info, my_off_t off, my_off_t row_id,
			       listener_cond_t *l);
  int writer_do_append(append_list_t *l);
  int do_remove_rows(my_off_t *offsets, int cnt);
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
  void writer_do_remove(remove_t *l);
#endif
  void *writer_start();
  static void *_writer_start(void* self) {
    return static_cast<queue_share_t*>(self)->writer_start();
  }
  my_off_t compact_do_copy(queue_compact_writer &writer, info_t *info, my_off_t *row_count, my_off_t* bytes_alive);
  int compact(info_t *info);
  queue_share_t();
  ~queue_share_t();
  queue_share_t(const queue_share_t&);
  queue_share_t& operator=(const queue_share_t&);
};

struct queue_connection_t : private dllist<queue_connection_t> {
  friend class dllist<queue_connection_t>;
  size_t reader_lock_cnt;
  bool owner_mode;
  queue_share_t *share_owned;
  my_off_t owned_row_off; /* might become 0 if owned row was removed by a DELETE statement and compaction occurred */
  my_off_t owned_row_id;
  my_off_t owned_row_off_post_compact;
  queue_source_t source;
  bool reset_source;
  void erase_owned();
  static size_t cnt;
  static queue_connection_t *current(bool create_if_empty = false);
  static int close(handlerton *hton, THD *thd);
private:
  queue_connection_t()
  : dllist<queue_connection_t>(), reader_lock_cnt(0), owner_mode(false),
    share_owned(NULL), owned_row_off(0), owned_row_id(0),
    owned_row_off_post_compact(0), source(0, 0), reset_source(false) {}
  ~queue_connection_t() {}
public:
  void add_to_owned_list(queue_connection_t *&head) {
    assert(! is_attached());
    attach_back(head);
  }
  queue_connection_t *remove_from_owned_list(queue_connection_t *&head) {
    return detach(head);
  }
  queue_connection_t *next_owned() {
    return next();
  }
};

class ha_queue: public handler
{
  THR_LOCK_DATA lock;
  queue_share_t *share;
  
  my_off_t pos;
  uchar *rows;
  size_t rows_size, rows_reserved;
  size_t bulk_insert_rows; /* should be -1 unless bulk_insertion */
  std::vector<my_off_t> *bulk_delete_rows;
  bool defer_reader_lock;
  
 public:
  ha_queue(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_queue();
  
  const char *table_type() const {
    return "QUEUE";
  }
  const char *index_type(uint) {
    return "NONE";
  }
  const char **bas_ext() const;
  ulonglong table_flags() const {
    return HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ | HA_CAN_GEOMETRY
      | HA_STATS_RECORDS_IS_EXACT | HA_CAN_BIT_FIELD
      /* | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE */ 
      | HA_FILE_BASED | HA_NO_AUTO_INCREMENT
      | HA_HAS_RECORDS;
  }

  ulong index_flags(uint, uint, bool) const {
    return 0;
  }
  
  int open(const char *name, int mode, uint test_if_locked);
  int close();
  int external_lock(THD *thd, int lock_type);
  int rnd_init(bool scan);
  int rnd_end();
  int rnd_next(uchar *buf);
  int rnd_pos(uchar *buf, uchar *_pos);
  void position(const uchar *record);
  
  int info(uint);
  ha_rows records();
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required
  uint8 table_cache_type() { return HA_CACHE_TBL_NOCACHE; }
  
  void start_bulk_insert(ha_rows rows);
  int end_bulk_insert();
  
  bool start_bulk_delete();
  int end_bulk_delete();
  
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
private:
  int delete_table(const char *name);
  int prepare_rows_buffer(size_t sz);
  void free_rows_buffer(bool force = false);
  void unpack_row(uchar *buf);
  size_t pack_row(uchar *buf, queue_source_t *source);
};

#undef queue_end

extern "C" {
  my_bool queue_wait_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_wait_deinit(UDF_INIT *initid);
  long long queue_wait(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
		       char *error);
  my_bool queue_end_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_end_deinit(UDF_INIT *initid);
  long long queue_end(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
		      char *error);
  my_bool queue_abort_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_abort_deinit(UDF_INIT *initid);
  long long queue_abort(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
			char *error);
  my_bool queue_rowid_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_rowid_deinit(UDF_INIT *initid);
  long long queue_rowid(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
			char *error);
  my_bool queue_set_srcid_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_set_srcid_deinit(UDF_INIT *initid);
  long long queue_set_srcid(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
			    char *error);
  my_bool queue_compact_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_compact_deinit(UDF_INIT *initid);
  long long queue_compact(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
			    char *error);
  my_bool queue_stats_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void queue_stats_deinit(UDF_INIT *initid);
  char* queue_stats(UDF_INIT *initid, UDF_ARGS *args, char *result,
		    unsigned long *length, char *is_null, char *error);
};

#endif
