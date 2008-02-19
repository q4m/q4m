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
    type_mask     = 0xc0000000,
    type_row      = 0x00000000,
    type_removed  = 0x80000000,
    type_checksum = 0x40000000,
    size_mask     = ~type_mask,
    max_size      = ~type_mask
  };
  queue_row_t() {} // build uninitialized
  queue_row_t(unsigned size) {
    assert((size & type_mask) == 0);
    int4store(_size, size | type_row);
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
    return off + header_size()
      + (type() != type_checksum ? size() : sizeof(my_off_t));
  }
  my_off_t validate_checksum(int fd, my_off_t off);
  // my_free should be used on deallocation
  static queue_row_t *create_checksum(const iovec* iov, int iovcnt);
private:
  queue_row_t(const queue_row_t&);
  queue_row_t& operator=(const queue_row_t&);
};

class queue_file_header_t {
public:
  enum {
    MAGIC         = 0x304d3451, // 'Q4M0' in little endian
    attr_is_dirty = 0x1
  };
private:
  char _magic[4];
  char _attr[4];
  char _end[8];
  char _begin[8];
  unsigned _padding[1024 - (4 + 4 + 8 + 8)];
public:
  queue_file_header_t();
  unsigned magic() const { return uint4korr(_magic); }
  unsigned attr() const { return uint4korr(_attr); }
  void set_attr(unsigned a) { int4store(_attr, a); }
  my_off_t end() const { return uint8korr(_end); }
  void set_end(my_off_t e) { int8store(_end, e); }
  my_off_t begin() const { return uint8korr(_begin); }
  void set_begin(my_off_t b) { int8store(_begin, b); }
  void write(int fd);
};

typedef std::list<std::pair<pthread_t, my_off_t> > queue_rows_owned_t;

class queue_share_t {
  
 public:
  struct append_t {
    const void *rows;
    size_t rows_size;
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
    append_t(const void *r, size_t rs)
    : rows(r), rows_size(rs), err(-1) {
    }
  private:
    append_t(const append_t&);
    append_t& operator=(const append_t&);
  };
  typedef std::vector<append_t*> append_list_t;
  
  struct remove_t {
    my_off_t *offsets;
    int cnt;
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
    remove_t(my_off_t *o, int c)
    : offsets(o), cnt(c), err(-1) {
    }
  };
  typedef std::vector<remove_t*> remove_list_t;
  
 private:
  uint use_count;
  char *table_name;
  uint table_name_length;
  
  pthread_mutex_t mutex, append_mutex;
  THR_LOCK store_lock;
  
  enum {
    e_sync,
    e_volatile,
  } mode;
  
  int fd;
  queue_file_header_t _header;
  
  struct {
    my_off_t off;
    char buf[4096];
  } cache;
  
  queue_rows_owned_t rows_owned;
  
  pthread_cond_t queue_cond;
  int num_readers;
  
  pthread_t writer_thread;
  pthread_cond_t to_writer_cond;
  pthread_cond_t *from_writer_cond;
  pthread_cond_t _from_writer_conds[2];
  bool writer_exit;
  append_list_t *append_list;
  remove_list_t *remove_list;
  
public:
  void fixup_header();
  static uchar *get_share_key(queue_share_t *share, size_t *length,
			      my_bool not_used);
  static queue_share_t *get_share(const char* table_name);
  void release();
  void lock() { pthread_mutex_lock(&mutex); }
  void unlock() { pthread_mutex_unlock(&mutex); }
  void lock_reader() { lock(); ++num_readers; unlock(); }
  void unlock_reader();
  void wake_listener() { pthread_cond_signal(&queue_cond); }
  int wait(time_t t) {
    timespec ts = { t, 0 };
    return pthread_cond_timedwait(&queue_cond, &mutex, &ts);
  }
  THR_LOCK *get_store_lock() { return &store_lock; }
  const queue_file_header_t *header() const { return &_header; }
  my_off_t reset_owner(pthread_t owner);
  int write_rows(const void *rows, size_t rows_size);
  /* functions below requires lock */
  const void *read_cache(my_off_t off, ssize_t size, bool populate_cache);
  ssize_t read(void *data, my_off_t off, ssize_t size, bool populate_cache);
  void update_cache(const void *data, my_off_t off, size_t size) {
    if (cache.off == 0
	|| cache.off + sizeof(cache.buf) <= off || off + size <= cache.off) {
      // nothing to do
    } else if (cache.off <= off) {
      memcpy(cache.buf + off - cache.off,
	     data,
	     min(size, sizeof(cache.buf) - (off - cache.off)));
    } else {
      memcpy(cache.buf,
	     static_cast<const char*>(data) + cache.off - off,
	     min(size - (cache.off - off), sizeof(cache.buf)));
    }
  }
  int next(my_off_t *off);
  my_off_t get_owned_row(pthread_t owner, bool remove = false);
  int remove_rows(my_off_t *offsets, int cnt);
  pthread_t find_owner(my_off_t off);
  my_off_t assign_owner(pthread_t owner);
private:
  int writer_do_append(append_list_t *l);
  void writer_do_remove(remove_list_t *l);
  void *writer_start();
  static void *_writer_start(void* self) {
    static_cast<queue_share_t*>(self)->writer_start();
  }
  int compact();
  queue_share_t();
  ~queue_share_t();
  queue_share_t(const queue_share_t&);
  queue_share_t& operator=(const queue_share_t&);
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
      | HA_CAN_BIT_FIELD | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE
      | HA_FILE_BASED | HA_NO_AUTO_INCREMENT;
  }

  ulong index_flags(uint, uint, bool) const {
    return 0;
  }
  
  int open(const char *name, int mode, uint test_if_locked);
  int close();
  int rnd_init(bool scan);
  int rnd_end();
  int rnd_next(uchar *buf);
  int rnd_pos(uchar *buf, uchar *_pos);
  void position(const uchar *record);
  
  int info(uint);
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
  int prepare_rows_buffer(size_t sz);
  void free_rows_buffer();
  void unpack_row(uchar *buf);
  size_t pack_row(uchar *buf);
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
};

#endif
