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

// forget about endianness, for now :-)

class queue_row_t {
  /* size is stored in the lower 30 bits, while upper 2 bits are used for
   * attributes.  if type == type_checksum, lower 30 bits adler32 is stored
   * in _size, and size of the checksum is stored in the body in type off_t.
   */
  unsigned _size;
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
    _size = size | type_row;
  }
  unsigned size() const {
    // NOTE: does not check if the row isn't checksum
    return _size & size_mask;
  }
  unsigned checksum() const {
    return size();
  }
  unsigned type() const {
    return _size & type_mask;
  }
  void set_type(unsigned type) {
    assert((type & size_mask) == 0);
    _size = (_size & size_mask) | type;
  }
  uchar *bytes() { return _bytes; }
  static size_t header_size() {
    return offsetof(queue_row_t, _bytes[0]);
  }
  static size_t checksum_size() {
    return header_size() + sizeof(off_t);
  }
  off_t next(off_t off) {
    return off + header_size()
      + (type() != type_checksum ? size() : sizeof(off_t));
  }
  off_t validate_checksum(int fd, off_t off);
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
  unsigned _magic;
  unsigned _attr;
  off_t    _end;
  off_t    _begin;
  unsigned _padding[(1024 - sizeof(unsigned) * 2 - sizeof(off_t) * 2) / sizeof(unsigned)];
public:
  queue_file_header_t();
  unsigned magic() const { return _magic; }
  unsigned attr() const { return _attr; }
  void set_attr(unsigned a) { _attr = a; }
  off_t end() const { return _end; }
  void set_end(off_t e) { _end = e; }
  off_t begin() const { return _begin; }
  void set_begin(off_t b) { _begin = b; }
  void write(int fd);
};

typedef std::list<std::pair<pthread_t, off_t> > queue_rows_owned_t;

class queue_share_t {
  
 public:
  struct append_t {
    queue_row_t **rows;
    int cnt;
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
    pthread_cond_t *cond;
    append_t(queue_row_t **r, int c, pthread_cond_t *co)
    : rows(r), cnt(c), err(-1), cond(co) {
    }
  private:
    append_t(const append_t&);
    append_t& operator=(const append_t&);
  };
  typedef std::vector<append_t*> append_list_t;
  
  struct remove_t {
    off_t *offsets;
    int cnt;
    int err; /* -1 if not completed, otherwise HA_ERR_XXX or 0 */
    pthread_cond_t *cond;
    remove_t(off_t *o, int c, pthread_cond_t *co)
    : offsets(o), cnt(c), err(-1), cond(co) {
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
    off_t off;
    char buf[4096];
  } cache;
  
  queue_rows_owned_t rows_owned;
  
  pthread_cond_t queue_cond;
  int num_readers;
  
  pthread_t writer_thread;
  pthread_cond_t writer_cond;
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
  off_t reset_owner(pthread_t owner);
  int write_rows(queue_row_t **row, int cnt, pthread_cond_t *cond);
  /* functions below requires lock */
  const void *read_cache(off_t off, ssize_t size, bool populate_cache);
  ssize_t read(void *data, off_t off, ssize_t size, bool populate_cache);
  void update_cache(const void *data, off_t off, size_t size) {
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
  int next(off_t *off);
  off_t get_owned_row(pthread_t owner, bool remove = false);
  int remove_rows(off_t *offsets, int cnt, pthread_cond_t *cond);
  pthread_t find_owner(off_t off);
  off_t assign_owner(pthread_t owner);
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
  pthread_cond_t cond;
  
  off_t pos;
  queue_row_t *row;
  size_t row_max_size; /* not including header */
  std::vector<queue_row_t*> *bulk_insert_rows;
  std::vector<off_t> *bulk_delete_rows;
  
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
    return 0;
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
  int prepare_row_buffer(size_t sz) {
    void *pt;
    if (row == NULL) {
      if ((pt = my_malloc(queue_row_t::header_size() + sz, MYF(0)))
	  == NULL) {
	return -1;
      }
    } else if (row_max_size < sz) {
      if ((pt = my_realloc(row, queue_row_t::header_size() + sz, MYF(0)))
	  == NULL) {
	return -1;
      }
    } else {
      return 0;
    }
    row = static_cast<queue_row_t*>(pt);
    row_max_size = sz;
    return 0;
  }
  void unpack_row(uchar *buf);
  int pack_row(uchar *buf);
  void free_bulk_insert_rows();
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
