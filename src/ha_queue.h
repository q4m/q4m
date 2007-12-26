/*
 * Copyright (C) 2007 Cybozu Labs, Inc.
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
  unsigned _size; /* upper 2 bits used for flags, removed, and reserved */
  uchar _bytes[1];
public:
  queue_row_t() : _size(0) {}
  queue_row_t(unsigned size, bool removed = false) {
    assert(size <= 0x3fffffff);
    _size = (removed ? 0x80000000 : 0) | size;
  }
  unsigned size() const {
    return _size & 0x3fffffff;
  }
  bool is_removed() const {
    return (_size & 0x80000000) != 0;
  }
  void set_is_removed() {
    _size |= 0x80000000;
  }
  uchar *bytes() { return _bytes; }
  static size_t header_size() {
    return offsetof(queue_row_t, _bytes[0]);
  }
private:
  queue_row_t(const queue_row_t&);
  queue_row_t& operator=(const queue_row_t&);
};

class queue_file_header_t {
public:
  static const unsigned MAGIC = 0x6d393031;
private:
  unsigned _magic;
  unsigned _padding1;
  off_t    _eod;
  unsigned _padding2[(4096 - sizeof(unsigned) * 2 - sizeof(off_t)) / sizeof(unsigned)];
public:
  queue_file_header_t();
  unsigned magic() const { return _magic; }
  off_t eod() const { return _eod; }
  void set_eod(off_t e) { _eod = e; }
  int write(int fd);
  int restore(int fd);
};

typedef std::list<std::pair<pthread_t, off_t> > queue_rows_owned_t;

class queue_share_t {
  uint use_count;
  char *table_name;
  uint table_name_length;
  
  pthread_mutex_t mutex;
  THR_LOCK store_lock;
  
  enum {
    e_sync,
    e_volatile,
  } mode;
  
  int fd;
  off_t first_row;
  queue_file_header_t _header;
  
  struct {
    off_t off;
    char buf[1024]; // should be smaller than queue_file_header_t for using off==0 for invalidation
  } cache;
  
  queue_rows_owned_t rows_owned;
  
  pthread_cond_t queue_cond;
  int num_readers;
  
public:
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
  /* functions below requires lock */
  const void *read_cache(off_t off, ssize_t size, bool populate_cache);
  ssize_t read(void *data, off_t off, ssize_t size, bool populate_cache);
  int write_file(const void *data, off_t off, size_t size);
  off_t begin() { return first_row; }
  off_t end() { return header()->eod(); }
  int next(off_t *off);
  off_t get_owned_row(pthread_t owner, bool remove = false);
  int write_row(queue_row_t *row, bool sync);
  int erase_row(off_t off, bool sync);
  int sync();
  pthread_t find_owner(off_t off);
  off_t assign_owner(pthread_t owner);
private:
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
  
  off_t pos;
  queue_row_t *row;
  bool is_bulk_insert, is_dirty;
  
 public:
  ha_queue(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_queue() {}
  
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
  int rnd_pos(uchar *buf, uchar *pos);
  void position(const uchar *record);
  
  int info(uint);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required
  
  void start_bulk_insert(ha_rows rows);
  int end_bulk_insert();
  
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
 private:
  void unpack_row(uchar *buf);
  void pack_row(uchar *buf);
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
