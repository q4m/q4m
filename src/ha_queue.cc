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

extern "C" {
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}
#include <algorithm>
#include <functional>
#include <list>
#include <vector>

#define MYSQL_SERVER

#include "mysql_priv.h"
#undef PACKAGE
#undef VERSION
#undef HAVE_DTRACE
#undef _DTRACE_VERSION

#include "queue_config.h"

#include "ha_queue.h"
#include <mysql/plugin.h>
#include "adler32.c"

extern uint build_table_filename(char *buff, size_t bufflen, const char *db,
				 const char *table, const char *ext,
				 uint flags);


using namespace std;

#define DO_COMPACT(all, free) ((all) >= 4*1024*1024 && (free) * 2 >= (all))
#define EXPAND_BY (65536)
#define Q4M ".Q4M"
#define Q4T ".Q4T"

static HASH queue_open_tables;
#ifdef SAFE_MUTEX
static pthread_mutex_t g_mutex = {
#ifdef PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP
  PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP,
#else
  PTHREAD_MUTEX_INITIALIZER,
#endif
  PTHREAD_MUTEX_INITIALIZER,
  __FILE__,
  __LINE__
};
#else
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

static handlerton *queue_hton;

/* if non-NULL, access is restricted to the rows owned, and it points
 * to queue_share_t
 */
static pthread_key_t share_key;


static void kill_proc(const char *msg)
{
  write(2, msg, strlen(msg));
  abort();
  static char *np = NULL;
  if (*np) {
    *np = NULL;
  }
}

static void sync_file(int fd)
{
  if (
#ifdef FDATASYNC_USE_FCNTL
      fcntl(fd, F_FULLFSYNC, 0) != 0
#else
      fdatasync(fd) != 0
#endif
      ) {
    kill_proc("failed to sync disk\n");
  }
}

off_t queue_row_t::validate_checksum(int fd, off_t off)
{
  off_t len;
  
  /* read checksum size */
  off += queue_row_t::header_size();
  if (::pread(fd, &len, sizeof(len), off) != sizeof(len)) {
    return 0;
  }
  off += sizeof(len);
  /* calc checksum */
  uchar buf[4096];
  uint32_t adler = 1;
  while (len != 0) {
    size_t bs = min(len, sizeof(buf));
    if (::pread(fd, buf, bs, off) != bs) {
      return 0;
    }
    adler = adler32(adler, buf, bs);
    off += bs;
    len -= bs;
  }
  /* compare checksum */
  return size() == (adler & size_mask) ? off : 0;
}

queue_row_t *queue_row_t::create_checksum(const iovec* iov, int iovcnt)
{
  off_t sz = 0;
  uint32_t adler = 1;
  
  for (int i = 0; i < iovcnt; i++) {
    adler = adler32(adler, iov[i].iov_base, iov[i].iov_len);
    sz += iov[i].iov_len;
  }
  
  queue_row_t *row =
    static_cast<queue_row_t*>(my_malloc(checksum_size(), MYF(0)));
  assert(row != NULL);
  row->_size = type_checksum | (adler & size_mask);
  memcpy(row->_bytes, &sz, sizeof(off_t));
  
  return row;
}

queue_file_header_t::queue_file_header_t()
  : _magic(MAGIC), _attr(0), _end(sizeof(queue_file_header_t)),
    _begin(sizeof(queue_file_header_t))
{
  memset(_padding, 0, sizeof(_padding));
}

void queue_file_header_t::write(int fd)
{
  if (pwrite(fd, this, sizeof(*this), 0) != sizeof(*this)) {
    kill_proc("failed to update header\n");
  }
}

uchar* queue_share_t::get_share_key(queue_share_t *share, size_t *length,
				    my_bool not_used __attribute__((unused)))
{
  *length = share->table_name_length;
  return reinterpret_cast<uchar*>(share->table_name);
}

void queue_share_t::fixup_header()
{
  /* update end */
  off_t off = _header.end();
  while (1) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size(), true)
	!= queue_row_t::header_size()) {
      break;
    }
    if (row.type() != queue_row_t::type_checksum) {
      break;
    }
    if ((off = row.validate_checksum(fd, off)) == 0) {
      break;
    }
    _header.set_end(off);
  }
  /* update begin */
  off = _header.begin();
  while (off < _header.end()) {
    queue_row_t row;
    if (read(&row ,off, queue_row_t::header_size(), true)
	!= queue_row_t::header_size()) {
      kill_proc("I/O error\n");
    }
    if (row.type() == queue_row_t::type_row) {
      break;
    }
    off = row.next(off);
  }
  _header.set_begin(off);
  /* save */
  _header.set_attr(_header.attr() & ~queue_file_header_t::attr_is_dirty);
  _header.write(fd);
  sync_file(fd);
}

queue_share_t *queue_share_t::get_share(const char *table_name)
{
  queue_share_t *share;
  uint table_name_length;
  char *tmp_name;
  char filename[FN_REFLEN];
  
  pthread_mutex_lock(&g_mutex);
  
  table_name_length = strlen(table_name);
  
  /* return the one, if found (after incrementing refcount) */
  if ((share = reinterpret_cast<queue_share_t*>(hash_search(&queue_open_tables, reinterpret_cast<const uchar*>(table_name), table_name_length)))
      != NULL) {
    ++share->use_count;
    pthread_mutex_unlock(&g_mutex);
    return share;
  }
  
  /* alloc */
  if (my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(queue_share_t),
		      &tmp_name, table_name_length + 1, NullS)
      == NULL) {
    goto ERR_RETURN;
  }
  
  /* init members that would always succeed in doing so */
  share->use_count = 1;
  share->table_name = tmp_name;
  strmov(share->table_name, table_name);
  share->table_name_length = table_name_length;
  pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&share->append_mutex, MY_MUTEX_INIT_FAST);
  thr_lock_init(&share->store_lock);
  share->cache.off = 0;
  new (&share->rows_owned) queue_rows_owned_t();
  pthread_cond_init(&share->queue_cond, NULL);
  share->num_readers = 0;
  /* open file */
  fn_format(filename, share->table_name, "", Q4M,
	    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  if ((share->fd = open(filename, O_RDWR, 0)) == -1) {
    goto ERR_ON_FILEOPEN;
  }
  /* load header */
  if (::pread(share->fd, &share->_header, sizeof(share->_header), 0)
      != sizeof(share->_header)) {
    goto ERR_AFTER_FILEOPEN;
  }
  if (share->_header.magic() != queue_file_header_t::MAGIC) {
    goto ERR_AFTER_FILEOPEN;
  }
  /* sanity check */
  if ((share->_header.attr() & queue_file_header_t::attr_is_dirty) != 0) {
    share->fixup_header();
  }
  /* set dirty flag */
  share->_header.set_attr(share->_header.attr()
			  | queue_file_header_t::attr_is_dirty);
  share->_header.write(share->fd);
  sync_file(share->fd);
  /* seek to end position for inserts */
  if (lseek(share->fd, share->_header.end(), SEEK_SET) == -1) {
    goto ERR_AFTER_FILEOPEN;
  }
  /* add to open_tables */
  if (my_hash_insert(&queue_open_tables, reinterpret_cast<uchar*>(share))) {
    goto ERR_AFTER_FILEOPEN;
  }
  
  /* success */
  pthread_mutex_unlock(&g_mutex);
  return share;
  
 ERR_AFTER_FILEOPEN:
  close(share->fd);
 ERR_ON_FILEOPEN:
  pthread_cond_destroy(&share->queue_cond);
  share->rows_owned.~list();
  thr_lock_delete(&share->store_lock);
  pthread_mutex_destroy(&share->append_mutex);
  pthread_mutex_destroy(&share->mutex);
  my_free(reinterpret_cast<uchar*>(share), MYF(0));
 ERR_RETURN:
  pthread_mutex_unlock(&g_mutex);
  return NULL;
  }

void queue_share_t::release()
{
  pthread_mutex_lock(&g_mutex);
  
  if (--use_count == 0) {
    hash_delete(&queue_open_tables, reinterpret_cast<uchar*>(this));
    _header.write(fd);
    sync_file(fd);
    _header.set_attr(_header.attr() & ~queue_file_header_t::attr_is_dirty);
    _header.write(fd);
    sync_file(fd);
    close(fd);
    pthread_cond_destroy(&queue_cond);
    rows_owned.~list();
    thr_lock_delete(&store_lock);
    pthread_mutex_destroy(&append_mutex);
    pthread_mutex_destroy(&mutex);
    my_free(reinterpret_cast<uchar*>(this), MYF(0));
  }
  
  pthread_mutex_unlock(&g_mutex);
}

void queue_share_t::unlock_reader()
{
  lock();
  
  if (--num_readers == 0) {
    if (DO_COMPACT(_header.end() - sizeof(_header),
		   _header.begin() - sizeof(_header))) {
      compact(); // how should we handle the error?
    }
  }
  
  unlock();
}

off_t queue_share_t::reset_owner(pthread_t owner)
{
  off_t off = 0;
  lock();
  
  for (queue_rows_owned_t::iterator i = rows_owned.begin();
       i != rows_owned.end();
       ++i) {
    if (i->first == owner) {
      off = i->second;
      rows_owned.erase(i);
      break;
    }
  }
  
  unlock();
  return off;
}

const void *queue_share_t::read_cache(off_t off, ssize_t size,
				      bool populate_cache)
{
  if (size > sizeof(cache.buf)) {
    return NULL;
  }
  if (cache.off <= off && off + size <= cache.off + sizeof(cache.buf)) {
    return cache.buf + off - cache.off;
  }
  if (! populate_cache) {
    return NULL;
  }
  if (pread(fd, cache.buf, sizeof(cache.buf), off) < size) {
    cache.off = 0; // invalidate
    return NULL;
  }
  cache.off = off;
  return cache.buf;
}

ssize_t queue_share_t::read(void *data, off_t off, ssize_t size,
			    bool populate_cache)
{
  const void* cp;
  if ((cp = read_cache(off, size, populate_cache)) != NULL) {
    memcpy(data, cp, size);
    return size;
  }
  return pread(fd, data, size, off);
}

int queue_share_t::writev(const iovec* iov, int iovcnt, ssize_t len)
{
  if (::writev(fd, iov, iovcnt) != len) {
    return -1;
  }
  off_t off = _header.end();
  for (int i = 0; i < iovcnt; i++) {
    update_cache(iov[i].iov_base, off, iov[i].iov_len);
    off += iov[i].iov_len;
  }
  return 0;
}

int queue_share_t::pwrite(const void *data, off_t off, size_t size)
{
  if (::pwrite(fd, data, size, off) != size) {
    return -1;
  }
  update_cache(data, off, size);
  return 0;
}

int queue_share_t::next(off_t *off)
{
  if (*off == _header.end()) {
    // eof
  } else {
    queue_row_t row;
    if (read(&row, *off, queue_row_t::header_size(), true)
	!= queue_row_t::header_size()) {
      return -1;
    }
    *off = row.next(*off);
    while (1) {
      if (*off == _header.end()) {
	break;
      }
      if (read(&row, *off, queue_row_t::header_size(), true)
	  != queue_row_t::header_size()) {
	return -1;
      }
      if (row.type() == queue_row_t::type_row) {
	break;
      }
      *off = row.next(*off);
    }
  }
  
  return 0;
}

off_t queue_share_t::get_owned_row(pthread_t owner, bool remove)
{
  for (queue_rows_owned_t::iterator i = rows_owned.begin();
       i != rows_owned.end();
       ++i) {
    if (i->first == owner) {
      if (remove) {
	rows_owned.erase(i);
      }
      return i->second;
    }
  }
  return 0;
}

int queue_share_t::write_rows(queue_row_t **rows, int cnt)
{
  iovec *iov = new iovec [cnt + 1];
  off_t wlen = 0;
  
  /* fill in iovec */
  for (size_t i = 0; i < cnt; i++) {
    iov[i + 1].iov_base = rows[i];
    wlen += iov[i + 1].iov_len = rows[i]->next(0);
  }
  /* set checksum */
  iov[0].iov_base = queue_row_t::create_checksum(iov + 1, cnt);
  wlen += iov[0].iov_len = static_cast<queue_row_t*>(iov[0].iov_base)->next(0);
  
  /* writer lock */
  pthread_mutex_lock(&append_mutex);
  
  /* lock */
  lock();
  /* extend the file by certain amount for speed */
  if ((_header.end() - 1) / EXPAND_BY != (_header.end() + wlen) / EXPAND_BY) {
    if (lseek(fd, ((_header.end() + wlen) / EXPAND_BY + 1) * EXPAND_BY - 1,
	      SEEK_SET)
	== -1
	|| ::write(fd, "", 1) != 1
	|| lseek(fd, _header.end(), SEEK_SET) == -1) {
      unlock();
      pthread_mutex_unlock(&append_mutex);
      my_free(iov[0].iov_base, MYF(0));
      delete [] iov;
      return -1;
    }
  }
  /* write */
  assert(wlen < INT_MAX); // TODO: fix limitation for 32-bit systems
  if (writev(iov, cnt + 1, wlen) != 0) {
    kill_proc("failed to write data to allocate file area\n");
  }
  /* unlock */
  unlock();
  /* sync data */
  switch (mode) {
  case e_sync:
    sync_file(fd);
    break;
  }
  /* update end */
  lock();
  if (_header.begin() == _header.end()) {
    _header.set_begin(_header.begin() + queue_row_t::checksum_size());
  }
  _header.set_end(_header.end() + wlen);
  unlock();
  
  /* writer unlock */
  pthread_mutex_unlock(&append_mutex);
  
  my_free(iov[0].iov_base, MYF(0));
  delete [] iov;
  return 0;
}

int queue_share_t::erase_row(off_t off, bool already_locked)
{
  queue_row_t row;
  
  if (! already_locked) {
    lock();
  }
  if (read(&row, off, queue_row_t::header_size(), false)
      != queue_row_t::header_size()) {
    unlock();
    return -1;
  }
  row.set_type(queue_row_t::type_removed);
  if (pwrite(&row, off, queue_row_t::header_size()) != 0) {
    unlock();
    return -1;
  }
  unlock();
  switch (mode) {
  case e_sync:
    sync_file(fd);
    break;
  }
  lock();
  if (off == _header.begin()) {
    if (next(&off) != 0) {
      unlock();
      return -1;
    }
    _header.set_begin(off);
  }
  unlock();
  
  return 0;
}

pthread_t queue_share_t::find_owner(off_t off)
{
  for (queue_rows_owned_t::const_iterator j = rows_owned.begin();
       j != rows_owned.end();
       ++j) {
    if (off == j->second) {
      return j->first;
    }
  }
  return 0;
}

off_t queue_share_t::assign_owner(pthread_t owner)
{
  off_t off = _header.begin();
  while (off != _header.end()) {
    if (find_owner(off) == 0) {
      rows_owned.push_back(queue_rows_owned_t::value_type(owner, off));
      return off;
    }
    if (next(&off) != 0) {
      return 0;
    }
  }
  return 0;
}

static int copy_file_content(int src_fd, off_t begin, off_t end, int dest_fd)
{
  char buf[65536];
  
  while (begin < end) {
    size_t bs = min(end - begin, sizeof(buf));
    if (pread(src_fd, buf, bs, begin) != bs
	|| write(dest_fd, buf, bs) != bs) {
      return -1;
    }
    begin += bs;
  }
  
  return 0;
}

int queue_share_t::compact()
{
  char filename[FN_REFLEN], tmp_filename[FN_REFLEN];
  int tmp_fd;
  off_t delta = _header.begin() - sizeof(queue_file_header_t);
  
  /* open new file */
  fn_format(filename, table_name, "", Q4M,
	    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  fn_format(tmp_filename, table_name, "", Q4T,
	    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  if ((tmp_fd = open(tmp_filename, O_CREAT | O_TRUNC | O_RDWR, 0660))
      == -1) {
    goto ERR_RETURN;
  }
  { /* write data (and seek to end) */
    queue_file_header_t hdr;
    switch (mode) {
    case e_volatile:
      break; // end points to top
    default:
      hdr.set_end(_header.end() - delta);
      break;
    }
    if (::write(tmp_fd, &hdr, sizeof(hdr)) != sizeof(hdr)
	|| copy_file_content(fd, _header.begin(), _header.end(), tmp_fd) != 0) {
      goto ERR_OPEN;
    }
    sync_file(tmp_fd);
  }
  /* rename */
  if (rename(tmp_filename, filename) != 0) {
    goto ERR_OPEN;
  }
  // is the directory entry synced with fsync?
  if (fsync(tmp_fd) != 0) {
    kill_proc("failed to sync disk\n");
  }
  /* replace fd */
  close(fd);
  fd = tmp_fd;
  { /* adjust offsets */
    _header.set_begin(_header.begin() - delta);
    _header.set_end(_header.end() - delta);
    for (queue_rows_owned_t::iterator i = rows_owned.begin();
	 i != rows_owned.end();
	 ++i) {
      i->second -= delta;
    }
    cache.off = 0; /* invalidate, since it may go below sizeof(_header) */
  }
  
  return 0;
    
 ERR_OPEN:
  close(tmp_fd);
  unlink(tmp_filename);
 ERR_RETURN:
  unlock();
  return -1;
}

static queue_share_t* get_share_check(const char* db_table_name)
{
  char buf[FN_REFLEN];
  char path[FN_REFLEN];

  // copy to buf, split to db name and table name, and build filename
  // FIXME: creates bogus name if db_table_name is too long (but no overruns)
  strncpy(buf, db_table_name, FN_REFLEN - 1);
  buf[FN_REFLEN - 1] = '\0';
  char* tbl = strchr(buf, '.');
  if (tbl == NULL)
    return NULL;
  *tbl++ = '\0';
  if (*tbl == '\0')
    return NULL;
  build_table_filename(path, FN_REFLEN - 1, buf, tbl, "", 0);
  
  return queue_share_t::get_share(path);
}

static void erase_owned()
{
  queue_share_t *share;
  
  if ((share = static_cast<queue_share_t*>(pthread_getspecific(share_key)))
      != NULL) {
    share->lock();
    off_t off = share->get_owned_row(pthread_self(), true);
    share->erase_row(off, true);
    // unlock in erase_row
    share->release();
    pthread_setspecific(share_key, NULL);
  }
}

static int close_connection_handler(handlerton *hton, THD *thd)
{
  queue_share_t *share;
  
  if ((share = static_cast<queue_share_t*>(pthread_getspecific(share_key)))
      != NULL) {
    if (share->reset_owner(pthread_self()) != 0) {
      share->wake_listener();
    }
    share->release();
    pthread_setspecific(share_key, NULL);
  }
  
  return 0;
}

handler *create_handler(handlerton *hton, TABLE_SHARE *table,
			MEM_ROOT *mem_root)
{
  return new (mem_root) ha_queue(hton, table);
}

static int init(void *p)
{
  
  queue_hton = (handlerton *)p;
  
  pthread_key_create(&share_key, NULL);
  hash_init(&queue_open_tables, system_charset_info, 32, 0, 0,
	    reinterpret_cast<hash_get_key>(queue_share_t::get_share_key), 0, 0);
  
  queue_hton->state = SHOW_OPTION_YES;
  queue_hton->close_connection = close_connection_handler;
  queue_hton->create = create_handler;
  queue_hton->flags = HTON_CAN_RECREATE;
  
  return 0;
}

static int deinit(void *p)
{
  hash_free(&queue_open_tables);
  queue_hton = NULL;
  
  return 0;
}

ha_queue::ha_queue(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share(NULL),
   pos(),
   row(NULL),
   row_max_size(0),
   bulk_insert_rows(NULL)
{
}

ha_queue::~ha_queue()
{
  if (bulk_insert_rows != NULL) {
    free_bulk_insert_rows();
  }
  if (row != NULL) {
    my_free(reinterpret_cast<uchar*>(row), MYF(0));
  }
}

static const char *ha_queue_exts[] = {
  Q4M,
  NullS
};

const char **ha_queue::bas_ext() const
{
  return ha_queue_exts;
}

int ha_queue::open(const char *name, int mode, uint test_if_locked)
{
  if ((share = queue_share_t::get_share(name)) == NULL) {
    return 1;
  }
  thr_lock_data_init(share->get_store_lock(), &lock, NULL);
  if (prepare_row_buffer(table->s->reclength) != 0) {
    return 1;
  }
  
  return 0;
}

int ha_queue::close()
{
  share->release();
  
  return 0;
}

int ha_queue::rnd_init(bool scan)
{
  share->lock_reader();
  pos = 0;
  return 0;
}

int ha_queue::rnd_end()
{
  share->unlock_reader();
  return 0;
}

int ha_queue::rnd_next(uchar *buf)
{
  int err = HA_ERR_END_OF_FILE;
  share->lock();

  if (pthread_getspecific(share_key)) {
    if (pos == 0 && (pos = share->get_owned_row(pthread_self())) != 0) {
      // ok
    } else {
      goto EXIT;
    }
  } else {
    if (pos == 0) {
      if ((pos = share->header()->begin()) == share->header()->end()) {
	goto EXIT;
      }
    } else {
      if (share->next(&pos) != 0) {
	err = HA_ERR_CRASHED_ON_USAGE;
	goto EXIT;
      } else if (pos == share->header()->end()) {
	goto EXIT;
      }
    }
    while (share->find_owner(pos) != 0) {
      if (share->next(&pos) != 0) {
	err = HA_ERR_CRASHED_ON_USAGE;
	goto EXIT;
      }
      if (pos == share->header()->end()) {
	goto EXIT;
      }
    }
  }
  
  /* read data to row buffer */
  if (share->read(row, pos, queue_row_t::header_size(), true)
      != queue_row_t::header_size()) {
    err = HA_ERR_CRASHED_ON_USAGE;
    goto EXIT;
  }
  if (prepare_row_buffer(row->size()) != 0) {
    err = HA_ERR_OUT_OF_MEM;
    goto EXIT;
  }
  if (share->read(row->bytes(), pos + queue_row_t::header_size(), row->size(),
		  false)
      != row->size()) {
    err = HA_ERR_CRASHED_ON_USAGE;
    goto EXIT;
  }
  
  /* unlock and convert to internal representation */
  share->unlock();
  unpack_row(buf);
  return 0;
  
 EXIT:
  share->unlock();
  return err;
}

void ha_queue::position(const uchar *record)
{
  my_store_ptr(ref, sizeof(pos), pos);
}

int ha_queue::rnd_pos(uchar *buf, uchar *_pos)
{
  pos = static_cast<off_t>(my_get_ptr(_pos, sizeof(pos)));
  
  /* we should return the row even if it had the deleted flag set during the
   * execution by other threads
   */
  if (share->read(row, pos, queue_row_t::header_size(), true)
      != queue_row_t::header_size()) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  if (share->read(row->bytes(), pos + queue_row_t::header_size(), row->size(),
		  false)
      != row->size()) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  
  unpack_row(buf);
  return 0;
}

int ha_queue::info(uint flag)
{
  // records = share->rows.size();
  // deleted = 0;
  
  return 0;
}

THR_LOCK_DATA **ha_queue::store_lock(THD *thd,
				     THR_LOCK_DATA **to,
				     enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) {
    lock.type=lock_type;
  }
  
  *to++= &lock;
  return to;
}

int ha_queue::create(const char *name, TABLE *table_arg,
		     HA_CREATE_INFO *create_info)
{
  char filename[FN_REFLEN];
  int fd;
  
  fn_format(filename, name, "", Q4M, MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  if ((fd = ::open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0660))
      == -1) {
    return HA_ERR_GENERIC; // ????
  }
  queue_file_header_t header;
  if (write(fd, &header, sizeof(header)) != sizeof(header)) {
    goto ERROR;
  }
  if (lseek(fd, EXPAND_BY - 1, SEEK_SET) == -1
      || write(fd, "", 1) != 1) {
    goto ERROR;
  }
  sync_file(fd);
  ::close(fd);
  return 0;
  
 ERROR:
  ::close(fd);
  unlink(filename);
  return HA_ERR_RECORD_FILE_FULL;
}

void ha_queue::start_bulk_insert(ha_rows rows __attribute__((unused)))
{
  assert(bulk_insert_rows == NULL);
  bulk_insert_rows = new vector<queue_row_t*>();
}

int ha_queue::end_bulk_insert()
{
  int ret = 0;
  
  if (bulk_insert_rows != NULL) {
    if (bulk_insert_rows->size() != 0) {
      if (share->write_rows(&bulk_insert_rows->front(),
			    bulk_insert_rows->size())
	  != 0) {
	ret = HA_ERR_RECORD_FILE_FULL;
      }
      if (ret == 0) {
	for (size_t i = 0; i < bulk_insert_rows->size(); i++) {
	  share->wake_listener();
	}
      }
      free_bulk_insert_rows();
    }
  }
  
  return ret;
}

int ha_queue::write_row(uchar *buf)
{
  if (pack_row(buf) != 0) {
    return HA_ERR_OUT_OF_MEM;
  }
  
  if (bulk_insert_rows != NULL) {
    bulk_insert_rows->push_back(row);
    row = NULL;
    if (prepare_row_buffer(table->s->reclength) != 0) {
      return HA_ERR_OUT_OF_MEM;
    }
    return 0;
  }

  if (share->write_rows(&row, 1) != 0) {
    return HA_ERR_RECORD_FILE_FULL;
  }
  share->wake_listener();
  return 0;
}

int ha_queue::update_row(const uchar *old_data __attribute__((unused)),
			 uchar *new_data)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_queue::delete_row(const uchar *buf __attribute__((unused)))
{
  share->lock();
  
  pthread_t owner = share->find_owner(pos);
  if (owner != 0 && owner != pthread_self()) {
    share->unlock();
    return HA_ERR_RECORD_DELETED;
  }
  share->erase_row(pos, true);
  // unlock in erase_row
  
  return 0;
}

void ha_queue::unpack_row(uchar *buf)
{
  const uchar *src = row->bytes();
			  
  memcpy(buf, row->bytes(), table->s->null_bytes);
  src += table->s->null_bytes;
  for (Field **field = table->field; *field != NULL; field++) {
    if (! (*field)->is_null()) {
      src = (*field)->unpack(buf + (*field)->offset(table->record[0]), src);
    }
  }
}

int ha_queue::pack_row(uchar *buf)
{
  size_t maxsize = table->s->reclength + table->s->fields * 2;
  for (uint *ptr = table->s->blob_field, *end = ptr + table->s->blob_fields;
       ptr != end;
       ++ptr) {
    maxsize += 2 + ((Field_blob*)table->field[*ptr])->get_length();
  }
  
  if (prepare_row_buffer(maxsize) != 0) {
    return -1;
  }
  uchar *dst = row->bytes();
  
  memcpy(dst, buf, table->s->null_bytes);
  dst += table->s->null_bytes;
  for (Field **field = table->field; *field != NULL; field++) {
    if (! (*field)->is_null()) {
      dst = (*field)->pack(dst, buf + (*field)->offset(buf));
    }
  }
  size_t size = dst - row->bytes();
  if (size > queue_row_t::max_size) {
    return -1;
  }
  new (row) queue_row_t(dst - row->bytes());
  
  return 0;
}

void ha_queue::free_bulk_insert_rows()
{
  for (vector<queue_row_t*>::iterator i = bulk_insert_rows->begin();
       i != bulk_insert_rows->end();
       ++i) {
    my_free(*i, MYF(0));
  }
  delete bulk_insert_rows;
  bulk_insert_rows = NULL;
}

struct st_mysql_storage_engine queue_storage_engine = {
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

mysql_declare_plugin(queue)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &queue_storage_engine,
  "QUEUE",
  "Kazuho Oku at Cybozu Labs, Inc.",
  "Queue storage engine for MySQL",
  PLUGIN_LICENSE_GPL,
  init,
  deinit,
  0x0001,
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL                        /* config options                  */
}
mysql_declare_plugin_end;

struct queue_wait_t {
  queue_share_t *share;
  time_t return_at;
  queue_wait_t(queue_share_t *s, time_t r)
    : share(s), return_at(r)
  {}
};

my_bool queue_wait_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  queue_share_t *share;
  time_t return_at;
  
  if (args->arg_count < 1) {
    strcpy(message, "queue_wait(): missing table name");
    return 1;
  }
  if (args->arg_type[0] != STRING_RESULT
      || (share = get_share_check(args->args[0])) == NULL) {
    strcpy(message, "queue_wait(): table not found");
    return 1;
  }
  if (args->arg_count >= 2) {
    if (args->arg_type[1] != INT_RESULT) {
      share->release();
      strcpy(message, "queue_wait(): timeout not an integer");
      return 1;
    }
    return_at = time(NULL) + *reinterpret_cast<long long*>(args->args[1]);
  } else {
    return_at = time(NULL) + 60;
  }
  
  initid->maybe_null = 0;
  initid->ptr = reinterpret_cast<char*>(new queue_wait_t(share, return_at));
  
  return 0;
}

void queue_wait_deinit(UDF_INIT *initid __attribute__((unused)))
{
  queue_wait_t *info = reinterpret_cast<queue_wait_t*>(initid->ptr);
  
  delete info;
}

long long queue_wait(UDF_INIT *initid, UDF_ARGS *args __attribute__((unused)),
		     char *is_null, char *error)
{
  /* set ha_data so that close_conn gets called,
   * should correspond to the implementation of handle::ha_data
   */
  current_thd->ha_data[queue_hton->slot] = reinterpret_cast<void*>(1);
  
  erase_owned();
  
  queue_wait_t *info = reinterpret_cast<queue_wait_t*>(initid->ptr);
  int ret = 0;
  
  info->share->lock();
  do {
    if (info->share->assign_owner(pthread_self()) != 0) {
      ret = 1;
      break;
    }
  } while (info->share->wait(info->return_at) == 0);
  pthread_setspecific(share_key, info->share);
  info->share->unlock();
  
  *is_null = 0;
  return ret;
}

my_bool queue_end_init(UDF_INIT *initid,
		       UDF_ARGS *args __attribute__((unused)),
		       char *message __attribute__((unused)))
{
  initid->maybe_null = 0;
  return 0;
}

void queue_end_deinit(UDF_INIT *initid __attribute__((unused)))
{
}

long long queue_end(UDF_INIT *initid __attribute__((unused)),
		    UDF_ARGS *args __attribute__((unused)),
		    char *is_null, char *error __attribute__((unused)))
{
  erase_owned();
  
  *is_null = 0;
  return 1;
}

my_bool queue_abort_init(UDF_INIT *initid,
			UDF_ARGS *args __attribute__((unused)),
			char *message)
{
  initid->maybe_null = 0;
  if ((initid->ptr = reinterpret_cast<char*>(pthread_getspecific(share_key)))
      == NULL) {
    strcpy(message, "queue_abort(): not in queue access mode");
    return 1;
  }
  return 0;
}

void queue_abort_deinit(UDF_INIT *initid)
{
  queue_share_t *share = reinterpret_cast<queue_share_t*>(initid->ptr);
  share->release();
  pthread_setspecific(share_key, NULL);
}

long long queue_abort(UDF_INIT *initid, UDF_ARGS *args __attribute__((unused)),
		      char *is_null, char *error)
{
  queue_share_t *share = reinterpret_cast<queue_share_t*>(initid->ptr);
  
  if (share->reset_owner(pthread_self()) != 0) {
    share->wake_listener();
  }
  
  *is_null = 0;
  return 1;
}
