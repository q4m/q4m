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
#include <mysql/plugin.h>

#include "queue_config.h"

#if SIZEOF_OFF_T != 8
#  error "support for 64-bit file offsets is mandatory"
#endif
#ifdef HAVE_LSEEK64
#  define lseek  lseek64
#  define pread  pread64
#  define pwrite pwrite64
#endif

#include "ha_queue.h"
#include "adler32.c"

extern uint build_table_filename(char *buff, size_t bufflen, const char *db,
				 const char *table, const char *ext,
				 uint flags);


using namespace std;

#define MIN_ROWS_BUFFER_SIZE (4096)
#define COMPACT_THRESHOLD (16777216)
#define DO_COMPACT(all, free) \
    ((all) >= COMPACT_THRESHOLD && (free) * 2 >= (all))
#define EXPAND_BY (1048576)
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
#elif defined(FDATASYNC_USE_FSYNC)
      fsync(fd) != 0
#elif defined(FDATASYNC_SKIP)
      0
#else
      fdatasync(fd) != 0
#endif
      ) {
    kill_proc("failed to sync disk\n");
  }
}

my_off_t queue_row_t::validate_checksum(int fd, my_off_t off)
{
  my_off_t len;
  char _len[sizeof(len)];
  
  /* read checksum size */
  off += queue_row_t::header_size();
  if (::pread(fd, _len, sizeof(len), off) != sizeof(len)) {
    return 0;
  }
  len = uint8korr(_len);
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
  my_off_t sz = 0;
  uint32_t adler = 1;
  
  for (int i = 0; i < iovcnt; i++) {
    adler = adler32(adler, iov[i].iov_base, iov[i].iov_len);
    sz += iov[i].iov_len;
  }
  
  queue_row_t *row =
    static_cast<queue_row_t*>(my_malloc(checksum_size(), MYF(0)));
  assert(row != NULL);
  int4store(row->_size, type_checksum | (adler & size_mask));
  int8store(row->_bytes, sz);
  
  return row;
}

queue_file_header_t::queue_file_header_t()
{
  int4store(_magic, MAGIC);
  int4store(_attr, 0);
  int8store(_end, static_cast<my_off_t>(sizeof(queue_file_header_t)));
  int8store(_begin, static_cast<my_off_t>(sizeof(queue_file_header_t)));
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
  my_off_t off = _header.end();
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
    if (read(&row, off, queue_row_t::header_size(), true)
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
  thr_lock_init(&share->store_lock);
  share->cache.off = 0;
  new (&share->rows_owned) queue_rows_owned_t();
  pthread_cond_init(&share->queue_cond, NULL);
  share->num_readers = 0;
  pthread_cond_init(&share->writer_cond, NULL);
  share->writer_exit = false;
  share->append_list = new append_list_t();
  share->remove_list = new remove_list_t();
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
  /* start writer thread */
  if (pthread_create(&share->writer_thread, NULL, _writer_start, share) != 0) {
    goto ERR_AFTER_FILEOPEN;
  }
  /* add to open_tables */
  if (my_hash_insert(&queue_open_tables, reinterpret_cast<uchar*>(share))) {
    goto ERR_AFTER_WRITER_START;
  }
  
  /* success */
  pthread_mutex_unlock(&g_mutex);
  return share;
  
 ERR_AFTER_WRITER_START:
  share->writer_exit = true;
  pthread_cond_signal(&share->writer_cond);
  pthread_join(share->writer_thread, NULL);
 ERR_AFTER_FILEOPEN:
  close(share->fd);
 ERR_ON_FILEOPEN:
  delete share->remove_list;
  delete share->append_list;
  pthread_cond_destroy(&share->writer_cond);
  pthread_cond_destroy(&share->queue_cond);
  share->rows_owned.~list();
  thr_lock_delete(&share->store_lock);
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
    writer_exit = true;
    pthread_cond_signal(&writer_cond);
    if (pthread_join(writer_thread, NULL) != 0) {
      kill_proc("failed to join writer thread\n");
    }
    _header.write(fd);
    sync_file(fd);
    _header.set_attr(_header.attr() & ~queue_file_header_t::attr_is_dirty);
    _header.write(fd);
    sync_file(fd);
    close(fd);
    delete remove_list;
    delete append_list;
    pthread_cond_destroy(&writer_cond);
    pthread_cond_destroy(&queue_cond);
    rows_owned.~list();
    thr_lock_delete(&store_lock);
    pthread_mutex_destroy(&mutex);
    my_free(reinterpret_cast<uchar*>(this), MYF(0));
  }
  
  pthread_mutex_unlock(&g_mutex);
}

void queue_share_t::unlock_reader()
{
  lock();
  if (--num_readers == 0
      && DO_COMPACT(_header.end() - sizeof(_header),
		    _header.begin() - sizeof(_header))) {
    pthread_cond_signal(&writer_cond);
  }
  unlock();
}

my_off_t queue_share_t::reset_owner(pthread_t owner)
{
  my_off_t off = 0;
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

int queue_share_t::write_rows(const void *rows, size_t rows_size,
			      pthread_cond_t *cond)
{
  append_t a(rows, rows_size, cond);
  
  pthread_mutex_lock(&mutex);
  append_list->push_back(&a);
  pthread_cond_signal(&writer_cond);
  do {
    pthread_cond_wait(cond, &mutex);
  } while (a.err == -1);
  pthread_mutex_unlock(&mutex);
  
  return a.err;
}

const void *queue_share_t::read_cache(my_off_t off, ssize_t size,
				      bool populate_cache)
{
  if (size > sizeof(cache.buf)) {
    return NULL;
  }
  if (cache.off != 0
      && cache.off <= off && off + size <= cache.off + sizeof(cache.buf)) {
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

ssize_t queue_share_t::read(void *data, my_off_t off, ssize_t size,
			    bool populate_cache)
{
  const void* cp;
  if ((cp = read_cache(off, size, populate_cache)) != NULL) {
    memcpy(data, cp, size);
    return size;
  }
  return pread(fd, data, size, off);
}

int queue_share_t::next(my_off_t *off)
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

my_off_t queue_share_t::get_owned_row(pthread_t owner, bool remove)
{
  for (queue_rows_owned_t::iterator i = rows_owned.begin();
       i != rows_owned.end();
       ++i) {
    if (i->first == owner) {
      my_off_t off = i->second;
      if (remove) {
	rows_owned.erase(i);
      }
      return off;
    }
  }
  return 0;
}

int queue_share_t::remove_rows(my_off_t *offsets, int cnt, pthread_cond_t *cond)
{
  remove_t r(offsets, cnt, cond);
  
  remove_list->push_back(&r);
  pthread_cond_signal(&writer_cond);
  do {
    pthread_cond_wait(cond, &mutex);
  } while (r.err == -1);
  
  return r.err;
}

pthread_t queue_share_t::find_owner(my_off_t off)
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

my_off_t queue_share_t::assign_owner(pthread_t owner)
{
  my_off_t off = _header.begin();
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

static void close_append_list(queue_share_t::append_list_t *l, int err)
{
  for (queue_share_t::append_list_t::iterator i= l->begin();
       i != l->end();
       ++i) {
    (*i)->err = err;
    pthread_cond_signal((*i)->cond);
  }
  delete l;
}

int queue_share_t::writer_do_append(append_list_t *l)
{
  /* build iovec */
  vector<iovec> iov;
  my_off_t total_len = 0;
  iov.push_back(iovec());
  for (append_list_t::iterator i = l->begin(); i != l->end(); ++i) {
    iov.push_back(iovec());
    iov.back().iov_base = const_cast<void*>((*i)->rows);
    total_len += iov.back().iov_len = (*i)->rows_size;
  }
  iov[0].iov_base =
    queue_row_t::create_checksum(&iov.front() + 1, iov.size() - 1);
  total_len += iov[0].iov_len =
    static_cast<queue_row_t*>(iov[0].iov_base)->next(0);
  /* expand if necessary */
  if ((_header.end() - 1) / EXPAND_BY
      != (_header.end() + total_len) / EXPAND_BY) {
    my_off_t new_len =
      ((_header.end() + total_len) / EXPAND_BY + 1) * EXPAND_BY;
    if (lseek(fd, new_len - 1, SEEK_SET) == -1
	|| write(fd, "", 1) != 1
	|| lseek(fd, _header.end(), SEEK_SET) == -1) {
      /* expansion failed */
      return HA_ERR_RECORD_FILE_FULL;
    }
  }
  { /* write and sync */
    vector<iovec>::const_iterator writev_from = iov.begin();
    my_off_t writev_len = writev_from->iov_len;
    for (vector<iovec>::const_iterator i = iov.begin() + 1;
	 i != iov.end();
	 ++i) {
      if (i - writev_from >= IOV_MAX
	  || writev_len + i->iov_len > SSIZE_MAX / 2) {
	if (writev(fd, &*writev_from, i - writev_from) != writev_len) {
	  my_free(iov[0].iov_base, MYF(0));
	  return HA_ERR_CRASHED_ON_USAGE;
	}
	writev_from = i;
	writev_len = 0;
      }
      writev_len += i->iov_len;
    }
    if (writev(fd, &*writev_from, iov.end() - writev_from) != writev_len) {
      my_free(iov[0].iov_base, MYF(0));
      return HA_ERR_CRASHED_ON_USAGE;
    }
    sync_file(fd);
  }
  /* update begin, end, cache */
  pthread_mutex_lock(&mutex);
  if (_header.begin() == _header.end()) {
    _header.set_begin(_header.begin() + iov[0].iov_len);
  }
  for (vector<iovec>::const_iterator i = iov.begin(); i != iov.end(); ++i) {
    update_cache(i->iov_base, _header.end(), i->iov_len);
    _header.set_end(_header.end() + i->iov_len);
  }
  pthread_mutex_unlock(&mutex);
  
  my_free(iov[0].iov_base, MYF(0));
  return 0;
}

void queue_share_t::writer_do_remove(remove_list_t* l)
{
  remove_list_t::iterator i;
  int err = 0;
  
  for (remove_list_t::iterator i = l->begin(); i != l->end(); ++i) {
    /* lock mutex for each bulk delete, so as to make the deletion atomic to the
     * deleter (would not be atomic for non-owner selects, but who cares :-o */
    pthread_mutex_lock(&mutex);
    for (int j = 0; err == 0 && j < (*i)->cnt; j++) {
      queue_row_t row;
      my_off_t off = (*i)->offsets[j];
      if (read(&row, off, queue_row_t::header_size(), false)
	  == queue_row_t::header_size()) {
	row.set_type(queue_row_t::type_removed);
	if (pwrite(fd, &row, queue_row_t::header_size(), off)
	    != queue_row_t::header_size()) {
	  err = HA_ERR_CRASHED_ON_USAGE;
	}
	update_cache(&row, off, queue_row_t::header_size());
	if (_header.begin() == off) {
	  if (next(&off) == 0) {
	    _header.set_begin(off);
	  } else {
	    err = HA_ERR_CRASHED_ON_USAGE;
	  }
	}
      } else {
	err = HA_ERR_CRASHED_ON_USAGE;
      }
    }
    pthread_mutex_unlock(&mutex);
    (*i)->err = err;
    pthread_cond_signal((*i)->cond);
  }
}

void *queue_share_t::writer_start()
{
  pthread_mutex_lock(&mutex);
  
  while (1) {
    /* wait for signal if we do not have any pending writes */
    while (remove_list->size() == 0 && append_list->size() == 0) {
      if (writer_exit) {
	goto EXIT;
      } else if (num_readers == 0
		 && DO_COMPACT(_header.end() - sizeof(_header),
			       _header.begin() - sizeof(_header))) {
	compact();
      } else {
	pthread_cond_wait(&writer_cond, &mutex);
      }
    }
    /* detach operation lists */
    remove_list_t *rl = NULL;
    append_list_t *al = NULL;
    if (remove_list->size() != 0) {
      rl = remove_list;
      remove_list = new remove_list_t();
    }
    if (append_list->size() != 0) {
      al = append_list;
      append_list = new append_list_t();
    }
    /* do the task and send back the results */
    pthread_mutex_unlock(&mutex);
    if (rl != NULL) {
      writer_do_remove(rl);
      delete rl;
    }
    if (al != NULL) {
      int err = 0;
      if ((err = writer_do_append(al)) != 0) {
	sync_file(fd);
      }
      close_append_list(al, err);
    } else {
      sync_file(fd);
    }
    pthread_mutex_lock(&mutex);
  }
  
 EXIT:
  pthread_mutex_unlock(&mutex);
  return NULL;
}

static int copy_file_content(int src_fd, my_off_t begin, my_off_t end,
			     int dest_fd)
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
  my_off_t delta = _header.begin() - sizeof(queue_file_header_t);
  
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
    if (write(tmp_fd, &hdr, sizeof(hdr)) != sizeof(hdr)
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

ha_queue::ha_queue(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share(NULL),
   pos(),
   rows(NULL),
   rows_size(0),
   rows_reserved(0),
   bulk_insert_rows(-1),
   bulk_delete_rows(NULL)
{
  pthread_cond_init(&cond, NULL);
}

ha_queue::~ha_queue()
{
  delete bulk_delete_rows;
  bulk_delete_rows = NULL;
  free_rows_buffer();
  pthread_cond_destroy(&cond);
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
  free_rows_buffer();
  return 0;
}

int ha_queue::rnd_next(uchar *buf)
{
  assert(rows_size == 0);
  
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
  
  { /* read data to row buffer */
    queue_row_t hdr;
    if (share->read(&hdr, pos, queue_row_t::header_size(), true)
	!= queue_row_t::header_size()) {
      err = HA_ERR_CRASHED_ON_USAGE;
      goto EXIT;
    }
    if (prepare_rows_buffer(queue_row_t::header_size() + hdr.size()) != 0) {
      err = HA_ERR_OUT_OF_MEM;
      goto EXIT;
    }
    if (share->read(rows, pos, queue_row_t::header_size() + hdr.size(), false)
	!= queue_row_t::header_size() + hdr.size()) {
      err = HA_ERR_CRASHED_ON_USAGE;
      goto EXIT;
    }
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
  assert(rows_size == 0);
  
  pos = static_cast<my_off_t>(my_get_ptr(_pos, sizeof(pos)));
  int err = 0;
  
  share->lock();
  /* we should return the row even if it had the deleted flag set during the
   * execution by other threads
   */
  queue_row_t hdr;
  if (share->read(&hdr, pos, queue_row_t::header_size(), true)
      != queue_row_t::header_size()) {
    err = HA_ERR_CRASHED_ON_USAGE;
    goto EXIT;
  }
  if (prepare_rows_buffer(queue_row_t::header_size() + hdr.size()) != 0) {
    err = HA_ERR_OUT_OF_MEM;
    goto EXIT;
  }
  if (share->read(rows, pos, hdr.size(), false) != hdr.size()) {
    err = HA_ERR_CRASHED_ON_USAGE;
    goto EXIT;
  }
  share->unlock();
  
  unpack_row(buf);
  return 0;
  
 EXIT:
  share->unlock();
  return err;
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
    // just follow ha_archive::store_lock
    if ((TL_WRITE_CONCURRENT_INSERT <= lock_type && lock_type <= TL_WRITE)
	&& ! thd_in_lock_tables(thd) && ! thd_tablespace_op(thd)) {
      lock.type = TL_WRITE_ALLOW_WRITE;
    } else if (lock_type == TL_READ_NO_INSERT && ! thd_in_lock_tables(thd)) {
      lock.type = TL_READ;
    } else {
      lock.type = lock_type;
    }
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
  assert(rows_size == 0);
  assert(bulk_insert_rows == -1);
  bulk_insert_rows = 0;
}

int ha_queue::end_bulk_insert()
{
  int ret = 0;
  
  if (rows_size != 0) {
    if ((ret = share->write_rows(rows, rows_size, &cond)) == 0) {
      for (size_t i = 0; i < bulk_insert_rows; i++) {
	share->wake_listener();
      }
    }
    rows_size = 0;
  }
  free_rows_buffer();
  bulk_insert_rows = -1;
  
  return ret;
}

bool ha_queue::start_bulk_delete()
{
  assert(bulk_delete_rows == NULL);
  bulk_delete_rows = new vector<my_off_t>();
  return false;
}

int ha_queue::end_bulk_delete()
{
  int ret = 0;
  
  assert(bulk_delete_rows != NULL);
  if (bulk_delete_rows->size() != 0) {
    share->lock();
    ret =
      share->remove_rows(&bulk_delete_rows->front(), bulk_delete_rows->size(),
			 &cond);
    share->unlock();
  }
  delete bulk_delete_rows;
  bulk_delete_rows = NULL;
  
  return ret;
}

int ha_queue::write_row(uchar *buf)
{
  size_t sz;
  
  if ((sz = pack_row(buf)) == 0) {
    return HA_ERR_OUT_OF_MEM;
  }
  if (bulk_insert_rows == -1) {
    int err = share->write_rows(rows, sz, &cond);
    free_rows_buffer();
    if (err != 0) {
      return err;
    }
    share->wake_listener();
  } else {
    rows_size += sz;
    bulk_insert_rows++;
  }
  
  return 0;
}

int ha_queue::update_row(const uchar *old_data __attribute__((unused)),
			 uchar *new_data)
{
  return HA_ERR_WRONG_COMMAND;
}

int ha_queue::delete_row(const uchar *buf __attribute__((unused)))
{
  int err = 0;
  
  share->lock();
  pthread_t owner = share->find_owner(pos);
  if (owner != 0 && owner != pthread_self()) {
    share->unlock();
    return HA_ERR_RECORD_DELETED;
  }
  if (bulk_delete_rows != NULL) {
    share->unlock();
    bulk_delete_rows->push_back(pos);
  } else {
    err = share->remove_rows(&pos, 1, &cond);
    share->unlock();
  }
  
  return err;
}

int ha_queue::prepare_rows_buffer(size_t sz)
{
  if (rows == NULL) {
    assert(rows_size == 0);
    rows_reserved = MIN_ROWS_BUFFER_SIZE;
    while (rows_reserved < sz) {
      rows_reserved *= 2;
    }
    if ((rows = static_cast<uchar*>(my_malloc(rows_reserved, MYF(0))))
	== NULL) {
      return -1;
    }
  } else if (rows_reserved < rows_size + sz) {
    size_t new_reserve = rows_reserved;
    do {
      new_reserve *= 2;
    } while (new_reserve < rows_size + sz);
    void *pt;
    if ((pt = my_realloc(rows, new_reserve, MYF(0))) == NULL) {
      return -1;
    }
    rows = static_cast<uchar*>(pt);
    rows_reserved = new_reserve;
  }
  return 0;
}

void ha_queue::free_rows_buffer()
{
  if (rows != NULL) {
    my_free(rows, MYF(0));
    rows = NULL;
  }
}

void ha_queue::unpack_row(uchar *buf)
{
  const uchar *src = rows + queue_row_t::header_size();
			  
  memcpy(buf, src, table->s->null_bytes);
  src += table->s->null_bytes;
  for (Field **field = table->field; *field != NULL; field++) {
    if (! (*field)->is_null()) {
      src = (*field)->unpack(buf + (*field)->offset(table->record[0]), src);
    }
  }
}

size_t ha_queue::pack_row(uchar *buf)
{
  /* allocate memory (w. some extra) */
  size_t sz = queue_row_t::header_size() + table->s->reclength
    + table->s->fields * 2;
  for (uint *ptr = table->s->blob_field, *end = ptr + table->s->blob_fields;
       ptr != end;
       ++ptr) {
    sz += 2 + ((Field_blob*)table->field[*ptr])->get_length();
  }
  if (sz > queue_row_t::max_size || prepare_rows_buffer(sz) != 0) {
    return -1;
  }
  /* write data */
  uchar *dst = rows + rows_size + queue_row_t::header_size();
  memcpy(dst, buf, table->s->null_bytes);
  dst += table->s->null_bytes;
  for (Field **field = table->field; *field != NULL; field++) {
    if (! (*field)->is_null()) {
      dst = (*field)->pack(dst, buf + (*field)->offset(buf));
    }
  }
  /* write header */
  sz = dst - (rows + rows_size);
  new (reinterpret_cast<queue_row_t*>(rows + rows_size))
    queue_row_t(sz - queue_row_t::header_size());
  return sz;
}

static handler *create_handler(handlerton *hton, TABLE_SHARE *table,
			MEM_ROOT *mem_root)
{
  return new (mem_root) ha_queue(hton, table);
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
    pthread_cond_t cond;
    pthread_cond_init(&cond, NULL);
    share->lock();
    my_off_t off = share->get_owned_row(pthread_self());
    assert(off != 0);
    share->remove_rows(&off, 1, &cond);
    share->get_owned_row(pthread_self(), true);
    share->unlock();
    share->release();
    pthread_setspecific(share_key, NULL);
    pthread_cond_destroy(&cond);
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

static int init_plugin(void *p)
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

static int deinit_plugin(void *p)
{
  hash_free(&queue_open_tables);
  queue_hton = NULL;
  
  return 0;
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
  init_plugin,
  deinit_plugin,
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
  if (ret != 0) {
    pthread_setspecific(share_key, info->share);
  }
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
