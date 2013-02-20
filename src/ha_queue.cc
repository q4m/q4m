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
#include <map>
#include <string>
#include <vector>

#define MYSQL_SERVER

#include "mysql_version.h"

#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#else
#include "sql_priv.h"
#include "sql_base.h"
#include "probes_mysql.h"
#endif
#undef PACKAGE
#undef VERSION
#undef HAVE_DTRACE
#undef _DTRACE_VERSION
#undef PACKAGE_NAME
#undef PACKAGE_STRING
#undef PACKAGE_TARNAME
#undef PACKAGE_VERSION
#include <mysql/plugin.h>

#if MYSQL_VERSION_ID >= 50500
#define my_free(PTR, FLAG) my_free(PTR)
#endif

#if MYSQL_VERSION_ID < 50500
#define mysql_mutex_lock(mutex) pthread_mutex_lock(mutex)
#define mysql_mutex_unlock(mutex) pthread_mutex_unlock(mutex)
#endif

#define Q4M_DELETE_MSYNC 1
#define Q4M_DELETE_MT_PWRITE 2
#define Q4M_DELETE_SERIAL_PWRITE 3

#if MYSQL_VERSION_ID < 50500
#include "queue_config.h"
#endif

#if SIZEOF_OFF_T != 8
#  error "support for 64-bit file offsets is mandatory"
#endif
#ifdef HAVE_LSEEK64
#  define lseek  lseek64
#  define pread  pread64
#  define pwrite pwrite64
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif

#include "ha_queue.h"
#include "adler32.c"

extern uint build_table_filename(char *buff, size_t bufflen, const char *db,
				 const char *table, const char *ext,
				 uint flags);


using namespace std;

#define MIN_ROWS_BUFFER_SIZE (4 * 1024)
#define FREE_ROWS_BUFFER_SIZE (64 * 1024)
#define COMPACT_THRESHOLD (16 * 1024 * 1024)
#define EXPAND_BY (4 * 1024 * 1024)
#if SIZEOF_INTP == 4
# define MMAP_MAX_DEFAULT (256ULL * 1024 * 1024)
#else
# define MMAP_MAX_DEFAULT (16384ULL * 1024 * 1024) // 16GB, since max_map_count=65530 on linux, limiting max accessible memory size to 256GB on linux
#endif

#define DO_COMPACT(all, free) \
  ((all) >= COMPACT_THRESHOLD && (free) * 4 >= (all) * 3)
#define Q4M ".Q4M"
#define Q4T ".Q4T"

static ulonglong mmap_max;
static MYSQL_SYSVAR_ULONGLONG(mmap_max,
			      mmap_max,
			      PLUGIN_VAR_READONLY | PLUGIN_VAR_RQCMDARG,
			      "per-table limit of mapping data onto memory (in bytes)",
			      NULL, NULL, MMAP_MAX_DEFAULT, EXPAND_BY, 0,
			      EXPAND_BY);

static int concurrent_compaction;
static MYSQL_SYSVAR_INT(use_concurrent_compaction,
			concurrent_compaction,
			PLUGIN_VAR_READONLY | PLUGIN_VAR_RQCMDARG,
			"set to 1 if concurrent compaction is on",
			NULL, NULL, 0, 0, 1, 0);
static int concurrent_compaction_interval;
static MYSQL_SYSVAR_INT(concurrent_compaction_interval,
			concurrent_compaction_interval,
			PLUGIN_VAR_RQCMDARG,
			"interval of accepting INSERTs during compaction (in bytes)",
			NULL, NULL, 1048576, 1024, INT_MAX, 0);

static HASH queue_open_tables;
static pthread_mutex_t open_mutex, listener_mutex, tbl_stat_mutex;
#if Q4M_DELETE_METHOD == Q4M_DELETE_MSYNC
static ptrdiff_t psz_mask;
#endif

#ifdef Q4M_USE_RELATIVE_TIMEDWAIT
# ifdef SAFE_MUTEX
static int safe_cond_timedwait_relative_np(pthread_cond_t *cond,
					   safe_mutex_t *mp,
					   struct timespec *abstime,
					   const char *file, uint line);
#define pthread_cond_timedwait_relative_np(A,B,C) safe_cond_timedwait_relative_np((A),(B),(C),__FILE__,__LINE__)
# elif defined(MY_PTHREAD_FASTMUTEX)
#define pthread_cond_timedwait_relative_np(A,B,C) pthread_cond_timedwait_relative_np((A),&(B)->mutex,(C))
# endif
#endif

static handlerton *queue_hton;

/* stat */
static pthread_mutex_t stat_mutex;
struct stat_value {
  my_off_t value;
  stat_value() : value(0) {}
  void incr(my_off_t d = 1) {
    pthread_mutex_lock(&stat_mutex);
    value += d;
    pthread_mutex_unlock(&stat_mutex);
  }
};
#define STAT_VALUE(n) stat_value stat_##n;
STAT_VALUE(sys_read);
STAT_VALUE(sys_write);
STAT_VALUE(sys_sync);
STAT_VALUE(read_cachehit);
STAT_VALUE(writer_append);
STAT_VALUE(writer_remove);
STAT_VALUE(cond_eval);
STAT_VALUE(cond_compile);
STAT_VALUE(cond_compile_cachehit);
STAT_VALUE(rows_written);
STAT_VALUE(rows_removed);
STAT_VALUE(queue_wait);
STAT_VALUE(queue_end);
STAT_VALUE(queue_abort);
STAT_VALUE(queue_rowid);
STAT_VALUE(queue_set_srcid);
#undef STAT_VALUE

#define log(fmt, ...) do {						\
    time_t _t = time(NULL);						\
    tm _tm;								\
    localtime_r(&_t, &_tm);						\
    fprintf(stderr,							\
	    "%02d%02d%02d %02d:%02d:%02d ha_queue: " __FILE__ ":%d: " fmt, \
	    _tm.tm_year % 100, _tm.tm_mon + 1, _tm.tm_mday,	        \
	    _tm.tm_hour, _tm.tm_min, _tm.tm_sec,			\
	    __LINE__, ## __VA_ARGS__);					\
  } while (0)
#define kill_proc(...) do { \
    log(__VA_ARGS__);	    \
    abort();		    \
    *(char*)1 = 1;	    \
  } while (0)

inline ssize_t sys_pread(int d, void *b, size_t n, my_off_t o)
{
  stat_sys_read.incr();
  return ::pread(d, b, n, o);
}

inline ssize_t sys_write(int d, const void *b, size_t n)
{
  stat_sys_write.incr();
  return ::write(d, b, n);
}

inline ssize_t sys_pwrite(int d, const void *b, size_t n, my_off_t o)
{
  stat_sys_write.incr();
  return ::pwrite(d, b, n, o);
}

inline ssize_t sys_writev(int d, const iovec *iov, int iovcnt)
{
  stat_sys_write.incr();
  return ::writev(d, iov, iovcnt);
}

static void sync_file(int fd)
{
  if (
#ifdef FDATASYNC_USE_FCNTL
      /* fallback to fsync on drives wo. F_FULLFSYNC support */
      fcntl(fd, F_FULLFSYNC, 0) == -1 && fsync(fd) != 0
#elif defined(FDATASYNC_USE_FSYNC)
      fsync(fd) != 0
#elif defined(FDATASYNC_SKIP)
      0
#else
      fdatasync(fd) != 0
#endif
      ) {
    kill_proc("failed to sync disk (errno:%d)\n", errno);
  }
  stat_sys_sync.incr();
}

static void setup_timespec(timespec* ts, int msec)
{
  timeval tv;
  gettimeofday(&tv, NULL);
  ts->tv_sec = tv.tv_sec + msec / 1000;
  ts->tv_nsec = (tv.tv_usec + msec % 1000 * 1000000) * 1000;
  if (ts->tv_nsec >= 1000000000) {
    ts->tv_sec += 1;
    ts->tv_nsec -= 1000000000;
  }
}

static int timedlock_mutex(pthread_mutex_t *mutex, int msec)
{
#if defined(HAVE_PTHREAD_MUTEX_TIMEDLOCK) && ! defined(SAFE_MUTEX)
  timespec ts;
  setup_timespec(&ts, msec);
  return pthread_mutex_timedlock(
#if defined(MY_PTHREAD_FASTMUTEX)
				 &mutex->mutex,
#else
				 mutex,
#endif
				 &ts);
#else
  return pthread_mutex_trylock(mutex);
#endif
}

static int timedwait_cond(pthread_cond_t *cond, pthread_mutex_t *mutex, int msec)
{
#ifdef Q4M_USE_RELATIVE_TIMEDWAIT
  timespec ts = {
    msec / 1000,
    msec % 1000 * 1000000
  };
  return pthread_cond_timedwait_relative_np(cond, mutex, &ts);
#else
  timespec ts;
  setup_timespec(&ts, msec);
  return pthread_cond_timedwait(cond, mutex, &ts);
#endif
}

static cac_mutex_t<queue_share_t::stats_t>* get_stats_for(const char* _name,
							  bool remove = false)
{
  static map<string, cac_mutex_t<queue_share_t::stats_t>*> stats;
  
  string name(_name);
  cac_mutex_t<queue_share_t::stats_t>* ret = NULL;
  
  pthread_mutex_lock(&tbl_stat_mutex);
  map<string, cac_mutex_t<queue_share_t::stats_t>*>::iterator i
    = stats.lower_bound(name);
  if (i != stats.end() && i->first == name) {
    if (remove) {
      stats.erase(i);
    } else {
      ret = i->second;
    }
  } else {
    if (remove) {
      // nothing to do
    } else {
      ret = new cac_mutex_t<queue_share_t::stats_t>(NULL);
      stats.insert(make_pair(name, ret));
    }
  }
  pthread_mutex_unlock(&tbl_stat_mutex);
  
  return ret;
}

my_off_t queue_row_t::validate_checksum(int fd, my_off_t off)
{
  my_off_t off_end;
  char _len[sizeof(my_off_t)];
  
  /* read checksum size */
  off += queue_row_t::header_size();
  if (sys_pread(fd, _len, sizeof(_len), off) != sizeof(_len)) {
    return 0;
  }
  off += sizeof(_len);
  off_end = off + uint8korr(_len);
  /* calc checksum */
  uint32_t adler = 1;
  while (off != off_end) {
    /* read header */
    queue_row_t r;
    if (off_end - off < header_size()
	|| sys_pread(fd, &r, header_size(), off)
	!= static_cast<ssize_t>(header_size())) {
      return 0;
    }
    switch (r.type()) {
    case type_checksum:
      return 0;
    case type_row_removed:
      r.set_type(type_row);
      break;
    case type_row_received_removed:
      r.set_type(type_row_received);
      break;
    default:
      break;
    }
    adler = adler32(adler, &r, header_size());
    off += header_size();
    /* read data */
    my_off_t row_end = off + r.size();
    if (row_end > off_end) {
      return 0;
    }
    while (off != row_end) {
      char buf[4096];
      ssize_t bs = min((size_t)(row_end - off), sizeof(buf));
      if (sys_pread(fd, buf, bs, off) != bs) {
	return 0;
      }
      adler = adler32(adler, buf, bs);
      off += bs;
    }
  }
  /* compare checksum */
  return size() == (adler & size_mask) ? off : 0;
}

void queue_row_t::create_checksum(queue_row_t *checksum, my_off_t sz,
				  uint32_t adler)
{
  int4store(checksum->_size, type_checksum | (adler & size_mask));
  int8store(checksum->_bytes, sz);
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
  create_checksum(row, sz, adler);
  
  return row;
}

queue_file_header_t::queue_file_header_t()
{
  int4store(_magic, MAGIC_V2);
  int4store(_attr, 0);
  int8store(_end, static_cast<my_off_t>(sizeof(queue_file_header_t)));
  int8store(_begin, static_cast<my_off_t>(sizeof(queue_file_header_t)));
  int8store(_begin_row_id, 1LL);
  memset(_last_received_offsets, 0, sizeof(_last_received_offsets));
  int8store(_row_count, 0ULL);
  memset(_padding, 0, sizeof(_padding));
}

void queue_file_header_t::write(int fd)
{
  if (sys_pwrite(fd, this, sizeof(*this), 0) != sizeof(*this)) {
    kill_proc("failed to update header\n");
  }
}

uchar* queue_share_t::get_share_key(queue_share_t *share, size_t *length,
				    my_bool not_used __attribute__((unused)))
{
  *length = share->table_name_length;
  return reinterpret_cast<uchar*>(share->table_name);
}

void queue_share_t::recalc_row_count(info_t *info, bool log)
{
  my_off_t off = info->_header.begin(), row_count = 0;
  
  while (off != info->_header.end()) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      kill_proc("I/O error: %s\n", table_name);
    }
    switch (row.type()) {
    case queue_row_t::type_row:
    case queue_row_t::type_row_received:
      row_count++;
      break;
    default:
      break;
    }
    off = row.next(off);
  }
  if (log || row_count != info->_header.row_count()) {
    log("setting row count of %s.Q4M to %llu (was %llu)\n", table_name,
	row_count, info->_header.row_count());
  }
  info->_header.set_row_count(row_count);
}

bool queue_share_t::fixup_header(info_t *info)
{
  log("%s.Q4M was not closed properly... checking consistency.\n", table_name);
  /* update end */
  my_off_t off = info->_header.end(), old_off = off;
  while (1) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      break;
    }
    if (row.type() != queue_row_t::type_checksum) {
      break;
    }
    if ((off = row.validate_checksum(fd, off)) == 0) {
      break;
    }
    info->_header.set_end(off);
  }
  log("setting end offset to %llu (was %llu).\n", off, old_off);
  /* update last_received_offsets */
  off = info->_header.begin();
  while (off < info->_header.end()) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      log("I/O error while reading table: %s, at %llu\n", table_name, off);
      return false;
    }
    if (row.type() == queue_row_t::type_row_received
	|| row.type() == queue_row_t::type_row_received_removed) {
      queue_source_t source(0, 0);
      if (read(&source,
	       off + queue_row_t::header_size() + row.size() - sizeof(source),
	       sizeof(source))
	  != sizeof(source)) {
	log("corrupt table: %s\n", table_name);
	return false;
      }
      if (source.sender() > QUEUE_MAX_SOURCES) {
	log("corrupt table: %s\n", table_name);
	return false;
      }
      info->_header.set_last_received_offset(source.sender(), source.offset());
    }
    off = row.next(off);
  }
  /* update begin */
  off = info->_header.begin();
  my_off_t row_id = info->_header.begin_row_id();
  while (off < info->_header.end()) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      log("I/O error while reading table: %s, at %llu\n", table_name, off);
      return false;
    }
    switch (row.type()) {
    case queue_row_t::type_row:
    case queue_row_t::type_row_received:
      goto BEGIN_FOUND;
    case queue_row_t::type_row_removed:
    case queue_row_t::type_row_received_removed:
      row_id++;
      break;
    case queue_row_t::type_num_rows_removed:
      row_id += row.size();
      break;
    default:
      break;
    }
    off = row.next(off);
  }
  if (info->_header.end() < off) {
    log("corrupt table: %s\n", table_name);
    return false;
  }
 BEGIN_FOUND:
  log("setting begin offset to %llu (rowid=%llu), was %llu (%llu)\n",
      off, row_id, info->_header.begin(), info->_header.begin_row_id());
  info->_header.set_begin(off, row_id);
  /* update row_count */
  recalc_row_count(info, true);
  /* save */
  info->_header.set_attr(info->_header.attr()
			 & ~queue_file_header_t::attr_is_dirty);
  info->_header.write(fd);
  sync_file(fd);
  log("finished consistency checking.\n");
  return true;
}

#ifdef Q4M_USE_MMAP
int queue_share_t::mmap_table(size_t new_size)
{
  cac_rwlock_t<mmap_info_t>::writeref map(this->map);
  
  if (map->ptr != NULL) {
    munmap(map->ptr, map->len);
    map->len = 0;
  }
  if ((map->ptr = static_cast<char*>(mmap(NULL, new_size,
#if Q4M_DELETE_METHOD == Q4M_DELETE_MSYNC
					  PROT_READ | PROT_WRITE,
#else
					  PROT_READ,
#endif
					  MAP_SHARED, fd, 0)))
      == MAP_FAILED) {
    log("mmap failed, will use file file I/O for table: %s\n", table_name);
    return -1;
  }
  map->len = new_size;
  
  return 0;
}
#endif

static char *parse_db_table_name(const char *db_table_name, char*& db,
                                 char*& table)
{
  char *db_table_buf;
  
  if ((db_table_buf = strdup(db_table_name)) == NULL) {
    log("out of memory\n");
    return NULL;
  }
  for (db = db_table_buf; *db == '/' || *db == '.'; db++)
    ;
  if (*db == '\0') {
    log("invalid table name: %s\n", db_table_name);
    goto Error;
  }
  for (table = db + 1; *table != '/'; table++) {
    if (*table == '\0') {
      log("invalid table name: %s\n", db_table_name);
      goto Error;
    }
  }
  *table++ = '\0';
  
  return db_table_buf;
 Error:
  free(db_table_buf);
  return NULL;
}

#if MYSQL_VERSION_ID < 50500
static bool load_table51(TABLE *table, const char *db_table_name)
{
  // precondition: LOCK_open should be acquired
  
  TABLE_SHARE *share;
  TABLE_LIST table_list;
  char key[MAX_DBKEY_LENGTH];
  uint key_length;
  int err;
  char *db_table_buf;
  
  memset(&table_list, 0, sizeof(TABLE_LIST));
  memset(table, 0, sizeof(TABLE));
  
  /* copy table name to buffer and split to db name and table name */
  if ((db_table_buf = parse_db_table_name(db_table_name, table_list.db,
                                          table_list.table_name))
      == NULL) {
    return false;
  }
  
  /* load table data */
  key_length = create_table_def_key(current_thd, key, &table_list, 0);
#if MYSQL_VERSION_ID < 50500
  share = get_table_share(current_thd, &table_list, key, key_length, 0, &err);
#else
  my_hash_value_type hash_value;
  hash_value = my_calc_hash(&table_def_cache, (uchar*) key, key_length);
  share = get_table_share(current_thd, &table_list, key, key_length, 0, &err, hash_value);
#endif
  if (share == NULL) {
    return true;
  }
  if (open_table_from_share(current_thd, share, table_list.table_name, 0,
			    READ_ALL, 0, table, FALSE)
      != 0) {
    goto Error;
  }
  
  /* free and return */
  free(db_table_buf);
  return true;
  
 Error:
  free(db_table_buf);
  return false;
}
#endif

queue_share_t *queue_share_t::get_share(const char *table_name, bool if_is_open)
{
  queue_share_t *share;
  uint table_name_length;
  char *tmp_name;
  char filename[FN_REFLEN];
  
  pthread_mutex_lock(&open_mutex);
  
#if Q4M_DELETE_METHOD == Q4M_DELETE_MSYNC
  if (psz_mask == 0) {
    psz_mask = ~ static_cast<ptrdiff_t>(getpagesize() - 1);
    log("pagemask set to: %lx\n", psz_mask);
  }
#endif
  
  table_name_length = strlen(table_name);
  
  /* return the one, if found (after incrementing refcount) */
  if ((share = reinterpret_cast<queue_share_t*>(my_hash_search(&queue_open_tables, reinterpret_cast<const uchar*>(table_name), table_name_length)))
      != NULL) {
    ++share->ref_cnt;
    pthread_mutex_unlock(&open_mutex);
    return share;
  }
  if (if_is_open) {
    pthread_mutex_unlock(&open_mutex);
    return NULL;
  }
  
  /* alloc */
  if (my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(queue_share_t),
		      &tmp_name, table_name_length + 1, NullS)
      == NULL) {
    goto ERR_RETURN;
  }
  
  /* init members that would always succeed in doing so */
  share->ref_cnt = 1;
  share->table_name = tmp_name;
  strmov(share->table_name, table_name);
  share->table_name_length = table_name_length;
  pthread_mutex_init(&share->compact_mutex, MY_MUTEX_INIT_FAST);
  {
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    // switch to writer-preferred lock on linux
#ifdef PTHREAD_RWLOCK_WRITER_NONRECURSIVE_INITIALIZER_NP
    pthread_rwlockattr_setkind_np(&attr,
				  PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
    pthread_rwlock_init(&share->rwlock, &attr);
    pthread_rwlockattr_destroy(&attr);
  }
  thr_lock_init(&share->store_lock);
  new (&share->info) cac_mutex_t<info_t>(NULL);
  new (&share->cond_expr_true)
    cond_expr_t(new queue_cond_t::const_node_t(queue_cond_t::value_t::int_value(1)),
		"1", 1, 0, 0);
  new (&share->listener_list) listener_list_t();
  
  { /* setup */
    cac_mutex_t<info_t>::lockref info(share->info);
    /* open file */
    fn_format(filename, share->table_name, "", Q4M,
	      MY_REPLACE_EXT | MY_UNPACK_FILENAME);
    if ((share->fd = open(filename, O_RDWR | O_LARGEFILE, 0)) == -1) {
      goto ERR_ON_FILEOPEN;
    }
    // log("open:fd=%d:file=%s\n", share->fd, filename);
    /* load header */
    if (sys_pread(share->fd, &info->_header, sizeof(info->_header), 0)
	!= sizeof(info->_header)) {
      goto ERR_AFTER_FILEOPEN;
    }
    switch (info->_header.magic()) {
    case queue_file_header_t::MAGIC_V1:
    case queue_file_header_t::MAGIC_V2:
      break;
    default:
      goto ERR_AFTER_FILEOPEN;
    }
    /* sanity check (or update row count if necessary) */
    if ((info->_header.attr() & queue_file_header_t::attr_is_dirty) != 0) {
      if (! share->fixup_header(info)) {
	goto ERR_AFTER_FILEOPEN;
      }
    } else if (info->_header.row_count() == 0) {
      share->recalc_row_count(info, false);
    }
    /* set dirty flag */
    info->_header.set_attr(info->_header.attr()
			   | queue_file_header_t::attr_is_dirty);
    info->_header.write(share->fd);
    sync_file(share->fd);
    /* seek to end position for inserts */
    if (lseek(share->fd, info->_header.end(), SEEK_SET) == -1) {
      goto ERR_AFTER_FILEOPEN;
    }
    { /* resize file to multiple of EXPAND_BY */
      struct stat st;
      if (fstat(share->fd, &st) != 0) {
	goto ERR_AFTER_FILEOPEN;
      }
      if (st.st_size % EXPAND_BY != 0
	  && ftruncate(share->fd,
		       (st.st_size + EXPAND_BY - 1) / EXPAND_BY * EXPAND_BY)
	  != 0) {
	log("failed to resize file to boundary: %s\n", filename);
	goto ERR_AFTER_FILEOPEN;
      }
    }
#ifdef Q4M_USE_MMAP
    /* mmap */
    if (share->mmap_table(max(min((info->_header.end() + EXPAND_BY - 1)
				  / EXPAND_BY * EXPAND_BY,
				  mmap_max),
			      EXPAND_BY))
	!= 0) {
      log("mmap failed\n");
      goto ERR_AFTER_FILEOPEN;
    }
#endif
  }
  share->stats = get_stats_for(table_name);
  
  /* start threads */
  share->writer_do_wake_listeners = false;
  if (pthread_create(&share->writer_thread, NULL, _writer_start, share) != 0) {
    goto ERR_AFTER_MMAP;
  }
  /* add to open_tables */
  if (my_hash_insert(&queue_open_tables, reinterpret_cast<uchar*>(share))) {
    goto ERR_AFTER_WRITER_START;
  }
  
  /* success, unlock */
  pthread_mutex_unlock(&open_mutex);
  
  return share;
  
 ERR_AFTER_WRITER_START:
  {
    cac_mutex_t<info_t>::lockref info(share->info);
    info->writer_exit = true;
    pthread_cond_signal(&info->to_writer_cond);
  }
  pthread_join(share->writer_thread, NULL);
 ERR_AFTER_MMAP:
#ifdef Q4M_USE_MMAP
  {
    cac_rwlock_t<mmap_info_t>::writeref mmapinfo(share->map);
    munmap(mmapinfo->ptr, mmapinfo->len);
  }
#endif
 ERR_AFTER_FILEOPEN:
  close(share->fd);
 ERR_ON_FILEOPEN:
  share->listener_list.~list();
  share->cond_expr_true.~cond_expr_t();
  share->info.~cac_mutex_t();
  thr_lock_delete(&share->store_lock);
  pthread_rwlock_destroy(&share->rwlock);
  pthread_mutex_destroy(&share->compact_mutex);
  my_free(reinterpret_cast<uchar*>(share), MYF(0));
 ERR_RETURN:
  pthread_mutex_unlock(&open_mutex);
  return NULL;
}

void queue_share_t::detach()
{
  pthread_mutex_lock(&open_mutex);
  my_hash_delete(&queue_open_tables, reinterpret_cast<uchar*>(this));
  pthread_mutex_unlock(&open_mutex);
}

void queue_share_t::release()
{
  pthread_mutex_lock(&open_mutex);
  
  if (--ref_cnt == 0) {
    my_hash_delete(&queue_open_tables, reinterpret_cast<uchar*>(this));
    {
      cac_mutex_t<info_t>::lockref info(this->info);
      info->writer_exit = true;
      pthread_cond_signal(&info->to_writer_cond);
    }
    if (pthread_join(writer_thread, NULL) != 0) {
      kill_proc("failed to join writer thread\n");
    }
    listener_list.~list();
    cond_expr_true.~cond_expr_t();
#ifdef Q4M_USE_MMAP
    {
      cac_rwlock_t<mmap_info_t>::writeref map(this->map);
      munmap(map->ptr, map->len);
    }
#endif
    {
      cac_mutex_t<info_t>::lockref info(this->info);
      info->_header.write(fd);
      sync_file(fd);
      info->_header.set_attr(info->_header.attr()
			     & ~queue_file_header_t::attr_is_dirty);
      info->_header.write(fd);
      sync_file(fd);
      close(fd);
      if (fixed_fields_ != NULL) {
	for (size_t i = 0; i < info->fields; i++) {
	  delete fixed_fields_[i];
	}
	delete [] fixed_fields_;
      }
    }
    info.~cac_mutex_t();
    thr_lock_delete(&store_lock);
    pthread_rwlock_destroy(&rwlock);
    pthread_mutex_destroy(&compact_mutex);
    my_free(reinterpret_cast<uchar*>(this), MYF(0));
  }
  
  pthread_mutex_unlock(&open_mutex);
}

bool queue_share_t::init_fixed_fields()
{
  if (fixed_fields_ != NULL) {
    return true;
  }
  
#if MYSQL_VERSION_ID < 50500
  // lock order: LOCK_open -> info_t
  mysql_mutex_lock(&LOCK_open);
  if (fixed_fields_ != NULL) {
    mysql_mutex_unlock(&LOCK_open);
    return true;
  }
  
  cac_mutex_t<info_t>::lockref info(this->info);
  TABLE table;
  if (fixed_fields_ != NULL) {
    mysql_mutex_unlock(&LOCK_open);
    return true;
  }
  if (! load_table51(&table, table_name)) {
    mysql_mutex_unlock(&LOCK_open);
    return false;
  }
  init_fixed_fields(info, &table);
  closefrm(&table, true);
  mysql_mutex_unlock(&LOCK_open);
  
  return true;
#else
  char *db, *tbl, *tmpbuf;
  if ((tmpbuf = parse_db_table_name(table_name, db, tbl)) == NULL) {
    return false;
  }
  TABLE* table = open_table_uncached(current_thd, table_name, db, tbl, false
#if MYSQL_VERSION_ID >= 50600
                                     , false /* open_in_engine */
#endif
                                     );
  if (table == NULL) {
    free(tmpbuf);
    return false;
  }
  {
    cac_mutex_t<info_t>::lockref info(this->info);
    if (fixed_fields_ == NULL) {
      init_fixed_fields(info, table);
    }
  }
  intern_close_table(table);
  free(tmpbuf);
  
  return true;
#endif
}

void queue_share_t::init_fixed_fields(info_t *info, TABLE *table)
{
  if (fixed_fields_ != NULL) {
    return;
  }
  
  /* setup fixed_fields */
  queue_fixed_field_t** fixed_fields
    = new queue_fixed_field_t* [table->s->fields];
  if (info->_header.magic() == queue_file_header_t::MAGIC_V2) {
    Field **field;
    int field_index;
    size_t off = table->s->null_bytes;
    for (field = table->field, field_index = 0;
	 *field != NULL;
	 field++, field_index++) {
      switch ((*field)->type()) {
#define TYPEMAP(type, cls) \
	case MYSQL_TYPE_##type:	\
	  fixed_fields[field_index] = new cls;	  \
          off += fixed_fields[field_index]->size(); \
	  break
	TYPEMAP(TINY, queue_int_field_t<1>(table, *field));
	TYPEMAP(SHORT, queue_int_field_t<2>(table, *field));
	TYPEMAP(INT24, queue_int_field_t<3>(table, *field));
	TYPEMAP(LONG, queue_int_field_t<4>(table, *field));
	TYPEMAP(LONGLONG, queue_int_field_t<8>(table, *field));
	TYPEMAP(FLOAT, queue_fixed_field_t(table, *field, sizeof(float)));
	TYPEMAP(DOUBLE, queue_fixed_field_t(table, *field, sizeof(double)));
	TYPEMAP(TIMESTAMP, queue_int_field_t<4>(table, *field));
	TYPEMAP(DATE, queue_int_field_t<4>(table, *field));
	TYPEMAP(NEWDATE, queue_int_field_t<3>(table, *field));
	TYPEMAP(TIME, queue_int_field_t<3>(table, *field));
	TYPEMAP(DATETIME, queue_int_field_t<8>(table, *field));
#undef TYPEMAP
      default:
	fixed_fields[field_index] = NULL;
	break;
      }
    }
  } else {
    fill(fixed_fields, fixed_fields + table->s->fields,
	 static_cast<queue_fixed_field_t*>(NULL));
  }
  /* setup other fields */
  info->null_bytes = table->s->null_bytes;
  info->fields = table->s->fields;
  info->fixed_buf_size = info->null_bytes;
  for (size_t i = 0; i < info->fields; i++) {
    const queue_fixed_field_t *field = fixed_fields[i];
    if (field != NULL) {
      if (field->is_convertible()) {
	info->cond_eval.add_column(field->name());
      }
      info->fixed_buf_size += field->size();
    }
  }
  info->fixed_buf = new uchar [info->fixed_buf_size];
  fixed_fields_ = fixed_fields;
}

bool queue_share_t::lock_reader(bool from_queue_wait)
{
  queue_connection_t *conn = queue_connection_t::current(true);
  
  if (from_queue_wait) {
    if (conn->reader_lock_cnt != 0) {
      return false;
    }
  } else {
    conn->reader_lock_cnt++;
  }
  
  pthread_rwlock_rdlock(&rwlock);
  return true;
}

void queue_share_t::unlock_reader(bool from_queue_wait, bool force_compaction)
{
  pthread_rwlock_unlock(&rwlock);
  
  if (! from_queue_wait) {
    queue_connection_t *conn = queue_connection_t::current();
    assert(conn != NULL);
    --conn->reader_lock_cnt;
  }
  
  // trigger compactation
  if ((force_compaction || ! from_queue_wait)
      && pthread_mutex_trylock(&compact_mutex) == 0) {
    bool do_compact = force_compaction;
    if (! do_compact) {
      queue_file_header_t *header = &info.unsafe_ref()->_header;
      if (DO_COMPACT(header->end() - sizeof(*header), bytes_removed)) {
	do_compact = true;
      }
    }
    if (do_compact) {
      pthread_rwlock_wrlock(&rwlock);
      cac_mutex_t<info_t>::lockref info(this->info);
      if (info->do_compact_cond == NULL
	  && (force_compaction
	      || DO_COMPACT(info->_header.end() - sizeof(info->_header),
			    bytes_removed))) {
	pthread_cond_t c;
	pthread_cond_init(&c, NULL);
	info->do_compact_cond = &c;
	pthread_cond_signal(&info->to_writer_cond);
	while (info->do_compact_cond != NULL) {
	  pthread_cond_wait(&c, this->info.mutex());
	}
	pthread_cond_destroy(&c);
      }
      pthread_rwlock_unlock(&rwlock);
    }
    pthread_mutex_unlock(&compact_mutex);
  }
}

void queue_share_t::unregister_listener(listener_t *l)
{
  for (listener_list_t::iterator i = listener_list.begin();
       i != listener_list.end();
       ++i) {
    if (i->l == l) {
      listener_list.erase(i);
      break;
    }
  }
}

bool queue_share_t::wake_listeners(bool from_writer)
{
  bool use_cond_expr = false;
  my_off_t off = (my_off_t)-1, row_id = 0;
  
  /* note: lock order should always be: listener_mutex -> rwlock -> mutex */
  if (timedlock_mutex(&listener_mutex, 10) != 0) {
    return false;
  }
  
  /* acquire rwlock */
  if (pthread_rwlock_tryrdlock(&rwlock) != 0) {
    pthread_mutex_unlock(&listener_mutex);
    return false;
  }
  
#ifdef Q4M_USE_MMAP
  /* remap if called from writer */
  if (from_writer
      && (map.unsafe_ref()->len
	  < min(info.unsafe_ref()->_header.end(), mmap_max))) {
    cac_mutex_t<info_t>::lockref info(this->info);
    if (map.unsafe_ref()->len < min(info->_header.end(), mmap_max)) {
      if (mmap_table(min((info->_header.end() + EXPAND_BY - 1) / EXPAND_BY
			 * EXPAND_BY,
			 mmap_max))
	  != 0) {
	log("mmap failed: size=%lu\n",
	    static_cast<unsigned long>(map.unsafe_ref()->len));
      }
    }
  }
#endif
  
  // remove listeners with signals received
  listener_list_t::iterator l = listener_list.begin();
  while (l != listener_list.end()) {
    if (l->l->listener->share_owned != NULL) {
      l = listener_list.erase(l);
    } else {
      if (l->cond != &cond_expr_true) {
	use_cond_expr = true;
      }
      if (l->cond->pos < off) {
	off = l->cond->pos;
	row_id = l->cond->row_id;
      }
      ++l;
    }
  }
  if (listener_list.size() == 0) {
    goto RETURN;
  }
  
  { // per-listener test
    cac_mutex_t<info_t>::lockref info(this->info);
    if (off == 0) {
      off = info->_header.begin();
      row_id = info->_header.begin_row_id();
    } else if (next(&off, &row_id) != 0) {
      log("internal error, table corrupt?\n");
      goto RETURN;
    }
    if (off != info->_header.end()) {
      l = listener_list.begin();
      while (l != listener_list.end()) {
	if (l->l->listener->share_owned != NULL) {
	  l = listener_list.erase(l);
	  continue;
	}
	while (find_owner(info, off) != 0) {
	  if (next(&off, &row_id) != 0) {
	    log("internal error, table corrupt? (off:%llu)\n", off);
	    goto RETURN;
	  } else if (off == info->_header.end()) {
	    goto RETURN;
	  }
      }
	if (check_cond_and_wake(info, off, row_id, &*l) != 0) {
	  l = listener_list.erase(l);
	} else {
	  ++l;
	}
      }
    }
  }
  
 RETURN:
  pthread_rwlock_unlock(&rwlock);
  pthread_mutex_unlock(&listener_mutex);
  return true;
}

struct queue_reset_owner_update_cond_expr {
  queue_share_t *share;
  my_off_t off;
  queue_reset_owner_update_cond_expr(queue_share_t *s, my_off_t o)
    : share(s), off(o) {}
  void operator()(queue_share_t::info_t *info, queue_share_t::cond_expr_t& e)
    const {
    if (off <= e.pos) {
      stat_cond_eval.incr();
      if (info->cond_eval.evaluate(e.node)) {
	// todo: should find a way to obtain prev. row
	e.pos = 0;
      }
    }
  }
};

my_off_t queue_share_t::reset_owner(queue_connection_t *conn)
{
  my_off_t off = 0;
  
  // find the row to be released, and remove it from owner list
  if (conn->share_owned != NULL) {
    if (concurrent_compaction) {
      pthread_rwlock_rdlock(&rwlock);
    }
    cac_mutex_t<info_t>::lockref info(this->info);
    conn->remove_from_owned_list(info->rows_owned);
    if ((off = conn->owned_row_off) != 0 && setup_cond_eval(info, off) == 0) {
      apply_cond_exprs(info, queue_reset_owner_update_cond_expr(this, off));
    }
    if (concurrent_compaction) {
      pthread_rwlock_unlock(&rwlock);
    }
  }
  
  return off;
}

int queue_share_t::write_rows(const void *rows, size_t rows_size,
			      size_t row_count)
{
  queue_connection_t *conn = queue_connection_t::current();
  queue_source_t *source =
    conn != NULL && conn->source.offset() != 0 ? &conn->source : NULL;
  
  append_t a(rows, rows_size, row_count, source);
  
  cac_mutex_t<info_t>::lockref info(this->info);
  
  if (source != NULL && ! conn->reset_source
      && (source->offset()
	  <= info->_header.last_received_offset(source->sender()))) {
    log("skipping forwarded duplicates: %s,max %llu,got %llu\n", table_name,
	info->_header.last_received_offset(source->sender()), source->offset());
    *source = queue_source_t(0, 0);
    return QUEUE_ERR_RECORD_EXISTS;
  }
  info->append_list->push_back(&a);
  pthread_cond_t *c = info->append_response_cond;
  pthread_cond_signal(&info->to_writer_cond);
  do {
    pthread_cond_wait(c, this->info.mutex());
  } while (a.err == -1);
  
  if (source != NULL) {
    *source = queue_source_t(0, 0);
  }
  return a.err;
}

ssize_t queue_share_t::read(void *data, my_off_t off, ssize_t size)
{
#ifdef Q4M_USE_MMAP
  {
    cac_rwlock_t<mmap_info_t>::readref map(this->map);
    if (off + size <= map->len) {
      memcpy(data, map->ptr + off, size);
      return size;
    }
  }
#endif
  return sys_pread(fd, data, size, off);
}

int queue_share_t::overwrite_byte(char byte, my_off_t off)
{
  int err = 0;

#if Q4M_DELETE_METHOD == Q4M_DELETE_MSYNC
  {
    cac_rwlock_t<mmap_info_t>::readref map(this->map);
    if (off < map->len) {
      assert(psz_mask != 0);
      map->ptr[off] = byte;
      char* page = static_cast<char*>(NULL)
	+ (reinterpret_cast<ptrdiff_t>(map->ptr + off) & psz_mask);
      if (msync(page, map->ptr + off - page + queue_row_t::header_size(),
		MS_ASYNC)
	  != 0) {
	log("msync failed\n");
	err = HA_ERR_CRASHED_ON_USAGE;
      }
      return err;
    }
  }
#endif
  
  if (sys_pwrite(fd, &byte, 1, off) != 1) {
    err = HA_ERR_CRASHED_ON_USAGE;
  }
  return err;
}

int queue_share_t::next(my_off_t *_off, my_off_t *row_id)
{
  my_off_t off = *_off;
  
  assert(off <= info.unsafe_ref()->_header.end());
  
  if (off == info.unsafe_ref()->_header.end()) {
    return 0;
  }
  queue_row_t row;
  if (read(&row, off, queue_row_t::header_size())
      != static_cast<ssize_t>(queue_row_t::header_size())) {
    return -1;
  }
  off = row.next(off);
  while (1) {
    if (off == info.unsafe_ref()->_header.end()) {
      if (row_id != NULL) {
	++*row_id;
      }
      *_off = off;
      return 0;
    }
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      return -1;
    }
    switch (row.type()) {
    case queue_row_t::type_row:
    case queue_row_t::type_row_received:
      if (row_id != NULL) {
	++*row_id;
      }
      *_off = off;
      return 0;
    case queue_row_t::type_row_removed:
    case queue_row_t::type_row_received_removed:
      if (row_id != NULL) {
	++*row_id;
      }
      break;
    case queue_row_t::type_num_rows_removed:
      if (row_id != NULL) {
	*row_id += row.size();
      }
      break;
    }
    off = row.next(off);
  }
}

int queue_share_t::remove_rows(my_off_t *offsets, int cnt)
{
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE
  int err;
  if ((err = do_remove_rows(offsets, cnt)) != 0) {
    return err;
  }
#endif

#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
  return 0;

#else
# if Q4M_DELETE_METHOD == Q4M_DELETE_SERIAL_PWRITE
  remove_t r(offsets, cnt);
# else
  remove_t r;
# endif
  
  cac_mutex_t<info_t>::lockref info(this->info);
  
  r.attach_back(info->remove_list);
  pthread_cond_t *c = info->remove_response_cond;
  pthread_cond_signal(&info->to_writer_cond);
  do {
    pthread_cond_wait(c, this->info.mutex());
  } while (r.err == -1);
  
  return r.err;
#endif
}

void queue_share_t::remove_owner(queue_connection_t *conn)
{
  conn->remove_from_owned_list(cac_mutex_t<info_t>::lockref(info)->rows_owned);
}

queue_connection_t *queue_share_t::find_owner(info_t *info, my_off_t off)
{
  if (info->max_owned_row_off < off) {
    return NULL;
  }
  queue_connection_t *c = info->rows_owned;
  if (c != NULL) {
    do {
      my_off_t owned_off = c->owned_row_off;
      // TODO do we need to update max_owned_row_off here?
      info->max_owned_row_off = max(info->max_owned_row_off, owned_off);
      if (off == owned_off) {
	return c;
      }
      c = c->next_owned();
    } while (c != info->rows_owned);
  }
  return NULL;
}

my_off_t queue_share_t::assign_owner(info_t *info, queue_connection_t *conn,
				     cond_expr_t *cond_expr)
{
  my_off_t off = cond_expr->pos, row_id = cond_expr->row_id;
  if (off == 0) {
    off = info->_header.begin();
    row_id = info->_header.begin_row_id();
  } else if (next(&off, &row_id) != 0) {
    return 0;
  }
  
  while (off != info->_header.end()) {
    cond_expr->pos = off;
    cond_expr->row_id = row_id;
    if (find_owner(info, off) == 0) {
      if (cond_expr == &cond_expr_true) {
	goto FOUND;
      } else {
	if (setup_cond_eval(info, off) != 0) {
	  log("internal error, table corrupt?");
	  return 0;
	}
	stat_cond_eval.incr();
	if (info->cond_eval.evaluate(cond_expr->node)) {
	  goto FOUND;
	}
      }
    }
    if (next(&off, &row_id) != 0) {
      return 0;
    }
  }
  return 0;
  
 FOUND:
  conn->share_owned = this;
  conn->owned_row_off = off;
  conn->owned_row_id = row_id;
  conn->add_to_owned_list(info->rows_owned);
  info->max_owned_row_off = max(info->max_owned_row_off, off);
  return off;
}

int queue_share_t::setup_cond_eval(info_t *info, my_off_t pos)
{
  /* read row data */
  queue_row_t hdr;
  if (read(&hdr, pos, queue_row_t::header_size())
      != static_cast<ssize_t>(queue_row_t::header_size())) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  if (read(info->fixed_buf, pos + queue_row_t::header_size(),
	   min((size_t)hdr.size(), info->fixed_buf_size))
      != static_cast<ssize_t>(min((size_t)hdr.size(), info->fixed_buf_size))) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  /* assign row data to evaluator */
  size_t col_index = 0, offset = info->null_bytes;
  for (size_t i = 0; i < info->fields; i++) {
    queue_fixed_field_t *field = fixed_fields_[i];
    if (field != NULL) {
      if (field->is_convertible()) {
	info->cond_eval.set_value(col_index++,
				  field->is_null(info->fixed_buf)
				  ? queue_cond_t::value_t::null_value()
				  : field->get_value(info->fixed_buf, offset));
      }
      if (! field->is_null(info->fixed_buf)) {
	offset += field->size();
      }
    }
  }
  assert(offset <= info->fixed_buf_size);
  return 0;
}

queue_share_t::cond_expr_t *
queue_share_t::compile_cond_expr(info_t *info, const char *expr, size_t len)
{
  cond_expr_t *e;
  
  if (expr == NULL) {
    return &cond_expr_true;
  }
  
  stat_cond_compile.incr();
  
  // return an existing one, if any
  if ((e = info->active_cond_exprs) != NULL) {
    do {
      if (e->expr_len == len && memcmp(e->expr, expr, len) == 0) {
	e->ref_cnt++;
	stat_cond_compile_cachehit.incr();
	return e;
      }
    } while ((e = e->next()) != info->active_cond_exprs);
  }
  if ((e = info->inactive_cond_exprs) != NULL) {
    do {
      if (e->expr_len == len && memcmp(e->expr, expr, len) == 0) {
	e->detach(info->inactive_cond_exprs);
	info->inactive_cond_expr_cnt--;
	e->attach_front(info->active_cond_exprs);
	e->ref_cnt++;
	stat_cond_compile_cachehit.incr();
	return e;
      }
    } while ((e = e->next()) != info->inactive_cond_exprs);
  }
  
  // compile and return
  queue_cond_t::node_t *n = info->cond_eval.compile_expression(expr, len);
  if (n == NULL) {
    return NULL;
  }
  e = new cond_expr_t(n, expr, len, 0, 0);
  e->attach_front(info->active_cond_exprs);
  return e;
}

void queue_share_t::release_cond_expr(cond_expr_t *e)
{
  if (e == &cond_expr_true) {
    return;
  }
  
  cac_mutex_t<info_t>::lockref info(this->info);
  assert(e->ref_cnt != 0);
  if (--e->ref_cnt == 0) {
    e->detach(info->active_cond_exprs);
    e->attach_front(info->inactive_cond_exprs);
    if (++info->inactive_cond_expr_cnt > 100) {
      info->inactive_cond_exprs->prev()->free(&info->inactive_cond_exprs);
      info->inactive_cond_expr_cnt--;
    }
  }
}

my_off_t queue_share_t::check_cond_and_wake(info_t *info, my_off_t off,
					    my_off_t row_id, listener_cond_t *l)
{
  while (off != info->_header.end()) {
    if (find_owner(info, off) == 0) {
      /* check if the row matches given condition */
      bool found = false;
      if (l->cond == &cond_expr_true) {
	found = true;
      } else if (l->cond->pos < off) {
	l->cond->pos = off;
	stat_cond_eval.incr();
	if (setup_cond_eval(info, off) != 0) {
	  log("internal error, table corrupt? (off:%llu)\n", off);
	  break;
	}
	if (info->cond_eval.evaluate(l->cond->node)) {
	  found = true;
	}
      }
      // log("cond: %s, offset: %llu, %s\n", cond->expr, off, found ? "found" : "not found");
      if (found) {
	queue_connection_t *conn = l->l->listener;
	conn->share_owned = this;
	conn->owned_row_off = off;
	conn->owned_row_id = row_id;
	conn->add_to_owned_list(info->rows_owned);
	info->max_owned_row_off = max(off, info->max_owned_row_off);
	l->l->queue_wait_index = l->queue_wait_index;
	pthread_cond_signal(&l->l->cond);
	return off;
      }
    }
    if (next(&off, &row_id) != 0) {
      log("internal error, table corrupt? (off:%llu)\n", off);
      break;
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
  }
  delete l;
}

int queue_share_t::writer_do_append(append_list_t *l)
{
  stat_writer_append.incr();
  /* build iovec */
  vector<iovec> iov;
  my_off_t total_len = 0, row_count = 0;
  iov.push_back(iovec());
  for (append_list_t::iterator i = l->begin(); i != l->end(); ++i) {
    iov.push_back(iovec());
    iov.back().iov_base = const_cast<void*>((*i)->rows);
    total_len += iov.back().iov_len = (*i)->rows_size;
    row_count += (*i)->row_count;
  }
  iov[0].iov_base =
    queue_row_t::create_checksum(&iov.front() + 1, iov.size() - 1);
  total_len += iov[0].iov_len =
    static_cast<queue_row_t*>(iov[0].iov_base)->next(0);
  // log("writev: %llu bytes (%d rows)\n", total_len, (int)(iov.size() - 1));
  { /* expand if necessary */
    queue_file_header_t *header = &info.unsafe_ref()->_header;
    if ((header->end() - 1) / EXPAND_BY
	!= (header->end() + total_len) / EXPAND_BY) {
      my_off_t new_len =
	((header->end() + total_len) / EXPAND_BY + 1) * EXPAND_BY;
      if (lseek(fd, new_len - 1, SEEK_SET) == -1
	  || sys_write(fd, "", 1) != 1
	  || lseek(fd, header->end(), SEEK_SET) == -1) {
      /* expansion failed */
	return HA_ERR_RECORD_FILE_FULL;
      }
    }
  }
  { /* write and sync */
    vector<iovec>::const_iterator writev_from = iov.begin();
    ssize_t writev_len = writev_from->iov_len;
    for (vector<iovec>::const_iterator i = iov.begin() + 1;
	 i != iov.end();
	 ++i) {
      if (i - writev_from >= IOV_MAX
	  || writev_len + i->iov_len > SSIZE_MAX / 2) {
	if (sys_writev(fd, &*writev_from, i - writev_from) != writev_len) {
	  my_free(iov[0].iov_base, MYF(0));
	  return HA_ERR_CRASHED_ON_USAGE;
	}
	writev_from = i;
	writev_len = 0;
      }
      writev_len += i->iov_len;
    }
    if (sys_writev(fd, &*writev_from, iov.end() - writev_from) != writev_len) {
      my_free(iov[0].iov_base, MYF(0));
      return HA_ERR_CRASHED_ON_USAGE;
    }
    sync_file(fd);
  }
  { /* update begin, end, cache, last_received_offset, row_count */
    cac_mutex_t<info_t>::lockref info(this->info);
    if (info->_header.begin() == info->_header.end()) {
      info->_header.set_begin(info->_header.begin()
			      + queue_row_t::checksum_size(),
			      info->_header.begin_row_id());
    }
    for (vector<iovec>::const_iterator i = iov.begin(); i != iov.end(); ++i) {
      info->_header.set_end(info->_header.end() + i->iov_len);
    }
    for (append_list_t::iterator i = l->begin(); i != l->end(); ++i) {
      const queue_source_t *s = (*i)->source;
      if (s != NULL) {
	info->_header.set_last_received_offset(s->sender(), s->offset());
      }
    }
    info->_header.set_row_count(info->_header.row_count() + row_count);
    info->rows_written += row_count;
  }
  
  my_free(iov[0].iov_base, MYF(0));
  return 0;
}

int queue_share_t::do_remove_rows(my_off_t *offsets, int cnt)
{
  int err = 0;
  
  for (int i = 0; i < cnt && err == 0; i++) {
    queue_row_t row;
    my_off_t off = offsets[i];
    {
      cac_mutex_t<info_t>::lockref info(this->info);
      if (off < info->_header.begin()) {
	// remove requests may arrive from multiple threads
	continue;
      }
      if (info->_header.end() <= off) {
	log("offset out of bounds: %llu, should be [%llu,%llu)\n", off, info->_header.begin(), info->_header.end());
	assert(0);
      }
    }
    if (read(&row, off, queue_row_t::header_size())
	== static_cast<ssize_t>(queue_row_t::header_size())) {
      switch (row.type()) {
      case queue_row_t::type_row:
	row.set_type(queue_row_t::type_row_removed);
	break;
      case queue_row_t::type_row_received:
	row.set_type(queue_row_t::type_row_received_removed);
	break;
      case queue_row_t::type_row_removed:
      case queue_row_t::type_row_received_removed:
	// rows might be DELETEed by its owner while in owner-mode
	break;
      default:
	log("internal inconsistency found, removing row with type: %08x at %llu\n", row.type(), off);
	err = HA_ERR_CRASHED_ON_USAGE;
	break;
      }
      err =
	overwrite_byte(reinterpret_cast<char*>(&row)[queue_row_t::type_offset],
		       off + queue_row_t::type_offset);
      bytes_removed += queue_row_t::header_size() + row.size();
      stat_rows_removed.incr();
      {
	cac_mutex_t<info_t>::lockref info(this->info);
	if (info->_header.begin() == off) {
	  my_off_t row_id = info->_header.begin_row_id();
	  if (next(&off, &row_id) == 0) {
	    info->_header.set_begin(off, row_id);
	  } else {
	    err = HA_ERR_CRASHED_ON_USAGE;
	  }
	}
	info->_header.set_row_count(info->_header.row_count() - 1);
	++info->rows_removed;
      }
    } else {
      log("got unexpected response while reading file\n");
      err = HA_ERR_CRASHED_ON_USAGE;
    }
  }
  
  return err;
}

#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
void queue_share_t::writer_do_remove(remove_t* l)
{
  stat_writer_remove.incr();
  
  remove_t *r = l;
  do {
#if Q4M_DELETE_METHOD == Q4M_DELETE_SERIAL_PWRITE
    r->err = do_remove_rows(r->offsets, r->cnt);
#else
    r->err = 0;
#endif
  } while ((r = r->detach(l)) != NULL);
}
#endif

void *queue_share_t::writer_start()
{
  cac_mutex_t<info_t>::lockref info(this->info);
  
  while (1) {
    /* wait for signal if we do not have any pending writes */
    do {
      if (info->do_compact_cond != NULL) {
        bytes_removed = 0;
        compact(info);
	pthread_cond_signal(info->do_compact_cond);
	info->do_compact_cond = NULL;
      }
      if (info->append_list->size() != 0
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
	  || info->remove_list != NULL
#endif
	  ) {
	break;
      } else if (info->writer_exit) {
	return NULL;
      }
      if (writer_do_wake_listeners) {
	timedwait_cond(&info->to_writer_cond, this->info.mutex(), 50);
      } else {
	pthread_cond_wait(&info->to_writer_cond, this->info.mutex());
      }
    } while (! writer_do_wake_listeners);
    /* detach operation lists */
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
    remove_t *rl = NULL;
    pthread_cond_t *remove_response_cond = NULL;
    if (info->remove_list != NULL) {
      rl = info->remove_list;
      info->remove_list = NULL;
      remove_response_cond = info->remove_response_cond;
      info->remove_response_cond = info->_remove_response_conds
	+ (info->_remove_response_conds == remove_response_cond);
    }
#endif
    append_list_t *al = NULL;
    pthread_cond_t *append_response_cond = NULL;
    if (info->append_list->size() != 0) {
      al = info->append_list;
      info->append_list = new append_list_t();
      append_response_cond = info->append_response_cond;
      info->append_response_cond = info->_append_response_conds
	+ (info->_append_response_conds == append_response_cond);
    }
    /* do the task and send back the results */
    pthread_mutex_unlock(this->info.mutex());
    { // hide ``info'' since we do not own the lock now
      int info __attribute__((unused)) = 0;
#if Q4M_DELETE_METHOD != Q4M_DELETE_SERIAL_PWRITE && defined(FDATASYNC_SKIP)
#else
      if (rl != NULL) {
	writer_do_remove(rl);
      }
#endif
      if (al != NULL) {
	int err = 0;
	if ((err = writer_do_append(al)) != 0) {
	  sync_file(fd);
	}
	close_append_list(al, err);
	pthread_cond_broadcast(append_response_cond);
	writer_do_wake_listeners = true;
      } else {
	sync_file(fd);
      }
      if (remove_response_cond != NULL) {
	pthread_cond_broadcast(remove_response_cond);
      }
      /* reset wake_listeners flag if successfully woke listeners */
      if (writer_do_wake_listeners && wake_listeners(true)) {
	writer_do_wake_listeners = false;
      }
    }
    pthread_mutex_lock(this->info.mutex());
  }
}

struct queue_compact_writer {
  queue_share_t *share;
  int fd;
  my_off_t off;
  vector<char> buf;
  int32_t adler;
  queue_compact_writer(queue_share_t *s, int f, my_off_t o)
  : share(s), fd(f), off(o), buf(), adler(1) {}
  bool flush() {
    if (sys_write(fd, &*buf.begin(), buf.size())
	!= static_cast<ssize_t>(buf.size())) {
      return false;
    }
    buf.clear();
    return true;
  }
  bool flush_if_necessary() {
    return buf.size() < 16384 ? true : flush();
  }
  bool append(const void *src, size_t len, const void *adler_src = NULL) {
    off += len;
    buf.insert(buf.end(), static_cast<const char*>(src),
	       static_cast<const char*>(src) + len);
    adler = adler32(adler, adler_src != NULL ? adler_src : src, len);
    return flush_if_necessary();
  }
  bool append_row_header(const queue_row_t *hdr) {
    queue_row_t tmp(hdr->size(), hdr->type());
    switch (hdr->type()) {
    case queue_row_t::type_row_removed:
      tmp.set_type(queue_row_t::type_row);
      break;
    case queue_row_t::type_row_received_removed:
      tmp.set_type(queue_row_t::type_row_received);
      break;
    }
    return append(hdr, queue_row_t::header_size(), &tmp);
  }
  bool append_rows_removed(size_t n) {
    queue_row_t hdr(n, queue_row_t::type_num_rows_removed);
    return append(&hdr, queue_row_t::header_size());
  }
  bool copy_data(my_off_t src, size_t sz) {
    off += sz;
    buf.resize(buf.size() + sz);
    if (share->read(&*buf.begin() + buf.size() - sz, src, sz)
	!= static_cast<ssize_t>(sz)) {
      return false;
    }
    adler = adler32(adler, &*buf.begin() + buf.size() - sz, sz);
    return flush_if_necessary();
  }
};

my_off_t queue_share_t::compact_do_copy(queue_compact_writer &writer, info_t* info, my_off_t *row_count)
{
  my_off_t off = info->_header.begin(), end_off = info->_header.end(),
    new_begin = 0, last_compact_check_off = 0, last_tmp_sync_off = 0;
  *row_count = 0;
  size_t rows_removed = 0;
  
  if (concurrent_compaction) {
    pthread_mutex_unlock(this->info.mutex());
  }
  
  while (off != end_off) {
    queue_row_t row;
    if (read(&row, off, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      log("file corrupt\n");
      goto ERROR;
    }
    switch (row.type()) {
    case queue_row_t::type_row:
    case queue_row_t::type_row_received:
      if (rows_removed != 0) {
	if (! writer.append_rows_removed(rows_removed)) {
	  log("I/O error\n");
	  goto ERROR;
	}
	rows_removed = 0;
      }
      if (new_begin == 0) {
	new_begin = writer.off;
      }
      if (concurrent_compaction) {
	pthread_mutex_lock(this->info.mutex());
      }
      if (info->rows_owned != NULL) {
	queue_connection_t *c = info->rows_owned;
	do {
	  if (c->owned_row_off == off) {
	    c->owned_row_off_post_compact = writer.off;
	  }
	} while ((c = c->next_owned()) != info->rows_owned);
      }
      if (concurrent_compaction) {
	pthread_mutex_unlock(this->info.mutex());
      }
      if (! writer.append_row_header(&row)
	  || ! writer.copy_data(off + queue_row_t::header_size(),
				row.size())) {
	log("I/O error\n");
	goto ERROR;
      }
      ++*row_count;
      break;
    case queue_row_t::type_row_removed:
    case queue_row_t::type_row_received_removed:
      rows_removed++;
      break;
    case queue_row_t::type_num_rows_removed:
      for (rows_removed += row.size();
	   rows_removed >= queue_row_t::max_size;
	   rows_removed -= queue_row_t::max_size) {
	if (! writer.append_rows_removed(queue_row_t::max_size)) {
	  log("I/O error\n");
	  goto ERROR;
	}
      }
      break;
    case queue_row_t::type_checksum:
      break;
    default:
      log("file corrupt\n");
      goto ERROR;
    }
    off = row.next(off);
    
    if (concurrent_compaction
	&& (last_compact_check_off + concurrent_compaction_interval
	    < writer.off)) {
      last_compact_check_off = writer.off;
      pthread_mutex_lock(this->info.mutex());
      append_list_t *al = NULL;
      pthread_cond_t *notify_cond = NULL;
      if (! info->append_list->empty()) {
	al = info->append_list;
	info->append_list = new append_list_t();
	notify_cond = info->append_response_cond;
	info->append_response_cond = info->_append_response_conds
	  + (info->_append_response_conds == notify_cond);
      }
      pthread_mutex_unlock(this->info.mutex());
      if (al != NULL) {
	int err = 0;
	if ((err = writer_do_append(al)) != 0) {
	  sync_file(fd);
	}
	close_append_list(al, err);
	pthread_cond_broadcast(notify_cond);
	writer_do_wake_listeners = true;
	if (err != 0) {
	  goto ERROR;
	}
	log("accepted commits during compaction, changing end from %llu to %llu\n (off: %llu, writer.off: %llu)\n",
	    end_off, info->_header.end(), off, writer.off);
	end_off = info->_header.end();
      }
      if (writer_do_wake_listeners && wake_listeners(true)) {
	writer_do_wake_listeners = false;
      }
      if (last_tmp_sync_off + 10485760 < writer.off) {
	last_tmp_sync_off = writer.off;
	sync_file(writer.fd);
      }
    }
  }
  
  if (rows_removed != 0 && ! writer.append_rows_removed(rows_removed)) {
    log("I/O error\n");
    goto ERROR;
  }
  writer.flush();
  
  if (concurrent_compaction) {
    pthread_mutex_lock(this->info.mutex());
    assert(end_off == info->_header.end());
  }
  
  return new_begin;
 ERROR:
  if (concurrent_compaction) {
    pthread_mutex_lock(this->info.mutex());
  }
  return -1;
}

int queue_share_t::compact(info_t *info)
{
  log("starting table compaction: %s (begin: %llu, end: %llu, rows: %llu)\n",
      table_name, info->_header.begin(), info->_header.end(),
      info->_header.row_count());
  
  char filename[FN_REFLEN], tmp_filename[FN_REFLEN];
  int tmp_fd;
  queue_file_header_t tmp_hdr;
  tmp_hdr.set_attr(tmp_hdr.attr() | queue_file_header_t::attr_is_dirty);
  
  /* reset owned_row_off_post_compact */
  if (info->rows_owned != NULL) {
    queue_connection_t *c = info->rows_owned;
    do {
      c->owned_row_off_post_compact = 0;
    } while ((c = c->next_owned()) != info->rows_owned);
  }
  /* open new file */
  fn_format(filename, table_name, "", Q4M,
	    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  fn_format(tmp_filename, table_name, "", Q4T,
	    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  if ((tmp_fd = open(tmp_filename, O_CREAT | O_TRUNC | O_RDWR | O_LARGEFILE,
		     0660))
      == -1) {
    log("failed to create temporary file: %s\n", tmp_filename);
    goto ERR_RETURN;
  }
  { 
    queue_compact_writer writer(this, tmp_fd,
				sizeof(tmp_hdr) + queue_row_t::checksum_size());
    my_off_t new_begin, row_count;
    /* write content to new file */
    if (lseek(tmp_fd, sizeof(tmp_hdr) + queue_row_t::checksum_size(), SEEK_SET)
	== -1) {
      goto ERR_OPEN;
    }
    if ((new_begin = compact_do_copy(writer, info, &row_count)) == (my_off_t)-1) {
      goto ERR_OPEN;
    }
    /* adjust write position if file is empty */
    if (writer.off
	== sizeof(queue_file_header_t) + queue_row_t::checksum_size()) {
      writer.off = sizeof(queue_file_header_t);
      if (lseek(tmp_fd, sizeof(queue_file_header_t), SEEK_SET) == -1) {
	goto ERR_OPEN;
      }
    }
    tmp_hdr.set_begin(max((my_off_t)sizeof(queue_file_header_t), new_begin),
		      info->_header.begin_row_id());
    tmp_hdr.set_end(writer.off);
    tmp_hdr.set_row_count(row_count);
    for (int i = 0; i < QUEUE_MAX_SOURCES; i++) {
      tmp_hdr.set_last_received_offset(i,
				       info->_header.last_received_offset(i));
    }
    if (sys_pwrite(tmp_fd, &tmp_hdr, sizeof(tmp_hdr), 0) != sizeof(tmp_hdr)) {
      goto ERR_OPEN;
    }
    /* write checksum */
    if (writer.off != sizeof(queue_file_header_t)) {
      char cbuf[queue_row_t::checksum_size()];
      queue_row_t::create_checksum(reinterpret_cast<queue_row_t*>(cbuf),
				   writer.off - sizeof(queue_file_header_t)
				   - queue_row_t::checksum_size(),
				   writer.adler);
      if (sys_pwrite(tmp_fd, cbuf, sizeof(cbuf), sizeof(tmp_hdr))
	  != static_cast<ssize_t>(sizeof(cbuf))) {
	goto ERR_OPEN;
      }
    }
    /* adjust file size */
    if (ftruncate(tmp_fd, (writer.off + EXPAND_BY - 1) / EXPAND_BY * EXPAND_BY)
	!= 0) {
      goto ERR_OPEN;
    }
    /* sync */
    sync_file(tmp_fd);
  }
  /* rename */
  if (info->is_deleting) {
    if (unlink(tmp_filename) != 0) {
      log("failed to unlink file: %s\n", tmp_filename);
      goto ERR_OPEN;
    }
  } else {
    if (rename(tmp_filename, filename) != 0) {
      log("failed to rename (2): %s => %s\n", tmp_filename, filename);
      goto ERR_OPEN;
    }
  }
  // is the directory entry synced with fsync?
  sync_file(tmp_fd);
  /* replace fd and mmap */
  close(fd);
  fd = tmp_fd;
#ifdef Q4M_USE_MMAP
  if (mmap_table(min((tmp_hdr.end() + EXPAND_BY - 1) / EXPAND_BY * EXPAND_BY,
		     mmap_max))
      != 0) {
    log("mmap failed: size=%lu\n",
	static_cast<unsigned long>(map.unsafe_ref()->len));
  }
#endif
  /* update internal info */
  info->_header = tmp_hdr;
  info->max_owned_row_off = 0;
  if (info->rows_owned != NULL) {
    queue_connection_t *c = info->rows_owned;
    do {
      if (c->owned_row_off_post_compact != 0) {
	c->owned_row_off = c->owned_row_off_post_compact;
	info->max_owned_row_off =
	  max(info->max_owned_row_off, c->owned_row_off);
	c = c->next_owned();
      } else {
	queue_connection_t *n = c->next_owned();
	c->owned_row_off = 0;
	c->remove_from_owned_list(info->rows_owned);
	c = n;
      }
    } while (c != info->rows_owned);
  }
  apply_cond_exprs(info, cond_expr_reset_pos_t());
  
  log("finished table compaction: %s (begin: %llu, end: %llu, rows: %llu)\n",
      table_name, info->_header.begin(), info->_header.end(),
      info->_header.row_count());
  return 0;
    
 ERR_OPEN:
  close(tmp_fd);
  unlink(tmp_filename);
 ERR_RETURN:
  return -1;
}

size_t queue_connection_t::cnt = 0;

queue_connection_t *queue_connection_t::current(bool create_if_empty)
{
  queue_connection_t *conn =
    static_cast<queue_connection_t*>(thd_get_ha_data(current_thd, queue_hton));
  
  if (conn == NULL && create_if_empty) {
    conn = new queue_connection_t();
    thd_set_ha_data(current_thd, queue_hton, conn);
    cnt++;
  }
  return conn;
}

int queue_connection_t::close(handlerton *hton, THD *thd)
{
  queue_connection_t *conn =
    static_cast<queue_connection_t*>(thd_get_ha_data(current_thd, queue_hton));
  
  if (conn->share_owned != NULL) {
    if (conn->share_owned->reset_owner(conn) != 0) {
      conn->share_owned->wake_listeners();
      ++cac_mutex_t<queue_share_t::stats_t>::lockref(*conn->share_owned->stats)
	->close_cnt;
    }
    conn->share_owned->release();
  }
  delete conn;
  --cnt;
  
  return 0;
}

void queue_connection_t::erase_owned()
{
  if (share_owned != NULL) {
    share_owned->lock_reader();
    if (owned_row_off != 0) {
      share_owned->remove_rows(&owned_row_off, 1);
    }
    share_owned->remove_owner(this);
    share_owned->unlock_reader();
    share_owned->release();
    share_owned = NULL;
    owned_row_off = 0;
    owned_row_id = 0;
  }
  owner_mode = false;
}

ha_queue::ha_queue(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share(NULL),
   pos(),
   rows(NULL),
   rows_size(0),
   rows_reserved(0),
   bulk_insert_rows(static_cast<size_t>(-1)),
   bulk_delete_rows(NULL),
   defer_reader_lock(false)
{
  assert(ref_length == sizeof(my_off_t));
}

ha_queue::~ha_queue()
{
  assert(share == NULL);
  delete bulk_delete_rows;
  bulk_delete_rows = NULL;
  free_rows_buffer(true);
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
    return HA_ERR_CRASHED_ON_USAGE;
  }
  share->init_fixed_fields(cac_mutex_t<queue_share_t::info_t>::lockref(share->info), table);
  thr_lock_data_init(share->get_store_lock(), &lock, NULL);
  
  return 0;
}

int ha_queue::close()
{
  assert(share != NULL);
  share->release();
  share = NULL;
  
  return 0;
}

int ha_queue::external_lock(THD *thd, int lock_type)
{
  switch (lock_type) {
  case F_RDLCK:
  case F_WRLCK:
    defer_reader_lock = true;
    break;
  case F_UNLCK:
    if (! defer_reader_lock) {
      share->unlock_reader();
    }
    defer_reader_lock = false;
    free_rows_buffer();
    break;
  default:
    break;
  }
  return 0;
}

int ha_queue::rnd_init(bool scan)
{
  pos = 0;
  return 0;
}

int ha_queue::rnd_end()
{
  return 0;
}

int ha_queue::rnd_next(uchar *buf)
{
  assert(rows_size == 0);
  
  int err = HA_ERR_END_OF_FILE;
  
  if (defer_reader_lock) {
    share->lock_reader();
    defer_reader_lock = false;
  }
  
  queue_connection_t *conn;
  if ((conn = queue_connection_t::current()) != NULL && conn->owner_mode) {
    if (pos == 0 && conn->share_owned == share
	&& (pos = conn->owned_row_off) != 0) {
      // ok
    } else {
      goto EXIT;
    }
  } else {
    cac_mutex_t<queue_share_t::info_t>::lockref info(share->info);
    if (pos == 0) {
      if ((pos = info->_header.begin()) == info->_header.end()) {
	goto EXIT;
      }
    } else {
      if (share->next(&pos, NULL) != 0) {
	err = HA_ERR_CRASHED_ON_USAGE;
	goto EXIT;
      } else if (pos == info->_header.end()) {
	goto EXIT;
      }
    }
    while (share->find_owner(info, pos) != 0) {
      if (share->next(&pos, NULL) != 0) {
	err = HA_ERR_CRASHED_ON_USAGE;
	goto EXIT;
      }
      if (pos == info->_header.end()) {
	goto EXIT;
      }
    }
  }
  
  { /* read data to row buffer */
    queue_row_t hdr;
    if (share->read(&hdr, pos, queue_row_t::header_size())
	!= static_cast<ssize_t>(queue_row_t::header_size())) {
      err = HA_ERR_CRASHED_ON_USAGE;
      goto EXIT;
    }
    switch (hdr.type()) {
    case queue_row_t::type_row_removed:
    case queue_row_t::type_row_received_removed:
      /* owned row removed by owner */
      goto EXIT;
    }
    if (prepare_rows_buffer(queue_row_t::header_size() + hdr.size()) != 0) {
      err = HA_ERR_OUT_OF_MEM;
      goto EXIT;
    }
    if (share->read(rows, pos, queue_row_t::header_size() + hdr.size())
	!= static_cast<ssize_t>(queue_row_t::header_size() + hdr.size())) {
      err = HA_ERR_CRASHED_ON_USAGE;
      goto EXIT;
    }
  }
  
  /* unlock and convert to internal representation */
  unpack_row(buf);
  table->status = 0;
  return 0;
  
 EXIT:
  table->status = STATUS_NOT_FOUND;
  return err;
}

void ha_queue::position(const uchar *record)
{
  my_store_ptr(ref, ref_length, pos);
}

int ha_queue::rnd_pos(uchar *buf, uchar *_pos)
{
  assert(rows_size == 0);
  
  pos = my_get_ptr(_pos, ref_length);
  
  /* we should return the row even if it had the deleted flag set during the
   * execution by other threads
   */
  queue_row_t hdr;
  if (share->read(&hdr, pos, queue_row_t::header_size())
      != static_cast<ssize_t>(queue_row_t::header_size())) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  if (prepare_rows_buffer(queue_row_t::header_size() + hdr.size()) != 0) {
    return HA_ERR_OUT_OF_MEM;
  }
  if (share->read(rows, pos, hdr.size()) != static_cast<ssize_t>(hdr.size())) {
    return HA_ERR_CRASHED_ON_USAGE;
  }
  
  unpack_row(buf);
  return 0;
}

int ha_queue::info(uint flag)
{
  stats.records = records();
  return 0;
}

ha_rows ha_queue::records()
{
  queue_connection_t *conn;
  my_off_t rc;
  
  if ((conn = queue_connection_t::current()) != NULL && conn->owner_mode) {
    rc = 0;
    if (conn->share_owned == share) {
      share->lock_reader();
      if (conn->owned_row_off != 0) {
	queue_row_t hdr;
	if (share->read(&hdr, conn->owned_row_off, queue_row_t::header_size())
	    == static_cast<ssize_t>(queue_row_t::header_size())) {
	  switch (hdr.type()) {
	  case queue_row_t::type_row:
	  case queue_row_t::type_row_received:
	    rc = 1;
	    break;
	  default:
	    break;
	  }
	}
      }
      share->unlock_reader();
    }
  } else {
    rc = cac_mutex_t<queue_share_t::info_t>::lockref(share->info)->_header.row_count();
  }
  
  return rc;
}

THR_LOCK_DATA **ha_queue::store_lock(THD *thd, THR_LOCK_DATA **to,
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
  
  queue_share_t* old_share = queue_share_t::get_share(name, false);
  
  /* set is_deleting flag to prevent the compaction thread from overriting .Q4M
   */
  if (old_share != NULL) {
    cac_mutex_t<queue_share_t::info_t>::lockref info(old_share->info);
    info->is_deleting = true;
  }
  
  /* unlink existing file (if exists, is the case for TRUNCATE TABLE) instead
   * of using O_TRUNC so that already-establised connections can still refer
   * to the data (NOTE: the call to this function is serialized by LOCK_open)
   */
  if (! (unlink(filename) == 0 || errno == ENOENT)) {
    log("failed to unlink file: %s\n", filename);
    return HA_ERR_GENERIC; // ????
  }
  if ((fd = ::open(filename, O_WRONLY | O_CREAT | O_EXCL | O_LARGEFILE, 0660))
      == -1) {
    return HA_ERR_GENERIC; // ????
  }
  queue_file_header_t header;
  if (sys_write(fd, &header, sizeof(header)) != sizeof(header)) {
    goto ERROR;
  }
  if (lseek(fd, EXPAND_BY - 1, SEEK_SET) == -1
      || sys_write(fd, "", 1) != 1) {
    goto ERROR;
  }
  sync_file(fd);
  ::close(fd);
  
  /* close the existing share that holds the descriptor pointing to the old file
   * (or the share will persist until the number of owning connections become
   * zero at some point)
   */
  if (old_share != NULL) {
    old_share->detach();
    old_share->release();
  }
  
  return 0;
  
 ERROR:
  ::close(fd);
  unlink(filename);
  return HA_ERR_RECORD_FILE_FULL;
}

void ha_queue::start_bulk_insert(ha_rows rows __attribute__((unused)))
{
  assert(rows_size == 0);
  assert(bulk_insert_rows == static_cast<size_t>(-1));
  bulk_insert_rows = 0;
}

int ha_queue::end_bulk_insert()
{
  int ret = 0;
  
  if (rows_size != 0) {
    ret = share->write_rows(rows, rows_size, bulk_insert_rows);
    switch (ret) {
    case QUEUE_ERR_RECORD_EXISTS:
      ret = 0;
      break;
    case 0:
      stat_rows_written.incr(bulk_insert_rows);
      break;
    }
    rows_size = 0;
  }
  free_rows_buffer();
  bulk_insert_rows = static_cast<size_t>(-1);
  
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
    ret =
      share->remove_rows(&bulk_delete_rows->front(), bulk_delete_rows->size());
  }
  delete bulk_delete_rows;
  bulk_delete_rows = NULL;
  
  return ret;
}

int ha_queue::write_row(uchar *buf)
{
  queue_connection_t *conn = queue_connection_t::current();
  size_t sz;
  
  if (conn != NULL && conn->source.offset() != 0) {
    sz = pack_row(buf, &conn->source);
  } else {
    sz = pack_row(buf, NULL);
  }
  if (sz == 0) {
    return HA_ERR_OUT_OF_MEM;
  }
  if (bulk_insert_rows == static_cast<size_t>(-1)) {
    int err = share->write_rows(rows, sz, 1);
    free_rows_buffer();
    switch (err) {
    case 0:
      stat_rows_written.incr();
      break;
    case QUEUE_ERR_RECORD_EXISTS:
      err = 0;
      break;
    default:
      return err;
    }
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
  
  if (bulk_delete_rows != NULL) {
    bulk_delete_rows->push_back(pos);
  } else {
    share->lock_reader();
    err = share->remove_rows(&pos, 1);
    share->unlock_reader();
  }
  
  return err;
}

int ha_queue::delete_table(const char *name)
{
  if (share != NULL || (share = queue_share_t::get_share(name)) != NULL) {
    {
      cac_mutex_t<queue_share_t::info_t>::lockref info(share->info);
      info->is_deleting = true;
    }
    share->detach();
    share->release();
    share = NULL;
  }
  get_stats_for(name, true);
  return handler::delete_table(name);
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

void ha_queue::free_rows_buffer(bool force)
{
  if (! force && rows_size < FREE_ROWS_BUFFER_SIZE) {
    return;
  }
  if (rows != NULL) {
    my_free(rows, MYF(0));
    rows = NULL;
    rows_size = 0;
  }
}

void ha_queue::unpack_row(uchar *buf)
{
  const uchar *src = rows + queue_row_t::header_size();
  Field **field;
  queue_fixed_field_t * const * fixed;
  
  memcpy(buf, src, table->s->null_bytes);
  src += table->s->null_bytes;
  for (field = table->field, fixed = share->get_fixed_fields();
       *field != NULL;
       field++, fixed++) {
    if (*fixed != NULL && ! (*field)->is_null()) {
      src = (*field)->unpack(buf + (*field)->offset(table->record[0]), src);
    }
  }
  for (field = table->field, fixed = share->get_fixed_fields();
       *field != NULL;
       field++, fixed++) {
    if (*fixed == NULL && ! (*field)->is_null()) {
      src = (*field)->unpack(buf + (*field)->offset(table->record[0]), src);
    }
  }
}

size_t ha_queue::pack_row(uchar *buf, queue_source_t *source)
{
  /* allocate memory (w. some extra) */
  size_t sz = queue_row_t::header_size() + table->s->reclength
    + table->s->fields * 2;
  if (source != NULL) {
    sz += sizeof(*source);
  }
  for (uint *ptr = table->s->blob_field, *end = ptr + table->s->blob_fields;
       ptr != end;
       ++ptr) {
    sz += 2 + ((Field_blob*)table->field[*ptr])->get_length();
  }
  if (sz > queue_row_t::max_size || prepare_rows_buffer(sz) != 0) {
    return 0;
  }
  /* write data */
  uchar *dst = rows + rows_size + queue_row_t::header_size();
  memcpy(dst, buf, table->s->null_bytes);
  dst += table->s->null_bytes;
  Field **field;
  queue_fixed_field_t * const *fixed;
  for (field = table->field, fixed = share->get_fixed_fields();
       *field != NULL;
       field++, fixed++) {
    if (*fixed != NULL && ! (*field)->is_null()) {
      dst = (*field)->pack(dst, buf + (*field)->offset(buf));
    }
  }
  for (field = table->field, fixed = share->get_fixed_fields();
       *field != NULL;
       field++, fixed++) {
    if (*fixed == NULL && ! (*field)->is_null()) {
      dst = (*field)->pack(dst, buf + (*field)->offset(buf));
    }
  }
  /* write source */
  if (source != NULL) {
    memcpy(dst, source, sizeof(*source));
    dst += sizeof(*source);
  }
  /* write header */
  sz = dst - (rows + rows_size);
  new (reinterpret_cast<queue_row_t*>(rows + rows_size))
    queue_row_t(sz - queue_row_t::header_size(), queue_row_t::type_row);
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
  const char *db, *tbl;
  
  // FIXME: creates bogus name if db_table_name is too long (but no overruns)
  if ((tbl = strchr(db_table_name, '.')) != NULL) {
    size_t db_len = min(static_cast<size_t>(tbl - db_table_name),
			sizeof(buf) - 1);
    memcpy(buf, db_table_name, db_len);
    buf[db_len] = '\0';
    db = buf;
    tbl = tbl + 1;
  } else {
    db = current_thd->db;
    tbl = db_table_name;
  }
  if (db == NULL) {
    return NULL;
  }
  
  build_table_filename(path, FN_REFLEN - 1, db, tbl, "", 0);
  queue_share_t *share = queue_share_t::get_share(path);
  if (share != NULL && ! share->init_fixed_fields()) {
    log("failed to initialize fixed field info.\n");
    share->release();
    share = NULL;
  }
  return share;
}

static bool show_engine_status(handlerton *hton, THD *thd, stat_print_fn *print)
{
  vector<char> out;
  char buf[256];
  
  // dump stat values
  pthread_mutex_lock(&stat_mutex);

#define SEP "\n------------------------------------\n"
#define HEADER(N) do { \
    const char *sep = SEP, * n = N; \
    out.push_back('\n'); \
    out.insert(out.end(), n, n + sizeof(N) - 1); \
    out.insert(out.end(), sep, sep + sizeof(SEP) - 1); \
  } while (0)
#define DUMP2(n, l) do { \
    sprintf(buf, "%-16s %20llu\n", l, stat_##n.value); \
    out.insert(out.end(), buf, buf + strlen(buf)); \
  } while (0)
#define DUMP(n) DUMP2(n, #n)
  
  HEADER("I/O calls");
  DUMP(sys_read);
  DUMP(sys_write);
  DUMP(sys_sync);
  DUMP(read_cachehit);
  HEADER("Writer thread");
  DUMP2(writer_append, "append");
  DUMP2(writer_remove, "remove");
  HEADER("Conditional subscription");
  DUMP2(cond_eval, "evaluation");
  DUMP2(cond_compile, "compile");
  DUMP2(cond_compile_cachehit, "compile_cachehit");
  HEADER("High-level stats");
  DUMP(rows_written);
  DUMP(rows_removed);
  DUMP(queue_wait);
  DUMP(queue_end);
  DUMP(queue_abort);
  DUMP(queue_rowid);
  DUMP(queue_set_srcid);
  
#undef SEP
#undef HEADER
#undef DUMP2
#undef DUMP
  
  pthread_mutex_unlock(&stat_mutex);
  
  return (*print)(thd, "QUEUE", 5, "", 0, &*out.begin(), out.size());
}

static bool show_status(handlerton *hton, THD *thd, stat_print_fn *print,
			enum ha_stat_type stat)
{
  switch (stat) {
  case HA_ENGINE_STATUS:
    return show_engine_status(hton, thd, print);
  default:
    return FALSE;
  }
}

static int init_plugin(void *p)
{
  queue_hton = (handlerton *)p;
  
  pthread_mutex_init(&open_mutex, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&listener_mutex, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&tbl_stat_mutex, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&stat_mutex, MY_MUTEX_INIT_FAST);
  my_hash_init(&queue_open_tables, system_charset_info, 32, 0, 0,
	    reinterpret_cast<my_hash_get_key>(queue_share_t::get_share_key), 0, 0);
  queue_hton->state = SHOW_OPTION_YES;
  queue_hton->close_connection = queue_connection_t::close;
  queue_hton->create = create_handler;
  queue_hton->show_status = show_status;
  queue_hton->flags = HTON_CAN_RECREATE;
  
  return 0;
}

static int deinit_plugin(void *p)
{
  if (queue_connection_t::cnt != 0) {
    // FIXME: what is the appropriate error code to return busy status
    return HA_ERR_GENERIC;
  }
  
  my_hash_free(&queue_open_tables);
  pthread_mutex_destroy(&stat_mutex);
  pthread_mutex_destroy(&tbl_stat_mutex);
  pthread_mutex_destroy(&listener_mutex);
  pthread_mutex_destroy(&open_mutex);
  queue_hton = NULL;
  
  return 0;
}

struct st_mysql_storage_engine queue_storage_engine = {
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

static struct st_mysql_sys_var *queue_plugin_vars[] = {
  MYSQL_SYSVAR(mmap_max),
  MYSQL_SYSVAR(use_concurrent_compaction),
  MYSQL_SYSVAR(concurrent_compaction_interval),
  NULL
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
  Q4M_VERSION_HEX,
  NULL,                       /* status variables                */
  queue_plugin_vars,
  NULL                        /* config options                  */
}
mysql_declare_plugin_end;

class share_lock_t : private dllist<share_lock_t> {
  friend class dllist<share_lock_t>;
  queue_share_t *share;
  size_t cnt;
  cac_mutex_t<queue_share_t::info_t>::lockref info;
  share_lock_t(); // never called
  ~share_lock_t() {}
public:
  static cac_mutex_t<queue_share_t::info_t>::lockref *
  lock(share_lock_t *&locks, queue_share_t *share, share_lock_t *&locks_buf) {
    share_lock_t *l = locks;
    if (l != NULL) {
      do {
	if (l->share == share) {
	  goto FOUND;
	}
      } while ((l = l->next()) != locks);
    }
    l = locks_buf++;
    new (l) dllist<share_lock_t>();
    l->share = share;
    l->cnt = 0;
    l->attach_front(locks);
  FOUND:
    if (l->cnt++ == 0) {
      if (! share->lock_reader(true)) {
	--l->cnt;
	return NULL;
      }
      new (&l->info) cac_mutex_t<queue_share_t::info_t>::lockref(share->info);
    }
    return &l->info;
  }
  static void unlock(share_lock_t *&locks, queue_share_t *share) {
    assert(locks != NULL);
    share_lock_t *l = locks;
    do {
      if (l->share == share) {
	assert(l->cnt > 0);
	if (--l->cnt == 0) {
	  l->info.~lockref();
	  l->share->unlock_reader(true);
	}
	break;
      }
    } while ((l = l->next()) != locks);
  }
};
  
static int _queue_wait_core(char **share_names, int num_shares, int timeout,
			    char *error)
{
  queue_share_t **shares;
  cac_mutex_t<queue_share_t::info_t>::lockref **share_infos;
  share_lock_t *locks = NULL;
  share_lock_t *locks_buf;
  queue_share_t::cond_expr_t **cond_exprs;
  int share_owned = -1;
  queue_connection_t *conn = queue_connection_t::current(true);
  
  *error = 0;
  
  conn->erase_owned();
  
  /* setup */
  shares = static_cast<queue_share_t**>(sql_alloc(num_shares * sizeof(queue_share_t*)));
  memset(shares, 0, num_shares * sizeof(queue_share_t*));
  share_infos = static_cast<cac_mutex_t<queue_share_t::info_t>::lockref**>(sql_alloc(num_shares * sizeof(cac_mutex_t<queue_share_t::info_t>::lockref*)));
  locks_buf = static_cast<share_lock_t*>(sql_alloc(num_shares * sizeof(share_lock_t)));
  cond_exprs = static_cast<queue_share_t::cond_expr_t**>(sql_alloc(num_shares * sizeof(queue_share_t::cond_expr_t*)));
  memset(cond_exprs, 0, num_shares * sizeof(queue_share_t::cond_expr_t*));
  
  /* setup, or immediately break if data found, note that locks for the tables
   * are not released until all the tables are scanned, in order NOT to return
   * a row of a lower-priority table */
  for (int i = 0; i < num_shares; i++) {
    const char *last = strchr(share_names[i], ':');
    if (last == NULL) {
      last = share_names[i] + strlen(share_names[i]);
    }
    if (last - share_names[i] > FN_REFLEN * 2) {
      log("table name too long: %s\n", share_names[i]);
      *error = 1;
      break;
    }
    char db_table_name[FN_REFLEN * 2 + 1];
    memcpy(db_table_name, share_names[i], last - share_names[i]);
    db_table_name[last - share_names[i]] = '\0';
    if ((shares[i] = get_share_check(db_table_name)) == NULL) {
      log("could not find table: %s\n", db_table_name);
      *error = 1;
      break;
    }
    if ((share_infos[i] = share_lock_t::lock(locks, shares[i], locks_buf))
	== NULL) {
      log("detected misuse of queue_wait(), returning error\n");
      shares[i]->release();
      shares[i] = NULL;
      *error = 1;
      break;
    }
    if ((cond_exprs[i] =
	 *last != '\0'
	 ? shares[i]->compile_cond_expr(*share_infos[i], last + 1,
					strlen(last + 1))
	 : shares[i]->compile_cond_expr(*share_infos[i], NULL, 0))
	== NULL) {
      log("failed to compile expression: %s\n", share_names[i]);
      *error = 1;
      break;
    }
    if (shares[i]->assign_owner(*share_infos[i], conn, cond_exprs[i]) != 0) {
      share_owned = i;
      ++cac_mutex_t<queue_share_t::stats_t>::lockref(*shares[i]->stats)
	->wait_immediate_cnt;
      break;
    }
  }
  for (int i = 0; i < num_shares && shares[i] != NULL; i++) {
    share_lock_t::unlock(locks, shares[i]);
  }
  if (*error != 0) {
    goto EXIT;
  }
  if (share_owned == -1) {
    /* not yet found, lock global mutex and check once more */
    pthread_mutex_lock(&listener_mutex);
    for (int i = 0; i < num_shares; i++) {
      share_infos[i] = share_lock_t::lock(locks, shares[i], locks_buf);
      if (shares[i]->assign_owner(*share_infos[i], conn, cond_exprs[i]) != 0) {
	for (int j = 0; j <= i; j++) {
	  share_lock_t::unlock(locks, shares[j]);
	}
	share_owned = i;
	++cac_mutex_t<queue_share_t::stats_t>::lockref(*shares[i]->stats)
	  ->wait_immediate_cnt;
	break;
      }
    }
    /* if not yet found, wait for data */
    if (share_owned == -1) {
      queue_share_t::listener_t listener(conn);
      for (int i = 0; i < num_shares; i++) {
	shares[i]->register_listener(&listener, cond_exprs[i], i);
	share_lock_t::unlock(locks, shares[i]);
      }
      timedwait_cond(&listener.cond, &listener_mutex, timeout * 1000);
      share_owned = listener.queue_wait_index;
      assert(share_owned == -1 || shares[share_owned] == conn->share_owned);
      for (int i = 0; i < num_shares; i++) {
	if (i != share_owned) {
	  shares[i]->unregister_listener(&listener);
	}
      }
      if (share_owned != -1) {
	++cac_mutex_t<queue_share_t::stats_t>::lockref(*shares[share_owned]
						       ->stats)
	  ->wait_delayed_cnt;
      }
    }
    pthread_mutex_unlock(&listener_mutex);
  }
  /* always enter owner-mode, regardless whether or not we own a row */
  conn->owner_mode = true;
  
 EXIT:
  for (int i = 0; i < num_shares && cond_exprs[i] != NULL; i++) {
    shares[i]->release_cond_expr(cond_exprs[i]);
  }
  for (int i = 0; i < num_shares && shares[i] != NULL; i++) {
    if (i != share_owned) {
      ++cac_mutex_t<queue_share_t::stats_t>::lockref(*shares[i]->stats)
	->wait_timeout_cnt;
      shares[i]->release();
    }
  }
  return share_owned;
}

my_bool queue_wait_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count == 0) {
    strcpy(message, "queue_wait(table_name[,timeout]): argument error");
    return 1;
  } else if (args->arg_count >= 2) {
    args->arg_type[args->arg_count - 1] = INT_RESULT;
    args->maybe_null[args->arg_count - 1] = 0;
  }
  for (int i = max((int)args->arg_count - 2, 0); i >= 0; i--) {
    args->arg_type[i] = STRING_RESULT;
    args->maybe_null[i] = 0;
  }
  initid->maybe_null = 0;
  
  return 0;
}

void queue_wait_deinit(UDF_INIT *initid __attribute__((unused)))
{
}

long long queue_wait(UDF_INIT *initid __attribute__((unused)), UDF_ARGS *args,
		     char *is_null, char *error)
{
  stat_queue_wait.incr();
  int timeout = args->arg_count >= 2
    ? *reinterpret_cast<long long*>(args->args[args->arg_count - 1]) : 60;
  
  *is_null = 0;
  return
    _queue_wait_core(args->args, max((int)args->arg_count - 1, 1), timeout,
                     error)
    + 1;
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
  stat_queue_end.incr();
  queue_connection_t *conn;
  
  if ((conn = queue_connection_t::current()) != NULL) {
    conn->erase_owned();
  }
  
  *is_null = 0;
  return 1;
}

my_bool queue_abort_init(UDF_INIT *initid,
			UDF_ARGS *args __attribute__((unused)),
			char *message)
{
  queue_connection_t *conn;
  if ((conn = queue_connection_t::current()) == NULL || ! conn->owner_mode) {
    strcpy(message, "queue_abort(): not in owner mode");
    return 1;
  }
  initid->maybe_null = 0;
  return 0;
}

void queue_abort_deinit(UDF_INIT *initid)
{
}

long long queue_abort(UDF_INIT *initid, UDF_ARGS *args __attribute__((unused)),
		      char *is_null, char *error)
{
  stat_queue_abort.incr();
  queue_connection_t *conn;
  
  if ((conn = queue_connection_t::current()) != NULL) {
    if (conn->share_owned != NULL) {
      if (conn->share_owned->reset_owner(conn) != 0) {
	conn->share_owned->wake_listeners();
	++cac_mutex_t<queue_share_t::stats_t>::lockref(*conn->share_owned
						       ->stats)
	  ->abort_cnt;
      }
      conn->share_owned->release();
      conn->share_owned = NULL;
    }
    conn->owner_mode = false;
  }
  
  *is_null = 0;
  return 1;
}

my_bool queue_rowid_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 0) {
    strcpy(message, "queue_rowid(): argument error");
    return 1;
  }
  queue_connection_t *conn;
  if ((conn = queue_connection_t::current()) == NULL || ! conn->owner_mode) {
    strcpy(message, "queue_rowid(): not in owner mode");
    return 1;
  }
  initid->maybe_null = 1;
  return 0;
}

void queue_rowid_deinit(UDF_INIT *initid __attribute__((unused)))
{
}

long long queue_rowid(UDF_INIT *initid __attribute__((unused)), UDF_ARGS *args,
		      char *is_null, char *error)
{
  stat_queue_rowid.incr();
  queue_connection_t *conn;
  if ((conn = queue_connection_t::current()) == NULL) {
    log("internal error, unexpectedly conn==NULL\n");
    *error = 1;
    return 0;
  }
  queue_share_t *share;
  if (! conn->owner_mode || (share = conn->share_owned) == NULL) {
    *is_null = 1;
    return 0;
  }
  return static_cast<long long>(conn->owned_row_id);
}

my_bool queue_set_srcid_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 3) {
    strcpy(message, "queue_set_srcid(source,mode,rowid): argument error");
    return 1;
  }
  args->arg_type[0] = INT_RESULT;
  args->maybe_null[0] = 0;
  args->arg_type[1] = STRING_RESULT;
  args->maybe_null[1] = 0;
  args->arg_type[2] = INT_RESULT;
  args->maybe_null[2] = 0;
  initid->maybe_null = 0;
  return 0;
}

void queue_set_srcid_deinit(UDF_INIT *initid __attribute__((unused)))
{
}

long long queue_set_srcid(UDF_INIT *initid __attribute__((unused)),
			  UDF_ARGS *args, char *is_null __attribute__((unused)),
			  char *error)
{
  stat_queue_set_srcid.incr();
  long long sender = *(long long*)args->args[0];
  if (sender < 0 || QUEUE_MAX_SOURCES <= sender) {
    log("queue_set_srcid: source number exceeds limit: %lld\n", sender);
    *error = 1;
    return 0;
  }
  queue_connection_t *conn = queue_connection_t::current(true);
  if (strcmp(args->args[1], "a") == 0) {
    conn->reset_source = false;
  } else if (strcmp(args->args[1], "w") == 0) {
    conn->reset_source = true;
  } else {
    log("queue_set_srcid: invalid mode: %s\n", args->args[1]);
    *error = 1;
    return 0;
  }
  conn->source = queue_source_t(sender, *(long long*)args->args[2]);
  return 1;
}

my_bool queue_compact_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1) {
    strcpy(message, "queue_compact(table_name): argument error");
    return 1;
  }
  args->arg_type[0] = STRING_RESULT;
  args->maybe_null[0] = 0;
  initid->maybe_null = 0;
  return 0;
}

void queue_compact_deinit(UDF_INIT *initid __attribute__((unused)))
{
}

long long queue_compact(UDF_INIT *initid __attribute__((unused)),
			UDF_ARGS *args, char *is_null __attribute__((unused)),
			char *error)
{
  queue_share_t *share;
  if ((share = get_share_check(args->args[0])) == NULL) {
    log("could not find table: %s\n", args->args[0]);
    *error = 1;
    return 0;
  }
  share->lock_reader();
  share->unlock_reader(false, true);
  share->release();
  return 1;
}

my_bool queue_stats_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1) {
    strcpy(message, "queue_stats(table_name): argument error");
    return 1;
  }
  args->arg_type[0] = STRING_RESULT;
  args->maybe_null[0] = 0;
  initid->maybe_null = 0;
  initid->ptr = (char*)malloc(4096);
  return 0;
}

void queue_stats_deinit(UDF_INIT *initid __attribute__((unused)))
{
  free(initid->ptr);
}

char* queue_stats(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error)
{
  queue_share_t *share;
  if ((share = get_share_check(args->args[0])) == NULL) {
    log("could not find table: %s\n", args->args[0]);
    *error = 1;
    return NULL;
  }
  my_off_t rows_written;
  my_off_t rows_removed;
  {
    cac_mutex_t<queue_share_t::info_t>::lockref info(share->info);
    rows_written = info->rows_written;
    rows_removed = info->rows_removed;
  }
  queue_share_t::stats_t stats =
    *cac_mutex_t<queue_share_t::stats_t>::lockref(*share->stats);
  sprintf(initid->ptr,
	  "rows_written: %llu\n"
	  "rows_removed: %llu\n"
	  "wait_immediate: %llu\n"
	  "wait_delayed: %llu\n"
	  "wait_timeout: %llu\n"
	  "restored_by_abort: %llu\n"
	  "restored_by_close: %llu\n",
	  rows_written,
	  rows_removed,
	  stats.wait_immediate_cnt,
	  stats.wait_delayed_cnt,
	  stats.wait_timeout_cnt,
	  stats.abort_cnt,
	  stats.close_cnt);
  share->release();
  *length = strlen(initid->ptr);
  *is_null = 0;
  return initid->ptr;
}

#if defined(Q4M_USE_RELATIVE_TIMEDWAIT) && defined(SAFE_MUTEX)
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_cond_timedwait_relative_np
int safe_cond_timedwait_relative_np(pthread_cond_t *cond, safe_mutex_t *mp,
				    struct timespec *abstime, const char *file,
				    uint line)
{
  int error;
  pthread_mutex_lock(&mp->global);
  if (mp->count != 1 || !pthread_equal(pthread_self(),mp->thread))
  {
    fprintf(stderr,"safe_mutex: Trying to cond_wait at %s, line %d on a not hold mutex\n",file,line);
    fflush(stderr);
    abort();
  }
  mp->count--;                                  /* Mutex will be released */
  pthread_mutex_unlock(&mp->global);
  error=pthread_cond_timedwait_relative_np(cond,&mp->mutex,abstime);
#ifdef EXTRA_DEBUG
  if (error && (error != EINTR && error != ETIMEDOUT && error != ETIME))
  {
    fprintf(stderr,"safe_mutex: Got error: %d (%d) when doing a safe_mutex_timedwait at %s, line %d\n", error, errno, file, line);
  }
#endif
  pthread_mutex_lock(&mp->global);
  mp->thread=pthread_self();
  if (mp->count++)
  {
    fprintf(stderr,
            "safe_mutex:  Count was %d in thread 0x%lx when locking mutex at %s, line %d (error: %d (%d))\n",
            mp->count-1, my_thread_dbug_id(), file, line, error, error);
    fflush(stderr);
    abort();
  }
  mp->file= file;
  mp->line=line;
  pthread_mutex_unlock(&mp->global);
  return error;
}
#endif
