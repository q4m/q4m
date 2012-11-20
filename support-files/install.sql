INSTALL PLUGIN queue SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_wait RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_end RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_abort RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_rowid RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_set_srcid RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_compact RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_stats RETURNS STRING SONAME 'libqueue_engine.so';
