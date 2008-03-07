INSTALL PLUGIN queue SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_wait RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_dread RETURNS STRING SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_end RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_abort RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_dwrite RETURNS STRING SONAME 'libqueue_engine.so';
