#pragma once

#if defined(HAVE_PSI_INTERFACE) && MYSQL_VERSION_ID >= 50706
#  define Q4M_HAVE_PSI_MEMORY_KEY
#endif

#ifdef Q4M_HAVE_PSI_MEMORY_KEY
#  define q4m_my_malloc(size, flags) \
  my_malloc(queue_key_memory_queue_share, size, flags)
#  define q4m_my_realloc(ptr, size, flags) \
  my_realloc(queue_key_memory_queue_share, ptr, size, flags)
#  define q4m_my_multi_malloc(flags, ...) \
  my_multi_malloc(queue_key_memory_queue_share, flags, __VA_ARGS__)
#else
#  define q4m_my_malloc(size, flags) my_malloc(size, flags)
#  define q4m_my_realloc(ptr, size, flags) \
  my_realloc(ptr, size, flags)
#  define q4m_my_multi_malloc(flags, ...) \
  my_multi_malloc(flags, __VA_ARGS__)
#endif
