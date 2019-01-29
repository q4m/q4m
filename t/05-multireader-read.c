#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>

static MYSQL mysql;

static MYSQL_RES *do_select(const char *sql)
{
  MYSQL_RES *res;
  
  if (mysql_query(&mysql, sql) != 0) {
    fprintf(stderr, "SQL error: %s\n", sql);
    exit(3);
  }
  if ((res = mysql_store_result(&mysql)) == NULL) {
    fprintf(stderr, "failed to obtain result from mysql\n");
    exit(3);
  }
  return res;
}

int main(int argc, char **argv)
{
  const char *host = getenv("MYSQL_HOST"),
    *user = getenv("MYSQL_USER"),
    *password = getenv("MYSQL_PASSWORD"),
    *db = getenv("MYSQL_DB"),
    *port_str = getenv("MYSQL_PORT"),
    *unix_socket = getenv("MYSQL_SOCKET");
  unsigned short port;
  unsigned i, loop;
  MYSQL_RES *res;
  
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <loop_count>\n", argv[0]);
    exit(1);
  } else if (sscanf(argv[1], "%u", &loop) != 1) {
    fprintf(stderr, "invalid loop count: %s\n", argv[1]);
    exit(1);
  }
  if (db == NULL)
    db = "test";
  if (port_str != NULL
      && sscanf(port_str, "%hu", &port) != 1) {
    fprintf(stderr, "invalid port number: %s\n", port_str);
    exit(1);
  }
  
  mysql_init(&mysql);
  if (mysql_real_connect(&mysql, host, user, password, db, port, unix_socket, 0)
      == NULL) {
    fprintf(stderr, "could not connect to mysql\n");
    exit(2);
  }
  
  /* wait until q4m_t2 becomes readable */
  while (1) {
    res = do_select("SELECT queue_wait('q4m_t2')");
    if (mysql_num_rows(res) == 0) {
      fprintf(stderr, "unexpected response from queue_wait\n");
      exit(3);
    }
    if (strcmp(mysql_fetch_row(res)[0], "0") != 0) {
      break;
    }
  }
  for (i = 0; i < loop;) {
    /* read data and print */
    res = do_select("SELECT queue_wait('q4m_t')");
    res = do_select("SELECT * FROM q4m_t");
    if (mysql_num_rows(res) != 0) {
      printf("%s\n", mysql_fetch_row(res)[0]);
      i++;
    }
    mysql_free_result(res);
  }
  /* queue_end */
  res = do_select("SELECT queue_end()");
  mysql_free_result(res);
  
  mysql_close(&mysql);
  return 0;
}
