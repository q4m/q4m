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

static void insert_row(int v, unsigned var_length)
{
  char *sql = alloca(var_length + 256);
  
  if (var_length != 0) {
    char *p;
    int l = random() % var_length;
    sprintf(sql, "INSERT INTO q4m_t VALUES (%d,'", v);
    for (p = sql + strlen(sql); l != 0; p++, l--) {
      *p = 'z';
    }
    strcpy(p, "')");
  } else {
    sprintf(sql, "INSERT INTO q4m_t VALUES (%d)", v);
  }
  if (mysql_query(&mysql, sql) != 0) {
    fprintf(stderr, "insert failed\n");
    exit(3);
  }
}

int main(int argc, char **argv)
{
  const char *host = getenv("MYSQL_HOST"),
    *user = getenv("MYSQL_USER"),
    *password = getenv("MYSQL_PASSWORD"),
    *db = getenv("MYSQL_DB"),
    *port_str = getenv("MYSQL_PORT"),
    *unix_socket = getenv("MYSQL_SOCKET"),
    *var_length_str = getenv("VAR_LENGTH");
  unsigned short port;
  unsigned i, loop, start_value, var_length = 0;
  MYSQL_RES *res;
  
  if (argc != 3) {
    fprintf(stderr, "Usage: %s <loop_count> <start_value>\n", argv[0]);
    exit(1);
  } else if (sscanf(argv[1], "%u", &loop) != 1) {
    fprintf(stderr, "invalid loop count: %s\n", argv[1]);
    exit(1);
  } else if (sscanf(argv[2], "%u", &start_value) != 1) {
    fprintf(stderr, "invalid start value: %s\n", argv[2]);
    exit(1);
  }
  if (db == NULL)
    db = "test";
  if (port_str != NULL
      && sscanf(port_str, "%hu", &port) != 1) {
    fprintf(stderr, "invalid port number: %s\n", port_str);
    exit(1);
  }
  if (var_length_str != NULL
      && sscanf(var_length_str, "%u", &var_length) != 1) {
    fprintf(stderr, "invalid var_length: %s\n", var_length_str);
    exit(1);
  }
  
  mysql_init(&mysql);
  if (mysql_real_connect(&mysql, host, user, password, db, port, unix_socket, 0)
      == NULL) {
    fprintf(stderr, "could not connect to mysql\n");
    exit(2);
  }
  
  for (i = 0; i < loop; i++) {
    /* insert one row */
    insert_row(i + start_value, var_length);
    /* queue_wait */
    while (1) {
      res = do_select("SELECT queue_wait('q4m_t')");
      res = do_select("SELECT * FROM q4m_t");
      if (mysql_num_rows(res) != 0) {
	break;
      }
      mysql_free_result(res);
    }
    printf("%s\n", mysql_fetch_row(res)[0]);
    mysql_free_result(res);
  }
  /* queue_end */
  res = do_select("SELECT queue_end()");
  mysql_free_result(res);
  
  mysql_close(&mysql);
  return 0;
}
