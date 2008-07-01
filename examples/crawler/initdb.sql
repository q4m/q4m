CREATE TABLE url (
  id int(10) unsigned NOT NULL AUTO_INCREMENT,
  url varchar(255) NOT NULL,
  title varchar(255) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY url (url)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `crawler_queue` (
  `id` int(10) unsigned NOT NULL,
  `fail_cnt` int(11) NOT NULL DEFAULT 0
) ENGINE=QUEUE DEFAULT CHARSET=utf8;

CREATE TABLE `crawler_reschedule_queue` (
  `id` int(10) unsigned NOT NULL,
  `fail_cnt` int(11) NOT NULL DEFAULT 0,
  `at` int(11) NOT NULL DEFAULT 0
) ENGINE=QUEUE DEFAULT CHARSET=utf8;


DELIMITER |

CREATE TRIGGER update_crawler_queue AFTER INSERT ON url
  FOR EACH ROW BEGIN
   INSERT INTO crawler_queue SET id=NEW.id;
  END;
|

DELIMITER ;
