-- 单独给集成测试用的库，避免测试数据污染开发库。
-- 这个脚本只会在 MySQL 数据卷第一次初始化时执行。
CREATE DATABASE IF NOT EXISTS luckycloudfile_test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON luckycloudfile_test.* TO 'lucky'@'%';

-- 迁移测试需要从“旧表结构”开始建库，所以单独给一个可清空的库。
CREATE DATABASE IF NOT EXISTS luckycloudfile_test_migration
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON luckycloudfile_test_migration.* TO 'lucky'@'%';
FLUSH PRIVILEGES;
