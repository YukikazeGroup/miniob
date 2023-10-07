/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "common/log/log.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, const Value &value, const std::string &field, FilterStmt *filter_stmt)
    : table_(table), value_(value), field_(field), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // check input argument
  const char *table_name = update.relation_name.c_str();
  const char *column_name = update.attribute_name.c_str();
  if (nullptr == db || nullptr == table_name ||
          nullptr == column_name || UNDEFINED == update.value.attr_type()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, column_name=%p, value_type=%d", 
        db, table_name, column_name, update.value.attr_type());
    return RC::INVALID_ARGUMENT;
  }
  
  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check whether the column exists
  const TableMeta &table_meta = table->table_meta();
  const FieldMeta *field_meta = table_meta.field(column_name);
  if (nullptr == field_meta) {
    LOG_WARN("field not exists. column_name=%p", column_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // check whether original attribute type is the same as the input value type
  const AttrType field_type = field_meta->type();
  const AttrType value_type = update.value.attr_type();
  if (field_type != value_type) {  
    LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
        table_name, field_meta->name(), field_type, value_type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  // check whether the condition is legal
  // some test case: no records need to be modified
  // try to create condition stmt.
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, update.value, update.attribute_name, filter_stmt);
  return rc;
}
