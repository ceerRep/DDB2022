#include <boost/algorithm/string/join.hpp>
#include <parsesql.hh>

#include <fmt/format.h>

// include the sql parser
#include "hsql/SQLParser.h"
// contains printing utilities
#include "hsql/util/sqlhelper.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/split.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <typeinfo>
#include <variant>

std::string format_column_name(std::string table, std::string column) {
  if (table.size())
    return table + "." + column;
  else
    return column;
}

std::tuple<std::string, std::string> split_column_name(std::string column) {
  auto comma_pos =
      std::find(column.data(), column.data() + column.size(), '.') -
      column.data();

  if (comma_pos == column.size())
    return {std::string(), column};
  else
    return {column.substr(0, comma_pos), column.substr(comma_pos + 1)};
}

std::optional<CompareConds> processCond(hsql::Expr *expr,
                                        std::string default_table_name) {
  auto expr1 = expr->expr;
  auto expr2 = expr->expr2;
  auto op = expr->opType;

  auto name1 = format_column_name(
      expr1->hasTable() ? expr1->table : default_table_name, expr1->getName());
  std::variant<int64_t, std::string> val2;

  if (expr2->isType(hsql::kExprColumnRef)) {
    val2 = format_column_name(expr2->hasTable() ? expr2->table
                                                : default_table_name,
                              expr2->getName());

    if (op == hsql::kOpEquals) {
      return CompareConds{name1, (CompareOps)op, val2};
    } else {
      std::cerr << fmt::format("Unexpected op in join operation: {} \n", op);
    }
  } else if (expr2->isLiteral()) {
    if (expr2->isType(hsql::kExprLiteralString)) {
      val2 = expr2->getName();
      return CompareConds{name1, (CompareOps)op, val2};
    } else if (expr2->isType(hsql::kExprLiteralInt)) {
      val2 = expr2->ival;
      return CompareConds{name1, (CompareOps)op, val2};
    } else {
      std::cerr << fmt::format("Unexpected literal type: {}\n", expr2->type);
    }
  } else {
    std::cerr << fmt::format("Unexpected expr type: {}\n", expr2->type);
  }
  return {};
}

SelectStmt parseSelectStmt(std::string stmt, DatabaseMetadata *db) {
  SelectStmt ret;

  hsql::SQLParserResult result;
  hsql::SQLParser::parse(stmt, &result);

  if (result.isValid() && result.size() > 0) {
    const hsql::SQLStatement *statement = result.getStatement(0);
    if (statement->isType(hsql::kStmtSelect)) {
      const auto *select =
          static_cast<const hsql::SelectStatement *>(statement);

      if (select->fromTable->list) {
        for (auto tab : *select->fromTable->list) {
          ret.table_names.push_back(tab->getName());
        }
      } else
        ret.table_names.push_back(select->fromTable->getName());

      for (auto expr : *select->selectList) {
        if (expr->isType(hsql::kExprStar)) {
          for (auto &&tname : ret.table_names)
            for (auto &&col_name : db->tables[tname].columns)
              ret.proj_columns.push_back(format_column_name(tname, col_name));
        } else
          ret.proj_columns.push_back(format_column_name(
              expr->hasTable() ? expr->table : ret.table_names[0],
              expr->getName()));
      }

      if (select->whereClause &&
          select->whereClause->isType(hsql::kExprOperator)) {
        auto now = select->whereClause;

        while (now->opType == hsql::kOpAnd) {
          auto cond = processCond(now->expr2, ret.table_names[0]);
          if (cond.has_value()) {
            if (now->expr2->expr2->isType(hsql::kExprColumnRef)) {
              ret.join_conds.push_back(*cond);
            } else {
              ret.filter_conds.push_back(*cond);
            }
          }
          now = now->expr;
        }

        auto cond = processCond(now, ret.table_names[0]);
        if (cond.has_value()) {
          if (now->expr2->isType(hsql::kExprColumnRef)) {
            ret.join_conds.push_back(*cond);
          } else {
            ret.filter_conds.push_back(*cond);
          }
        }
      }

    } else {
      throw std::runtime_error("not select stastement");
    }
  } else {
    throw std::runtime_error(result.errorMsg());
  }

  return ret;
}

// push down proj_col and sel_cond to all children node
// when single_table_name is not empty, ignore table name part in column names
// when single_table_name become not empty, filter proj_cols and sel_conds
void pushDownAndOptimize(BasicNode *now,
                         std::optional<std::set<std::string>> proj_cols,
                         std::vector<CompareConds> sel_conds,
                         std::string single_table_name, DatabaseMetadata *db) {
  if (ProjectionNode *projection = dynamic_cast<ProjectionNode *>(now)) {

    if (!proj_cols) {
      proj_cols = std::set<std::string>(projection->column_names.begin(),
                                        projection->column_names.end());
    } else {
      std::set<std::string> new_proj_cols;

      for (auto cname : projection->column_names) {
        auto [c0, c1] = split_column_name(cname);
        for (auto cname1 : *proj_cols) {
          auto [c2, c3] = split_column_name(cname1);

          if (single_table_name == "") {
            if (cname == cname1) {
              new_proj_cols.insert(cname);
              break;
            }
          } else {
            if (c1 == c3) {
              new_proj_cols.insert(cname);
              break;
            }
          }
        }
      }

      proj_cols = new_proj_cols;
    }

    projection->column_names =
        std::vector<std::string>(proj_cols->begin(), proj_cols->end());
    pushDownAndOptimize(projection->child.get(), proj_cols, sel_conds,
                        single_table_name, db);
    projection->disabled = projection->child->disabled;
  } else if (SelectionNode *selection = dynamic_cast<SelectionNode *>(now)) {
    std::vector<CompareConds> new_conds = selection->conds;
    new_conds.insert(new_conds.end(), sel_conds.begin(), sel_conds.end());
    selection->conds = new_conds;
    sel_conds = new_conds;
    pushDownAndOptimize(selection->child.get(), proj_cols, sel_conds,
                        single_table_name, db);
    selection->skipped = true;
    selection->disabled = selection->child->disabled;
  } else if (NJoinNode *njoin = dynamic_cast<NJoinNode *>(now)) {
    bool disabled = false;

    if (!njoin->change_all_table_name) {
      proj_cols->insert(njoin->join_column_names.begin(),
                        njoin->join_column_names.end());
      for (auto &&child : njoin->join_children) {
        pushDownAndOptimize(child.get(), proj_cols, sel_conds,
                            single_table_name, db);
      }
    } else {
      auto now_name = njoin->change_all_table_name;
      auto join_col =
          std::get<1>(split_column_name(njoin->join_column_names[0]));

      for (auto &&child : njoin->join_children) {
        auto child_projection = dynamic_cast<ProjectionNode *>(child.get());
        auto new_name =
            std::get<0>(split_column_name(child_projection->column_names[0]));
        std::set<std::string> new_projs;
        std::vector<CompareConds> new_conds;

        new_projs.insert(format_column_name(new_name, join_col));

        for (auto cname : *proj_cols) {
          auto [c0, c1] = split_column_name(cname);
          if (c0 == now_name)
            new_projs.insert(format_column_name(new_name, c1));
        }

        for (auto cond : sel_conds) {
          auto [c0, c1] = split_column_name(cond.val1);
          if (c0 == now_name)
            new_conds.push_back(
                {format_column_name(new_name, c1), cond.op, cond.val2});
        }

        pushDownAndOptimize(child_projection, new_projs, new_conds, new_name,
                            db);

        child_projection->skipped = true;
      }
    }

    for (auto &&child : njoin->join_children) {
      disabled = disabled || child->disabled;
    }
    njoin->disabled = disabled;

  } else if (UnionNode *union_node = dynamic_cast<UnionNode *>(now)) {
    auto now_name = union_node->change_all_table_name;

    bool disabled = true;

    for (auto &&child : union_node->union_children) {
      auto child_projection = dynamic_cast<ProjectionNode *>(child.get());
      auto new_name =
          std::get<0>(split_column_name(child_projection->column_names[0]));
      std::set<std::string> new_projs;
      std::vector<CompareConds> new_conds;

      for (auto cname : *proj_cols) {
        auto [c0, c1] = split_column_name(cname);
        if (c0 == now_name)
          new_projs.insert(format_column_name(new_name, c1));
      }

      for (auto cond : sel_conds) {
        auto [c0, c1] = split_column_name(cond.val1);
        if (c0 == now_name)
          new_conds.push_back(
              {format_column_name(new_name, c1), cond.op, cond.val2});
      }

      pushDownAndOptimize(child_projection, new_projs, new_conds, new_name, db);
      child_projection->skipped = true;
      disabled = disabled && child_projection->disabled;
    }

    union_node->disabled = disabled;
  } else if (ReadTableNode *rtable = dynamic_cast<ReadTableNode *>(now)) {
    rtable->column_names.insert(rtable->column_names.end(), proj_cols->begin(),
                                proj_cols->end());

    auto &&table_info = db->tables[rtable->orig_table_name];
    if (table_info.frag_type == TableMetadata::VFRAG) {
      rtable->select_conds.clear();

      auto [sname, fname] = split_column_name(rtable->table_name);

      std::set<std::string> cols;

      auto &&[fn, colnames] = table_info.vfrag_cols[sname];

      for (auto cname : colnames) {
        cols.insert(std::get<1>(split_column_name(cname)));
      }

      for (auto cond : sel_conds) {
        auto [tb, cname] = split_column_name(cond.val1);
        if (cols.count(cname))
          rtable->select_conds.push_back(cond);
      }
    }
    else
      rtable->select_conds = sel_conds;

    // check conditions
    for (auto cond1 : sel_conds)
      for (auto cond2 : sel_conds) {
        if (cond1.val1 != cond2.val1)
          continue;

        if (cond1.val2.index() == 0) {
          if ((cond1.op == CompareOps::LE || cond1.op == CompareOps::LT ||
               cond1.op == CompareOps::EQ) &&
              ((cond2.op == CompareOps::GE || cond2.op == CompareOps::GT ||
                cond2.op == CompareOps::EQ))) {
            int64_t end = std::get<0>(cond1.val2);
            if (cond1.op == CompareOps::LE || cond1.op == CompareOps::EQ)
              end++;

            int64_t start = std::get<0>(cond2.val2);
            if (cond2.op == CompareOps::GT)
              start++;

            if (end <= start) {
              rtable->disabled = true;
              return;
            }
          }
        } else if (cond1.op == CompareOps::EQ && cond2.op == CompareOps::EQ) {
          if (std::get<1>(cond1.val2) != std::get<1>(cond2.val2)) {
            rtable->disabled = true;
            return;
          }
        }
      }
  }
}

void processCreateMeta(std::string create_frag_stmt, DatabaseMetadata *db) {
  // CREATEMETA V/H site.frag ON table WHERE cond/column
  // CREATEMETA T table ON HFRAG/VFRAG WHERE cols
  std::vector<std::string> tokens;
  boost::split(tokens, create_frag_stmt, boost::is_any_of(" \t"),
               boost::token_compress_on);

  if (tokens.size() < 7) {
    // ERROR
    // NOT ENOUGH TOKENS
    return;
  }

  if (boost::to_lower_copy(tokens[0]) != "createmeta" ||
      boost::to_lower_copy(tokens[3]) != "on" ||
      boost::to_lower_copy(tokens[5]) != "where") {
    // format error
    return;
  }
  if (boost::to_lower_copy(tokens[1]) == "v") {
    auto [sitename, fragname] = split_column_name(tokens[2]);
    auto tablename = tokens[4];
    std::vector<std::string> cols;

    for (int i = 6; i < tokens.size(); i++)
      cols.push_back(tokens[i]);

    db->tables[tablename].vfrag_cols[sitename] =
        std::make_tuple(fragname, cols);
  } else if (boost::to_lower_copy(tokens[1]) == "h") {
    std::stringstream sql_ss;
    sql_ss << "select " << tokens[2] << " from " << tokens[4] << " where";
    for (int i = 6; i < tokens.size(); i++)
      sql_ss << " " << tokens[i];
    sql_ss << ";";
    auto stmt = sql_ss.str();
    std::cout << fmt::format("createmeta stmt parse sql: {}\n", stmt);

    hsql::SQLParserResult result;
    hsql::SQLParser::parse(stmt, &result);
    const hsql::SQLStatement *statement = result.getStatement(0);
    const auto *select = static_cast<const hsql::SelectStatement *>(statement);

    auto sitename = (*(select->selectList))[0]->table;
    auto fragname = (*(select->selectList))[0]->getName();
    auto tablename = select->fromTable->getName();
    std::vector<CompareConds> conds;

    auto now = select->whereClause;

    while (now->opType == hsql::kOpAnd) {
      auto cond = processCond(now->expr2, "");
      if (cond.has_value()) {
        if (now->expr2->expr2->isType(hsql::kExprColumnRef)) {
          conds.push_back(*cond);
        } else {
          conds.push_back(*cond);
        }
      }
      now = now->expr;
    }

    auto cond = processCond(now, "");
    if (cond.has_value()) {
      if (now->expr2->isType(hsql::kExprColumnRef)) {
        conds.push_back(*cond);
      } else {
        conds.push_back(*cond);
      }
    }

    db->tables[tablename].hfrag_conds[sitename] =
        std::make_tuple(fragname, conds);
  } else if (boost::to_lower_copy(tokens[1]) == "t") {
    auto tablename = tokens[2];
    db->tables[tablename].name = tablename;

    if (boost::to_lower_copy(tokens[4]) == "hfrag")
      db->tables[tablename].frag_type = TableMetadata::HFRAG;
    else
      db->tables[tablename].frag_type = TableMetadata::VFRAG;

    for (int i = 6; i < tokens.size(); i++) {
      std::string colname, coltype;
      int pos = std::find(tokens[i].begin(), tokens[i].end(), ':') -
                tokens[i].begin();
      colname = tokens[i].substr(0, pos);
      coltype = tokens[i].substr(pos + 1);
      db->tables[tablename].columns.push_back(colname);
      db->tables[tablename].column_type[colname] = coltype;
    }

  } else {
    // format error
    return;
  }
}

void printSelectStmt(SelectStmt result) {
  for (auto &&name : result.table_names)
    std::cout << name << ", ";
  std::cout << std::endl << std::endl;

  for (auto &&col : result.proj_columns)
    std::cout << col << ", ";
  std::cout << std::endl << std::endl;

  for (auto &&cond : result.join_conds) {
    std::cout << cond.val1 << ' ' << (int)cond.op << ' ';
    if (cond.val2.index() == 0) {
      std::cout << std::get<0>(cond.val2);
    } else if (cond.val2.index() == 1) {
      std::cout << std::get<1>(cond.val2);
    }
    std::cout << ", ";
  }
  std::cout << std::endl << std::endl;

  for (auto &&cond : result.filter_conds) {
    std::cout << cond.val1 << ' ' << (int)cond.op << ' ';
    if (cond.val2.index() == 0) {
      std::cout << std::get<0>(cond.val2);
    } else if (cond.val2.index() == 1) {
      std::cout << std::get<1>(cond.val2);
    }
    std::cout << ", ";
  }
  std::cout << std::endl << std::endl;
}

// readtable === union/join -> proj_placeholder -> select_placeholder ->
// readtable
std::shared_ptr<BasicNode> buildDistributedReadNode(std::string tablename,
                                                    DatabaseMetadata *db) {
  auto &&table_info = db->tables[tablename];
  if (table_info.frag_type == table_info.VFRAG) {
    auto join_node = std::make_shared<NJoinNode>();
    join_node->change_all_table_name = tablename;
    std::string join_col;

    // find out common columns
    {
      auto &&vfragcols = table_info.vfrag_cols;
      auto &&firstfrag = *vfragcols.begin();
      std::set<std::string> common_cols(std::get<1>(firstfrag.second).begin(),
                                        std::get<1>(firstfrag.second).end());

      for (auto &&[sname, fraginfo] : vfragcols) {
        auto &&[fname, cols] = fraginfo;

        decltype(common_cols) new_common_cols;
        for (auto &&cname : common_cols) {
          if (std::find(cols.begin(), cols.end(), cname) != cols.end())
            new_common_cols.insert(cname);
        }
        common_cols = new_common_cols;
      }

      join_col = *common_cols.begin();
    }

    for (auto &&[sname, fraginfo] : table_info.vfrag_cols) {
      auto &&[fname, cols] = fraginfo;
      join_node->join_column_names.push_back(fname + "." + join_col);
      auto proj = std::make_shared<ProjectionNode>();
      for (auto cname : cols) {
        proj->column_names.push_back(fname + "." + cname);
      }

      auto sel = std::make_shared<SelectionNode>();
      auto read = std::make_shared<ReadTableNode>();
      proj->child = sel;
      proj->read_node = read.get();
      sel->child = read;
      read->table_name = sname + "." + fname;
      read->orig_table_name = tablename;

      join_node->join_children.push_back(proj);
    }

    return join_node;
  } else // table_info.frag_type == table_info.HFRAG
  {
    auto union_node = std::make_shared<UnionNode>();
    union_node->change_all_table_name = tablename;

    for (auto &&[sname, fraginfo] : table_info.hfrag_conds) {
      auto &&[fname, conds] = fraginfo;
      auto proj = std::make_shared<ProjectionNode>();
      for (auto c : db->tables[tablename].columns)
        proj->column_names.push_back(fname + "." + c);
      auto sel = std::make_shared<SelectionNode>();
      for (auto s : conds) {
        sel->conds.push_back({fname + "." + s.val1, s.op, s.val2});
      }
      auto read = std::make_shared<ReadTableNode>();
      proj->child = sel;
      proj->read_node = read.get();
      sel->child = read;
      read->table_name = sname + "." + fname;
      read->orig_table_name = tablename;

      union_node->union_children.push_back(proj);
    }

    return union_node;
  }
}

std::shared_ptr<BasicNode> dfsBuildSelectFromTable(
    std::string now_table, std::string parent_table,
    std::map<std::string, std::map<std::string, CompareConds>> &joins,
    DatabaseMetadata *db) {
  // auto now_read_table = std::make_shared<ReadTableNode>();
  // now_read_table->table_name = now_table;

  auto now_read_table = buildDistributedReadNode(now_table, db);
  std::shared_ptr<BasicNode> now = now_read_table;

  for (auto &&[tname, join_cond] : joins[now_table]) {
    if (tname != parent_table) {
      auto child_node = dfsBuildSelectFromTable(tname, now_table, joins, db);
      auto join_node = std::make_shared<NJoinNode>();
      join_node->join_column_names.push_back(join_cond.val1);
      join_node->join_column_names.push_back(
          std::get<std::string>(join_cond.val2));
      join_node->join_children.push_back(now);
      join_node->join_children.push_back(child_node);
      now = join_node;
    }
  }

  return now;
}

// proj -> select -> join -> readtable
std::shared_ptr<BasicNode>
buildRawNodeTreeFromSelectStmt(const SelectStmt &selectStmt,
                               DatabaseMetadata *db) {
  std::shared_ptr<ProjectionNode> projection =
      std::make_shared<ProjectionNode>();

  // projection->parent = nullptr;
  projection->column_names = selectStmt.proj_columns;

  auto selection = std::make_shared<SelectionNode>();
  selection->conds = selectStmt.filter_conds;

  projection->child = selection;

  // build join seqs
  {
    std::map<std::string, std::map<std::string, CompareConds>> joins;
    std::map<std::string, int> counts;

    for (auto tname : selectStmt.table_names)
      counts[tname] = 0;

    for (auto &&cond : selectStmt.join_conds) {
      auto [t1, c1] = split_column_name(cond.val1);
      auto [t2, c2] = split_column_name(std::get<std::string>(cond.val2));
      std::cout << t1 << ' ' << t2 << std::endl;
      joins[t1][t2] = cond;
      joins[t2][t1] = cond;
      counts[t1]++;
      counts[t2]++;
    }

    for (auto &&[tname, count] : counts) {
      std::cout << count << ' ' << tname << std::endl;
      if (count <= 1) {
        selection->child = dfsBuildSelectFromTable(tname, "", joins, db);
        break;
      }
    }
  }

  return projection;
}

int compareVar(std::variant<int64_t, std::string> &l,
               std::variant<int64_t, std::string> &r) {
  if (l.index() != r.index())
    return 0;

  if (l.index() == 0) {
    return std::get<0>(l) - std::get<0>(r);
  }
  if (l.index() == 1) {
    return strcmp(std::get<1>(l).c_str(), std::get<1>(r).c_str());
  }

  return 0;
}

bool checkFragCond(std::vector<std::variant<int64_t, std::string>> &row,
                   std::vector<CompareConds> &conds,
                   std::map<std::string, int> &posmap) {
  for (auto &&cond : conds) {
    int pos = posmap[cond.val1];

    auto result = compareVar(row[pos], cond.val2);

    if (cond.op == CompareOps::EQ) {
      if (!(result == 0))
        return false;
    } else if (cond.op == CompareOps::LE) {
      if (!(result <= 0))
        return false;
    } else if (cond.op == CompareOps::LT) {
      if (!(result < 0))
        return false;
    } else if (cond.op == CompareOps::GE) {
      if (!(result >= 0))
        return false;
    } else if (cond.op == CompareOps::GT) {
      if (!(result > 0))
        return false;
    }
  }

  return true;
}

std::map<std::string, InsertStmt> insertStmtToSites(InsertStmt &istmt,
                                                    DatabaseMetadata *db) {
  std::map<std::string, InsertStmt> ret;

  auto &&table_info = db->tables[istmt.table_name];

  if (table_info.frag_type == table_info.VFRAG) {
    std::vector<std::tuple<std::string, std::string, std::vector<int>>>
        col_index;

    for (auto &&[sname, sdata] : table_info.vfrag_cols) {
      auto &&[fname, colnames] = sdata;

      col_index.emplace_back(sname, fname, std::vector<int>());

      auto &vec = std::get<2>(col_index.back());

      for (auto col : colnames) {
        vec.push_back(
            std::find(istmt.columns.begin(), istmt.columns.end(), col) -
            istmt.columns.begin());
      }

      ret[sname] = InsertStmt();
      ret[sname].table_name = fname;
      ret[sname].columns = colnames;
    }

    for (auto &&data : istmt.values) {
      for (auto &&[sname, fname, inds] : col_index) {
        ret[sname].values.emplace_back();
        auto &val = ret[sname].values.back();

        for (auto i : inds)
          val.push_back(data[i]);
      }
    }
  } else {
    std::map<std::string, int> pos_map;

    for (auto col : table_info.columns)
      pos_map[col] =
          std::find(istmt.columns.begin(), istmt.columns.end(), col) -
          istmt.columns.begin();

    for (auto &&data : istmt.values) {
      for (auto &&[sname, sdata] : table_info.hfrag_conds) {
        auto &&[fname, fcond] = sdata;

        if (checkFragCond(data, fcond, pos_map)) {
          ret[sname].values.push_back(data);
          break;
        }
      }
    }

    for (auto &&[sname, insdata] : ret) {
      insdata.columns = istmt.columns;
      insdata.table_name = std::get<0>(table_info.hfrag_conds[sname]);
    }
  }

  return ret;
}

InsertStmt insertStmtFromTSV(std::string table, std::string filename,
                             DatabaseMetadata *db) {
  InsertStmt ret;

  auto &table_info = db->tables[table];
  ret.table_name = table;
  ret.columns = db->tables[table].columns;

  std::string line;
  std::ifstream ifs(filename);
  while (std::getline(ifs, line)) {
    std::vector<std::string> cols;
    std::vector<std::variant<int64_t, std::string>> values;
    boost::split(cols, line, boost::is_any_of("\t"), boost::token_compress_on);

    if (cols.size() == ret.columns.size()) {
      for (int i = 0; i < cols.size(); i++) {
        if (table_info.column_type[table_info.columns[i]] == "int")
          values.emplace_back(std::stoll(cols[i]));
        else
          values.emplace_back(cols[i]);
      }
    }

    ret.values.push_back(values);
  }

  return ret;
}

InsertStmt parseInsertStmt(std::string sql, DatabaseMetadata *db) {
  InsertStmt ret;
  hsql::SQLParserResult result;
  hsql::SQLParser::parse(sql, &result);
  if (result.isValid() && result.size() > 0) {
    const hsql::SQLStatement *statement = result.getStatement(0);

    if (statement->isType(hsql::kStmtInsert)) {
      const auto *insert =
          static_cast<const hsql::InsertStatement *>(statement);

      ret.table_name = insert->tableName;

      if (insert->columns) {
        for (auto col : *insert->columns) {
          ret.columns.push_back(col);
        }
      } else {
        ret.columns = db->tables[ret.table_name].columns;
      }

      ret.values.emplace_back();
      for (auto expr : *insert->values) {
        if (expr->isType(hsql::kExprLiteralInt)) {
          ret.values.back().emplace_back(expr->ival);
        } else if (expr->isType(hsql::kExprLiteralString)) {
          ret.values.back().emplace_back(expr->getName());
        } else {
        }
      }
    }
  } else {
    throw std::runtime_error(result.errorMsg());
  }
  return ret;
}

void dfs_join_dfs(
    std::string &key, int floor, std::vector<std::vector<std::string> *> &now,
    std::vector<std::multimap<std::string, std::vector<std::string>>>
        &sorted_results,
    std::vector<std::vector<std::string>> &results) {
  if (floor == sorted_results.size()) {
    results.emplace_back();
    for (auto pv : now)
      results.back().insert(results.back().end(), pv->begin(), pv->end());
    return;
  }

  auto itr1 = sorted_results[floor].lower_bound(key);
  auto itr2 = sorted_results[floor].upper_bound(key);

  while (itr1 != itr2) {
    now.push_back(&itr1->second);
    dfs_join_dfs(key, floor + 1, now, sorted_results, results);
    now.pop_back();
    itr1++;
  }
}

std::vector<std::vector<std::string>>
dfs_join(std::string key,
         std::vector<std::multimap<std::string, std::vector<std::string>>>
             &sorted_results) {
  std::vector<std::vector<std::string>> ret;
  std::vector<std::string> now0{key};
  std::vector<std::vector<std::string> *> now{&now0};
  dfs_join_dfs(key, 0, now, sorted_results, ret);

  return ret;
}

// createtable name colname1,attr1,... ... | createmetas ... | ...

std::string buildCreateTableFromColumns(std::string tablename,
                                        std::vector<std::string> info) {
  std::stringstream sql_ss;
  sql_ss << "create table " << tablename << " (";

  std::vector<std::string> cols;

  for (auto c : info) {
    std::vector<std::string> tmp;
    boost::split(tmp, c, boost::is_any_of(","));
    cols.push_back(boost::algorithm::join(tmp, " "));
  }
  sql_ss << boost::algorithm::join(cols, ", ");
  sql_ss << ");";

  return sql_ss.str();
}

std::map<std::string, std::string>
parseCreateTable(std::string create_sql, DatabaseMetadata *db,
                 std::vector<std::string> *metas) {
  std::vector<std::string> stmts;
  std::vector<std::string> table_info_tokens;
  std::map<std::string, std::string> ret;

  boost::split(stmts, create_sql, boost::is_any_of("|"));

  std::string table_info_stmt = stmts[0], create_frag = stmts[1];

  boost::split(table_info_tokens, table_info_stmt, boost::is_any_of(" "));

  // createmeta t

  for (int i = 1; i < stmts.size(); i++) {
    processCreateMeta(stmts[i], db);

    if (metas)
      metas->push_back(stmts[i]);
  }

  auto &&table_meta = db->tables[table_info_tokens[1]];
  if (table_meta.frag_type == TableMetadata::HFRAG) {
    std::vector<std::string> info(table_info_tokens.begin() + 2,
                                  table_info_tokens.end());

    for (auto &&[sname, sdata] : table_meta.hfrag_conds) {
      auto &&[fname, data] = sdata;
      ret[sname] = buildCreateTableFromColumns(fname, info);
    }
  } else {
    for (auto &&[sname, sdata] : table_meta.vfrag_cols) {
      auto &&[fname, vcols] = sdata;

      std::vector<std::string> info;
      std::set<std::string> vcols_set(vcols.begin(), vcols.end());

      for (int i = 2; i < table_info_tokens.size(); i++) {
        std::vector<std::string> tmp;
        boost::split(tmp, table_info_tokens[i], boost::is_any_of(","));
        if (vcols_set.count(tmp[0]))
          info.push_back(table_info_tokens[i]);
      }

      ret[sname] = buildCreateTableFromColumns(fname, info);
    }
  }

  return ret;
}
