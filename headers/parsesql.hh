#ifndef _PARSE_SQL_HH

#define _PARSE_SQL_HH

#include <hsql/sql/Expr.h>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "hsql/SQLParser.h"

enum class CompareOps {
  EQ = hsql::kOpEquals,
  LE = hsql::kOpLessEq,
  LT = hsql::kOpLess,
  GE = hsql::kOpGreaterEq,
  GT = hsql::kOpGreater
};

inline std::ostream &operator<<(std::ostream &os, CompareOps ec) {
  std::string s;

  if (ec == CompareOps::EQ)
    s = "=";
  else if (ec == CompareOps::GE)
    s = ">=";
  else if (ec == CompareOps::GT)
    s = ">";
  else if (ec == CompareOps::LE)
    s = "<=";
  else if (ec == CompareOps::LT)
    s = "<";
  else
    s = "<>";

  return os << s;
}

struct CompareConds {
  std::string val1;
  CompareOps op;
  std::variant<int64_t, std::string> val2;
};

struct TableMetadata {
  enum { HFRAG, VFRAG };

  int frag_type;
  std::string name;
  std::vector<std::string> columns;
  std::map<std::string, std::string> column_type;
  // sitename, fragname, cond
  std::map<std::string, std::tuple<std::string, std::vector<CompareConds>>>
      hfrag_conds;
  std::map<std::string, std::tuple<std::string, std::vector<std::string>>>
      vfrag_cols;
};

struct DatabaseMetadata {
  std::vector<std::string> sites;
  std::map<std::string, TableMetadata> tables;
};

struct SelectStmt {
  std::vector<std::string> table_names;
  std::vector<std::string> proj_columns;
  std::vector<CompareConds> join_conds;
  std::vector<CompareConds> filter_conds;
};

struct InsertStmt {
  std::string table_name;
  std::vector<std::string> columns;
  std::vector<std::vector<std::variant<int64_t, std::string>>> values;
};

struct DeleteStmt {
  std::string table_name;
};

struct BasicNode {
  //   BasicNode *parent;
  int result = 0;

  bool skipped = false;
  bool disabled = false;

  virtual ~BasicNode() = default;
  virtual std::string to_string(int prefix = 0) = 0;
  virtual std::shared_ptr<BasicNode> copy() = 0;
};

struct ProjectionNode : public BasicNode {
  std::vector<std::string> column_names;

  std::shared_ptr<BasicNode> child;

  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "projection (";
    for (auto &&name : column_names) {
      ss << name << ", ";
    }
    ss << ")";

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    ss << '\n';
    ss << child->to_string(prefix + 1);

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override {
    if (skipped)
      return child->copy();
    else {
      auto proj = std::make_shared<ProjectionNode>();
      proj->column_names = column_names;
      proj->disabled = disabled;
      proj->skipped = skipped;
      proj->child = child->copy();

      return proj;
    }
  }
};

struct SelectionNode : public BasicNode {
  std::vector<CompareConds> conds;
  std::shared_ptr<BasicNode> child;

  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "selection (";
    for (auto &&cond : conds) {
      ss << cond.val1 << ' ' << cond.op << ' ';
      if (cond.val2.index() == 0) {
        ss << std::get<0>(cond.val2);
      } else if (cond.val2.index() == 1) {
        ss << std::get<1>(cond.val2);
      }
      ss << ", ";
    }
    ss << ")";

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    ss << '\n';
    ss << child->to_string(prefix + 1);

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override {
    if (skipped) {
      return child->copy();
    } else {
      auto sel = std::make_shared<SelectionNode>();
      sel->disabled = disabled;
      sel->skipped = skipped;
      sel->conds = conds;
      sel->child = child->copy();

      return sel;
    }
  }
};

struct RenameNode : public BasicNode {
  std::string table_name;
  std::shared_ptr<BasicNode> child;

  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "rename table " << table_name;

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    ss << '\n';
    ss << child->to_string(prefix + 1);

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override { return nullptr; }
};

struct ReadTableNode : public BasicNode {
  std::string table_name;
  std::string orig_table_name;
  std::vector<std::string> column_names;
  std::vector<CompareConds> select_conds;

  // TODO: meta datas
  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "readtable from ";
    ss << table_name;
    ss << " (";

    for (auto col : column_names)
      ss << col << ", ";
    ss << ") where (";

    for (auto cond : select_conds) {
      ss << cond.val1 << ' ' << cond.op << ' ';
      if (cond.val2.index() == 0) {
        ss << std::get<0>(cond.val2);
      } else if (cond.val2.index() == 1) {
        ss << std::get<1>(cond.val2);
      }
      ss << ", ";
    }
    ss << ")";

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override {
    return std::make_shared<ReadTableNode>(*this);
  }
};

struct NJoinNode : public BasicNode {
  std::vector<std::string> join_column_names;
  std::vector<std::shared_ptr<BasicNode>> join_children;

  std::optional<std::string> change_all_table_name;

  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "njoin (";
    for (auto &&name : join_column_names) {
      ss << name << ", ";
    }
    ss << ")";

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    if (change_all_table_name) {
      ss << " tablename -> " << *change_all_table_name;
    }

    for (auto &&child : join_children) {
      ss << '\n';
      ss << child->to_string(prefix + 1);
    }

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override {
    int child_num = 0;
    std::shared_ptr<BasicNode> last_enabled_child = join_children[0];

    for (auto &&child : join_children) {
      bool useful = true;

      if (child->disabled) {
        useful = false;
      }

      if (change_all_table_name) {
        auto child_projection = dynamic_cast<ProjectionNode *>(child.get());
        if (child_projection->column_names.size() <= 1) {
          useful = false;
        }
      }

      if (useful) {
        child_num++;
        last_enabled_child = child;
      }
    }

    if (child_num <= 1) {
      auto child_copy = last_enabled_child->copy();
      if (change_all_table_name) {
        auto rename = std::make_shared<RenameNode>();
        rename->child = child_copy;
        rename->table_name = *change_all_table_name;

        return rename;
      } else
        return child_copy;
    } else {
      auto join = std::make_shared<NJoinNode>();
      join->join_column_names = join_column_names;
      join->disabled = disabled;
      join->skipped = skipped;
      join->change_all_table_name = change_all_table_name;

      for (auto &&child : join_children) {
        if (change_all_table_name &&
            dynamic_cast<ProjectionNode *>(child.get())->column_names.size() <=
                1) {
        } else
          join->join_children.push_back(child->copy());
      }

      return join;
    }
  }
};

struct UnionNode : public BasicNode {
  std::vector<std::shared_ptr<BasicNode>> union_children;

  std::optional<std::string> change_all_table_name;

  virtual std::string to_string(int prefix = 0) override {
    std::stringstream ss;
    for (int i = 0; i < prefix; i++)
      ss << ' ';
    ss << "union (";
    ss << ")";

    if (skipped)
      ss << " SKIPPED";
    if (disabled)
      ss << " DISABLED";

    ss << " " << result;

    if (change_all_table_name) {
      ss << " tablename -> " << *change_all_table_name;
    }

    for (auto &&child : union_children) {
      ss << '\n';
      ss << child->to_string(prefix + 1);
    }

    return ss.str();
  }

  virtual std::shared_ptr<BasicNode> copy() override {
    int child_num = 0;
    std::shared_ptr<BasicNode> last_enabled_child;

    for (auto &&child : union_children) {
      if (!child->disabled) {
        child_num++;
        last_enabled_child = child;
      }
    }

    if (child_num == 1) {
      auto child_copy = last_enabled_child->copy();
      if (change_all_table_name) {
        auto rename = std::make_shared<RenameNode>();
        rename->child = child_copy;
        rename->table_name = *change_all_table_name;

        return rename;
      } else
        return child_copy;
    } else {
      auto union_node = std::make_shared<UnionNode>();

      union_node->disabled = disabled;
      union_node->skipped = skipped;
      union_node->change_all_table_name = change_all_table_name;

      for (auto &&child : union_children) {
        if (!child->disabled) {
          union_node->union_children.push_back(child->copy());
        }
      }

      return union_node;
    }
  }
};

std::string format_column_name(std::string table, std::string column);
std::tuple<std::string, std::string> split_column_name(std::string column);
std::optional<CompareConds> processCond(hsql::Expr *expr,
                                        std::string default_table_name);
SelectStmt parseSelectStmt(std::string stmt, DatabaseMetadata *db);
void pushDownAndOptimize(BasicNode *now,
                         std::optional<std::set<std::string>> proj_cols,
                         std::vector<CompareConds> sel_conds,
                         std::string single_table_name);
void processCreateMeta(std::string create_frag_stmt, DatabaseMetadata *db);
void printSelectStmt(SelectStmt result);
std::shared_ptr<BasicNode> buildDistributedReadNode(std::string tablename,
                                                    DatabaseMetadata *db);
std::shared_ptr<BasicNode> dfsBuildSelectFromTable(
    std::string now_table, std::string parent_table,
    std::map<std::string, std::map<std::string, CompareConds>> &joins,
    DatabaseMetadata *db);
std::shared_ptr<BasicNode>
buildRawNodeTreeFromSelectStmt(const SelectStmt &selectStmt,
                               DatabaseMetadata *db);

int compareVar(std::variant<int64_t, std::string> &l,
               std::variant<int64_t, std::string> &r);
bool checkFragCond(std::vector<std::variant<int64_t, std::string>> &row,
                   std::vector<CompareConds> &conds,
                   std::map<std::string, int> &posmap);
std::map<std::string, InsertStmt> insertStmtToSites(InsertStmt &istmt,
                                                    DatabaseMetadata *db);
InsertStmt insertStmtFromTSV(std::string table, std::string filename,
                             DatabaseMetadata *db);

InsertStmt parseInsertStmt(std::string sql, DatabaseMetadata *db);

std::vector<std::vector<std::string>>
dfs_join(std::string key,
         std::vector<std::multimap<std::string, std::vector<std::string>>>
             &sorted_results);

std::map<std::string, std::string>
parseCreateTable(std::string create_sql, DatabaseMetadata *db,
                 std::vector<std::string> *metas);
#endif
