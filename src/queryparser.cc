#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <regex>
#include <set>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

// include the sql parser
#include "hsql/SQLParser.h"
// contains printing utilities
#include "hsql/util/sqlhelper.h"

// 默认命名空间
using namespace std;

namespace {
// 空字符填充
string BLANK_STR = "";

// 原SQL-Parser引入的操作符枚举类型，沿用
// Operator types. These are important for expressions of type kExprOperator.
enum OperatorType {
  kOpNone,

  // Ternary operator
  kOpBetween,

  // n-nary special case
  kOpCase,
  kOpCaseListElement, // `WHEN expr THEN expr`

  // Binary operators.
  kOpPlus,
  kOpMinus,
  kOpAsterisk,
  kOpSlash,
  kOpPercentage,
  kOpCaret,

  kOpEquals, // 10
  kOpNotEquals,
  kOpLess,
  kOpLessEq,
  kOpGreater,
  kOpGreaterEq,
  kOpLike,
  kOpNotLike,
  kOpILike,
  kOpAnd,
  kOpOr,
  kOpIn,
  kOpConcat,

  // Unary operators.
  kOpNot,
  kOpUnaryMinus,
  kOpIsNull,
  kOpExists
};

// 运算符解析int -> string
string parseOp(int opType) {
  switch (opType) {
  case 10:
    return "=";
  case 12:
    return "<";
  case 13:
    return "<=";
  case 14:
    return ">";
  case 15:
    return ">=";
  default:
    break;
  }
  return "";
}

// 运算表达式 A op B 三元组
// 我们假设左操作数一定为Table.Column类型
struct ExprTriple {
  // 左操作数
  int leftValType; // 0 - number, 1 - string, 2 - table.attribute
  string leftTable;
  string leftColumn;
  // 操作符
  int opType;
  // 右操作数
  int rightValType;
  int rightVal;
  string rightStr;
  string rightTable;
  string rightColumn;
};

/*
 *
 * 初始化时获得的全局变量及函数
 *
 */
map<string, map<string, vector<ExprTriple>>>
    FT;                     // 表名-分片名-分片条件：三元组vector
map<string, int> howToFrag; // 表名-分片方式
map<string, string> primaryKey; // 表名-主键
map<pair<string, string>, set<pair<string, string>>>
    keyMap; // 主键-外键映射关系，双向 (table, clomun) -> (table, clomun)
map<string, map<string, vector<ExprTriple>>>
    SFT; // 站点-分片名-分片条件：三元组vector
// 初始化分片信息
ExprTriple initExprTriple(int leftValType, string leftTable, string leftColumn,
                          int opType, int rightValType, int rightVal,
                          string rightStr, string rightTable,
                          string rightColumn);

/*
 *
 * 解析sql语句时需要的全局变量及函数
 *
 */
int cnt = 0;                       // 遍历where子句三元组计数器
ExprTriple con;                    // 当前表达式三元组变量
vector<ExprTriple> whereCondition; // where子句所有三元组条件集合
// parser解析后，递归求where子句参数
void findCondition(hsql::Expr *expr);
// 判断选择分片
bool ExprTripleValCompare(int opType1, int val1, int opType2, int val2);
// 判断当前表达式是否可筛选分片
void getInvolvedFrag(ExprTriple cur_expr, set<string> &involvedFragments,
                     set<string> iterFragments);
// 找到select子句对应的垂直分片
void locateVerticalFrag(string tName, string cName,
                        set<string> &involvedFragments);
// 处理where子句中，主外键关系所添加的新约束条件
vector<ExprTriple> addNewCondition(pair<string, string> leftAttr,
                                   pair<string, string> rightAttr);

/*
 *
 * 生成各站点SQL语句需要的全局变量及函数
 *
 */
vector<pair<string, string>> select_attributes; // select子句中的所有表名.列名
map<string, string> fragToSite;        // 分片名-站点名映射关系
map<string, vector<string>> siteToSQL; // 站点名-执行SQL语句
// select子句中选择的属性按表名排序
bool attributeSort(pair<string, string> a1, pair<string, string> a2) {
  return a1.first < a2.first;
}
void genSQLForSites(set<string> involvedFragments, vector<string> tables);

// test
void fragInit();
void initFragInfo();

void printExprTriple(ExprTriple e) {
  cout << e.leftTable << "." << e.leftColumn << " " << e.opType << " ";
  if (e.rightValType == 0)
    cout << e.rightVal << endl;
  else if (e.rightValType == 1)
    cout << e.rightStr << endl;
  else
    cout << e.rightTable << "." << e.rightColumn << endl;
}

std::pair<int, std::map<std::string, std::vector<std::string>>>
parseQuery(std::string query) {
  int fragType = -1;
  std::vector<std::vector<std::string>> ret(4);
  siteToSQL.clear();

  // 使用原有sql-parser解析语句
  hsql::SQLParserResult result;
  hsql::SQLParser::parse(query, &result);

  // 如果解析成功则继续处理
  if (result.isValid()) {

    // printf("Parsed successfully!\n");
    // printf("Number of statements: %lu\n", result.size());

    // 逐条分析
    for (auto i = 0u; i < result.size(); ++i) {

      const hsql::SQLStatement *statement = result.getStatement(i);

      // 如果是选择语句
      if (statement->isType(hsql::kStmtSelect)) {

        const auto *select =
            static_cast<const hsql::SelectStatement *>(statement);

        /*
         *
         * 一些解析过程中的变量
         *
         */

        // 表名，站点名，分片名，列名
        string tName, sName, fName, cName;
        // 最终得到的所需分片信息
        set<string> involvedFragments;
        // 对于from中垂直分片的表格，默认保存表名-主键名，如果表名对应的键不是主键，说明不需要为主键额外选取垂直分片
        map<string, string> remainedKey;

        /*
         *
         * From 子句解析
         *
         */

        // from语句涉及表
        vector<string> tables;
        // from子句的解析
        if (select->fromTable->name == nullptr) {
          //   printf("Involved Table: ");
          for (hsql::TableRef *tbl : *select->fromTable->list) {
            printf("%s ", tbl->name);
            tables.push_back(tbl->name);
          }
          printf("\n");
        } else {
          //   printf("Involved Table: %s\n", select->fromTable->name);
          tables.push_back(select->fromTable->name);
        }

        /*
         *
         * Select 子句解析
         *
         */

        // 如果from中的表是水平分片，默认需要所有分片
        for (vector<string>::iterator i = tables.begin(); i != tables.end();
             i++) {
          if (fragType == -1)
            fragType = howToFrag[*i];
          if (howToFrag[*i] == 1)
            continue;
          for (map<string, vector<ExprTriple>>::iterator j = FT[*i].begin();
               j != FT[*i].end(); j++) {
            involvedFragments.insert(j->first);
          }
        }

        // printf("Involved Attributes: \n"); // select查询的列
        for (hsql::Expr *expr : *select->selectList) {

          // 如果是*, kExprStar
          if (expr->type == 6) {
            // printf("*");
            // 对于垂直分片，所有分片全部被需要
            for (vector<string>::iterator i = tables.begin(); i != tables.end();
                 i++) {
              if (howToFrag[*i] == 0)
                continue;
              for (map<string, vector<ExprTriple>>::iterator j = FT[*i].begin();
                   j != FT[*i].end(); j++) {
                involvedFragments.insert(j->first);
              }
            }
            select_attributes.push_back(pair<string, string>("*", "*"));
            break;
          } else {
            // table.column
            if (expr->table != nullptr) {
              //   printf("%s.%s ", expr->table, expr->name);
              // 检查分片
              tName = expr->table;
              cName = expr->name;
              // 如果有垂直分片，那么可以少取，如果是主键则需要额外判断，如果取了其他分片，则无需再取，否则取任何都可以
              if (howToFrag[tName] == 1) {
                // 非主键分片被选择，主键一定包含
                if (primaryKey[tName] != cName) {
                  locateVerticalFrag(tName, cName, involvedFragments);
                  remainedKey[tName] = cName;
                }
              }
              // 向select_attributes添加
              select_attributes.push_back(pair<string, string>(tName, cName));
            }
            // 暂未考虑这种情况：column
            else {
              //   printf("%s ", expr->name);
              // 检查分片
              tName = tables[0];
              cName = expr->name;
              // 如果有垂直分片，那么可以少取
              if (howToFrag[tName] == 1) {
                // 非主键分片被选择，主键一定包含
                if (primaryKey[tName] != cName) {
                  locateVerticalFrag(tName, cName, involvedFragments);
                  remainedKey[tName] = cName;
                }
              }
            }
          }
        }
        // printf("\n");

        set<string>::iterator j;
        // cout << "SELECT involves:" << endl;
        // for (j = involvedFragments.begin(); j != involvedFragments.end();
        // j++) {
        //   cout << *j << endl;
        // }
        // cout << endl;

        /*
         *
         * Where 子句解析
         *
         */

        // 迭代过程中过的分片
        set<string> iterFragments;

        if (select->whereClause != nullptr) {
          // select涉及的where子句
          findCondition(select->whereClause);
          whereCondition.push_back(con);

          vector<ExprTriple>::iterator i;

          for (i = whereCondition.begin(); i != whereCondition.end(); i++) {

            // 连接操作
            if (i->leftValType == 2 && i->rightValType == 2 &&
                i->opType == kOpEquals) {

              // 获取连接两侧的表名和列名
              // e.g book.id = orders.book_id
              pair<string, string> leftAttr(i->leftTable, i->leftColumn);
              pair<string, string> rightAttr(i->rightTable, i->rightColumn);
              // 根据主外键关系添加新的约束条件
              vector<ExprTriple> addedCondition =
                  addNewCondition(leftAttr, rightAttr);
              // 添加新约束条件到末尾
              whereCondition.insert(whereCondition.end(),
                                    addedCondition.begin(),
                                    addedCondition.end());
            }

            string tName = i->leftTable;
            string cName = i->leftColumn;

            // 水平分片
            if (howToFrag[tName] == 0) {
              //   cout << "Horizontal Fragment Condition: ";
              // 左操作数为表名.列名
              //   cout << tName << "." << cName << " ";
              // 操作数
              //   cout << i->opType << " ";
              // 右操作数判断
              // 右值为数值
              if (i->rightValType == 0) {
                // cout << i->rightVal << endl;
                getInvolvedFrag(*i, involvedFragments, iterFragments);
              }
              // 右值为字符串
              else if (i->rightValType == 1) {
                // cout << i->rightStr << endl;
                getInvolvedFrag(*i, involvedFragments, iterFragments);
              } else
                ;
              // cout << i->rightTable << "." << i->rightColumn << endl;
            }
            // 垂直分片
            else {
              // 不是主键，则更新remainedKey为其他键,
              if (cName != primaryKey[tName]) {
                remainedKey[tName] = cName;
                locateVerticalFrag(tName, cName, involvedFragments);
              }
            }
          }
          // 所有where子句遍历后仍有垂直分片主键存留
          map<string, string>::iterator key;
          for (key = remainedKey.begin(); key != remainedKey.end(); key++) {
            string tName = key->first;
            string cName = key->second;
            locateVerticalFrag(tName, cName, involvedFragments);
          }
        }

        // cout << "\nTotally involves: " << endl;
        // for (j = involvedFragments.begin(); j != involvedFragments.end();
        // j++) {
        //   cout << *j << endl;
        // }

        // 基于该select语句生成各站点的sql语句
        genSQLForSites(involvedFragments, tables);
        // cout << endl << "*** SQL for each sites ***" << endl << endl;
        // map<string, vector<string>>::iterator ss;
        // for (ss = siteToSQL.begin(); ss != siteToSQL.end(); ss++) {
        //   cout << ss->first << ":" << endl;
        //   for (vector<string>::iterator s = ss->second.begin();
        //        s != ss->second.end(); s++) {
        //     cout << *s << endl;
        //   }
        //   cout << endl;
        // }
      }

      // Print a statement summary.
      // hsql::printStatementInfo(result.getStatement(i));
    }
    return std::make_pair(fragType, siteToSQL);
  } else {
    auto msg = fmt::format("Invalid statement: {} at {}:{}", result.errorMsg(),
                           result.errorLine(), result.errorColumn());
    throw std::runtime_error(msg);
  }
}

void findCondition(hsql::Expr *expr) {

  // 判断是否是运算符
  // ExprType[kExprOperator]
  bool isOp = (expr->type == 10);
  // printf("IsOp: %d\n", isOp);
  // printf("OpNum: %d\n", expr->opType); // kOpAnd

  // 是运算符，则取操作数（暂时只考虑and,or等双目运算符）
  if (isOp) {
    findCondition(expr->expr);
    if (expr->opType == 19) {
      cnt = 0;
      whereCondition.push_back(con);
      // printf("AND\n");
    } else {
      // 运算符
      cnt += 1;
      con.opType = expr->opType;
      // printf("%d\n", expr->opType);
    }
    findCondition(expr->expr2);
    return;
  } else {
    cnt += 1;
    // 属性
    if (expr->name != nullptr) {
      // 表名.列名参数
      if (expr->table != nullptr) {
        // 左操作数
        if (cnt == 1) {
          con.leftValType = 2;
          con.leftTable = expr->table;
          con.leftColumn = expr->name;
        }
        // 右操作数
        else if (cnt == 3) {
          con.rightValType = 2;
          con.rightTable = expr->table;
          con.rightColumn = expr->name;
        }
        // printf("%s.%s\n", expr->table, expr->name);
      } else {
        // 左操作数
        if (cnt == 1) {
          con.leftValType = 1;
          con.leftTable = BLANK_STR;
          con.leftColumn = expr->name;
        }
        // 右操作数
        else if (cnt == 3) {
          con.rightValType = 1;
          con.rightStr = expr->name;
        }
        // printf("%s\n", expr->name);
      }
    } else {
      // 左操作数
      if (cnt == 1) {

      }
      // 右操作数
      else if (cnt == 3) {
        con.rightValType = 0;
        con.rightVal = expr->ival;
      }
      // printf("%ld\n", expr->ival);
    }
    return;
  }
}

void Stringsplit(const string &str, const string &split, vector<string> &res) {
  // std::regex ws_re("\\s+"); // 正则表达式,匹配空格
  std::regex reg(split); // 匹配split
  std::sregex_token_iterator pos(str.begin(), str.end(), reg, -1);
  decltype(pos) end; // 自动推导类型
  for (; pos != end; ++pos) {
    res.push_back(pos->str());
  }
}

// create <v/h>fragment <frag_name> [by <columns>] from <table> [where
// <condition>] on <site_name> v: create vfragment f1 by id,name from publisher
// on node0 h: create hfragment f1 from publisher where id<104000 and name='prc'
// on node0
void initFragInfo() {

  // 单个字符分词
  vector<string> strList;
  string str = "create vfragment f1 by id,name from publisher on node0";
  Stringsplit(str, " ", strList);
  for (auto s : strList)
    cout << s << " ";
  cout << endl;

  // 使用字符串分词
  vector<string> strList2;
  string str2("create hfragment f1 from publisher where id<104000 and "
              "name='prc' on node0");
  Stringsplit(str2, " ", strList2);
  for (auto s : strList2)
    cout << s << " ";
  cout << endl;

  // 先判断是何种分片
  int typeIndex = str.find("fragment");
  // 垂直分片
  if (str[typeIndex - 1] == 'v') {
    cout << "Creating vertical fragment!" << endl;
  } else
    cout << "Creating horizontal fragment!";
}

ExprTriple initExprTriple(int leftValType, string leftTable, string leftColumn,
                          int opType, int rightValType, int rightVal,
                          string rightStr, string rightTable,
                          string rightColumn) {
  ExprTriple cur_expr;
  cur_expr.leftValType = leftValType;
  cur_expr.leftTable = leftTable;
  cur_expr.leftColumn = leftColumn;
  cur_expr.opType = opType;
  cur_expr.rightValType = rightValType;
  cur_expr.rightVal = rightVal;
  cur_expr.rightStr = rightStr;
  cur_expr.rightTable = rightTable;
  cur_expr.rightColumn = rightColumn;
  return cur_expr;
}

bool ExprTripleValCompare(int opType1, int val1, int opType2, int val2) {
  switch (opType1) {
  case kOpEquals:
    if (opType2 == kOpEquals && val2 == val1)
      return true;
    break;
  case kOpGreater:
    if (opType2 == kOpEquals && val2 > val1)
      return true;
    else if (opType2 == kOpGreater || opType2 == kOpGreaterEq)
      return true;
    else if ((opType2 == kOpLess || opType2 == kOpLessEq) && (val2 > val1))
      return true;
    else
      break;
  case kOpGreaterEq:
    if (opType2 == kOpEquals && val2 >= val1)
      return true;
    else if (opType2 == kOpGreater || opType2 == kOpGreaterEq)
      return true;
    else if (opType2 == kOpLess && val2 > val1)
      return true;
    else if (opType2 == kOpLessEq && val2 >= val1)
      return true;
    else
      break;
  case kOpLess:
    if (opType2 == kOpEquals && val2 < val1)
      return true;
    else if (opType2 == kOpLess || opType2 == kOpLessEq)
      return true;
    else if ((opType2 == kOpGreater || opType2 == kOpGreaterEq) &&
             (val2 < val1))
      return true;
    else
      break;
  case kOpLessEq:
    if (opType2 == kOpEquals && val2 <= val1)
      return true;
    else if (opType2 == kOpLess || opType2 == kOpLessEq)
      return true;
    else if (opType2 == kOpGreater && val2 < val1)
      return true;
    else if (opType2 == kOpGreaterEq && val2 <= val1)
      return true;
    else
      break;
  case kOpNotEquals:
    if (opType2 == kOpEquals && val2 != val1)
      return true;
    else
      break;
  default:
    break;
  }
  return false;
}

// 水平分片：右操作数为数字/纯字符串的条件比较
// iterFragments每次遍历为空，用于存储更新后的involvedFragments
void getInvolvedFrag(ExprTriple cur_expr, set<string> &involvedFragments,
                     set<string> iterFragments) {

  // 当前遍历条件的表名和列名
  string tName = cur_expr.leftTable;
  string cName = cur_expr.leftColumn;

  // 遍历当前所需分片集合，set<string>
  set<string>::iterator i;
  for (i = involvedFragments.begin(); i != involvedFragments.end(); i++) {

    // 当前遍历的分片名
    string fName = *i;
    // 如果当前遍历分片与当前条件同属一个表，有可能缩小查找范围
    if (FT[tName].find(fName) != FT[tName].end()) {

      bool isRequired = true;

      // 遍历分片条件集合，看是否有交集
      vector<ExprTriple> exprList = FT[tName][fName];
      vector<ExprTriple>::iterator j;

      // 数值比较
      if (cur_expr.rightValType == 0) {
        for (j = exprList.begin(); j != exprList.end(); j++) {
          // 分片条件是相同列名的条件，对于相同列名的分片条件，必须全满足才取
          if (cur_expr.leftColumn == j->leftColumn) {
            // 查看是否有交集，如果相同列名分片条件有不交，则不取
            if (!ExprTripleValCompare(cur_expr.opType, cur_expr.rightVal,
                                      j->opType, j->rightVal)) {
              isRequired = false;
              break;
            }
          }
        }
        if (isRequired)
          iterFragments.insert(fName);
        continue;
      }

      // 字符串，判断是否相等
      if (cur_expr.rightValType == 1) {
        for (j = exprList.begin(); j != exprList.end(); j++) {
          // 分片条件是相同列名的条件
          if (cur_expr.leftColumn == j->leftColumn &&
              cur_expr.rightStr == j->rightStr) {
            // 查看字符串是否相等
            iterFragments.insert(fName);
          }
        }
        continue;
      }
    }
    // 不属于同一个表，没有找到对应的条件
    else
      iterFragments.insert(fName);
  }

  /* 调试用打印
  set<string>::iterator j;
  for(j=iterFragments.begin();j!=iterFragments.end();j++){
      cout << *j << endl;
  }
  */

  // 更新之后查找的分片集合
  involvedFragments = iterFragments;
}

void locateVerticalFrag(string tName, string cName,
                        set<string> &involvedFragments) {
  map<string, vector<ExprTriple>>::iterator i;
  for (i = FT[tName].begin(); i != FT[tName].end(); i++) {
    vector<ExprTriple>::iterator e;
    vector<ExprTriple> exprList = i->second;
    for (e = exprList.begin(); e != exprList.end(); e++) {
      if (e->leftColumn == cName) {
        involvedFragments.insert(i->first);
        return;
      }
    }
  }
}

vector<ExprTriple> addNewCondition(pair<string, string> leftAttr,
                                   pair<string, string> rightAttr) {

  // 寻找关联键的where子句条件
  // e.g 寻找和book.id关联的键是否有条件
  set<pair<string, string>>::iterator relatedAttr;
  set<pair<string, string>> Attributes = keyMap[leftAttr];
  // 要新增的约束条件
  vector<ExprTriple> addedCondition;
  for (relatedAttr = Attributes.begin(); relatedAttr != Attributes.end();
       relatedAttr++) {

    // 从头遍历where子句集合，对所有非连接操作的条件，按照主外键关系添加新的条件
    // e.g 如果book.id有约束，则添加以book.id为外键relatedAttr的相同约束
    vector<ExprTriple>::iterator j;

    for (j = whereCondition.begin(); j != whereCondition.end(); j++) {
      // 非连接
      if (!(j->leftValType == 2 && j->rightValType == 2 &&
            j->opType == kOpEquals)) {
        printExprTriple(*j);
        // 左操作数是关联的键
        if (leftAttr.first == j->leftTable &&
            leftAttr.second == j->leftColumn) {
          ExprTriple new_expr = *j;
          new_expr.leftTable = relatedAttr->first;
          new_expr.leftColumn = relatedAttr->second;
          printf("Add new condition: %s.%s %d %d %s\n",
                 new_expr.leftTable.c_str(), new_expr.leftColumn.c_str(),
                 new_expr.opType, new_expr.rightVal, new_expr.rightStr.c_str());
          addedCondition.push_back(new_expr);
        }
      }
    }
  }

  // 对于右操作数同理
  // 寻找关联键的where子句条件
  // e.g 寻找和book.id关联的键是否有条件
  Attributes = keyMap[rightAttr];
  for (relatedAttr = Attributes.begin(); relatedAttr != Attributes.end();
       relatedAttr++) {

    // 从头遍历where子句集合，对所有非连接操作的条件，按照主外键关系添加新的条件
    // e.g 如果book.id有约束，则添加以book.id为外键relatedAttr的相同约束
    vector<ExprTriple>::iterator j;

    for (j = whereCondition.begin(); j != whereCondition.end(); j++) {
      // 非连接
      if (!(j->leftValType == 2 && j->rightValType == 2 &&
            j->opType == kOpEquals)) {
        // 左操作数是关联的键
        if (rightAttr.first == j->leftTable &&
            rightAttr.second == j->leftColumn) {
          ExprTriple new_expr = *j;
          new_expr.leftTable = relatedAttr->first;
          new_expr.leftColumn = relatedAttr->second;
          printf("Add new condition: %s.%s %d %d %s\n",
                 new_expr.leftTable.c_str(), new_expr.leftColumn.c_str(),
                 new_expr.opType, new_expr.rightVal, new_expr.rightStr.c_str());
          addedCondition.push_back(new_expr);
        }
      }
    }
  }

  return addedCondition;
}

void genSQLForSites(set<string> involvedFragments, vector<string> tables) {

  sort(select_attributes.begin(), select_attributes.end(), attributeSort);

  // 遍历select子句中的需要的表名.列名
  vector<pair<string, string>>::iterator attr = select_attributes.begin();
  // select * 单独考虑
  if (attr->first == "*") {
    // 对于from子句中的所有表，为他们的分片生成sql语句
    for (vector<string>::iterator t = tables.begin(); t != tables.end(); t++) {
      string tName = *t;
      // 从FT中遍历当前表的分片，如果它的一个分片在involvedFragments里面，则为这个分片对应的站点生成sql语句
      map<string, vector<ExprTriple>>::iterator frag;
      for (frag = FT[tName].begin(); frag != FT[tName].end(); frag++) {
        string fName = frag->first;
        if (involvedFragments.find(fName) != involvedFragments.end()) {
          // 找到需要生成SQL的分片的站点
          string sName = fragToSite[fName];
          // 生成sql语句
          string sql = "select * from " + fName;
          // 生成where子句条件
          if (whereCondition.size() != 0) {
            sql += " where ";
            vector<ExprTriple>::iterator e;
            for (e = whereCondition.begin(); e != whereCondition.end(); e++) {
              if (e->leftTable == tName && e->rightValType == 0) {
                sql += fName + "." + e->leftColumn + " " + parseOp(e->opType) +
                       " " + to_string(e->rightVal) + " and ";
              } else if (e->leftTable == tName && e->rightValType == 1) {
                sql += fName + "." + e->leftColumn + " " + parseOp(e->opType) +
                       " " + e->rightStr + " and ";
              }
            }
            // 去除末尾" and"
            sql = sql.substr(0, sql.length() - 5);
          }
          // cout << sName << ": " << sql << endl;
          siteToSQL[sName].push_back(sql);
        }
      }
    }
  } else {
    while (attr != select_attributes.end()) {
      // 当前的表名
      string tName = attr->first;
      set<string> cNameList;
      cNameList.insert(attr->second);
      // 如果该表还有其他的列要选择
      attr++;
      while (attr != select_attributes.end() && attr->first == tName) {
        cNameList.insert(attr->second);
        attr++;
      }

      // 从FT中遍历当前表的分片，如果它的一个分片在involvedFragments里面，则为这个分片对应的站点生成sql语句
      map<string, vector<ExprTriple>>::iterator frag;
      for (frag = FT[tName].begin(); frag != FT[tName].end(); frag++) {
        string fName = frag->first;
        if (involvedFragments.find(fName) != involvedFragments.end()) {
          // 找到需要生成SQL的分片的站点
          string sName = fragToSite[fName];

          // 如果该表格是水平分片
          if (howToFrag[tName] == 0) {
            // 生成sql语句
            string sql = "select ";
            for (set<string>::iterator i = cNameList.begin();
                 i != cNameList.end(); i++) {
              sql += fName + '.' + *i + ", ";
            }
            sql = sql.substr(0, sql.length() - 2); // 去除多余", "
            sql += " from " + fName;
            // 生成where子句条件
            if (whereCondition.size() != 0) {
              bool hasWhere = false;
              sql += " where ";
              vector<ExprTriple>::iterator e;
              for (e = whereCondition.begin(); e != whereCondition.end(); e++) {
                if (e->leftTable == tName && e->rightValType == 0) {
                  sql += fName + "." + e->leftColumn + " " +
                         parseOp(e->opType) + " " + to_string(e->rightVal) +
                         " and ";
                  hasWhere = true;
                } else if (e->leftTable == tName && e->rightValType == 1) {
                  sql += fName + "." + e->leftColumn + " " +
                         parseOp(e->opType) + " " + e->rightStr + " and ";
                  hasWhere = true;
                }
              }
              if (hasWhere) {
                // 去除末尾" and"
                sql = sql.substr(0, sql.length() - 5);
              } else
                sql = sql.substr(0, sql.length() - 7);
            }
            // cout << sName << ": " << sql << endl;
            siteToSQL[sName].push_back(sql);
          }
          // 垂直分片
          else {
            set<string> have; // 该垂直分片里包含的所需属性
            have.insert(primaryKey[tName]); // 垂直分片必须包含主键

            // 找到该垂直分片所包含的列
            vector<ExprTriple>::iterator e;
            for (e = FT[tName][fName].begin(); e != FT[tName][fName].end();
                 e++) {
              if (cNameList.find(e->leftColumn) != cNameList.end())
                have.insert(e->leftColumn);
            }

            // 生成sql语句
            string sql = "select ";
            for (set<string>::iterator i = have.begin(); i != have.end(); i++) {
              sql += fName + '.' + *i + ", ";
            }
            sql = sql.substr(0, sql.length() - 2); // 去除多余", "
            sql += " from " + fName;
            // 生成where子句条件
            if (whereCondition.size() != 0) {
              bool hasWhere = false;
              sql += " where ";
              vector<ExprTriple>::iterator e;
              for (e = whereCondition.begin(); e != whereCondition.end(); e++) {
                // 对于垂直分片来说，不在本分片内的列的约束条件不考虑
                if (e->leftTable == tName &&
                    have.find(e->leftColumn) != have.end() &&
                    e->rightValType == 0) {
                  sql += fName + "." + e->leftColumn + " " +
                         parseOp(e->opType) + " " + to_string(e->rightVal) +
                         " and ";
                  hasWhere = true;
                } else if (e->leftTable == tName &&
                           have.find(e->leftColumn) != have.end() &&
                           e->rightValType == 1) {
                  sql += fName + "." + e->leftColumn + " " +
                         parseOp(e->opType) + " " + e->rightStr + " and ";
                  hasWhere = true;
                }
              }
              if (hasWhere) {
                // 去除末尾" and"
                sql = sql.substr(0, sql.length() - 5);
              } else
                sql = sql.substr(0, sql.length() - 7);
            }
            // cout << sName << ": " << sql << endl;
            siteToSQL[sName].push_back(sql);
          }
        }
      }
    }
  }
}

void fragInit() {
  // P1
  string tName = "publisher";
  howToFrag[tName] = 0;
  primaryKey[tName] = "id";
  string fName = "p1";
  string sName = "node0";
  // expr1: publisher.id < 104000
  string cName = "id";
  int opType = kOpLess;
  int val = 104000;
  // ExprTriple initExprTriple(int leftValType, string leftTable, string
  // leftColumn, int opType, int rightValType, int rightVal, string rightStr,
  // string rightTable, string rightColumn)
  ExprTriple cur_expr = initExprTriple(2, tName, cName, opType, 0, val,
                                       BLANK_STR, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: nation = 'PRC'
  cName = "nation";
  string str = "PRC";
  opType = kOpEquals;
  cur_expr =
      initExprTriple(2, tName, cName, opType, 1, 0, str, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // P2
  fName = "p2";
  sName = "node1";
  // expr1: publisher.id < 104000
  cName = "id";
  opType = kOpLess;
  val = 104000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: nation = 'USA'
  cName = "nation";
  str = "USA";
  opType = kOpEquals;
  cur_expr =
      initExprTriple(2, tName, cName, opType, 1, 0, str, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // P3
  fName = "p3";
  sName = "node2";
  // expr1: publisher.id >= 104000
  cName = "id";
  opType = kOpGreaterEq;
  val = 104000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: nation = 'PRC'
  cName = "nation";
  str = "PRC";
  opType = kOpEquals;
  cur_expr =
      initExprTriple(2, tName, cName, opType, 1, 0, str, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // P4
  fName = "p4";
  sName = "node3";
  // expr1: publisher.id >= 104000
  cName = "id";
  opType = kOpGreaterEq;
  val = 104000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: nation = 'PRC'
  cName = "nation";
  str = "USA";
  opType = kOpEquals;
  cur_expr =
      initExprTriple(2, tName, cName, opType, 1, 0, str, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // B1
  tName = "book";
  primaryKey[tName] = "id";
  howToFrag[tName] = 0;
  fName = "b1";
  sName = "node0";
  // expr1: book.id < 205000
  cName = "id";
  opType = kOpLess;
  val = 205000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // B2
  fName = "b2";
  sName = "node1";
  // expr1: book.id >= 205000
  cName = "id";
  opType = kOpGreaterEq;
  val = 205000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: book.id < 210000
  opType = kOpLess;
  val = 210000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // B3
  fName = "b3";
  sName = "node2";
  // expr1: book.id >= 210000
  cName = "id";
  opType = kOpGreaterEq;
  val = 210000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // C1
  tName = "customer";
  primaryKey[tName] = "id";
  howToFrag[tName] = 1;
  fName = "c1";
  sName = "node0";
  // expr1: (id,name)
  cName = "id";
  cur_expr =
      initExprTriple(2, tName, cName, 0, 0, 0, BLANK_STR, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  cName = "name";
  cur_expr =
      initExprTriple(2, tName, cName, 0, 0, 0, BLANK_STR, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // C2
  fName = "c2";
  sName = "node1";
  // expr1: (id,name)
  cName = "id";
  cur_expr =
      initExprTriple(2, tName, cName, 0, 0, 0, BLANK_STR, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  cName = "rank";
  cur_expr =
      initExprTriple(2, tName, cName, 0, 0, 0, BLANK_STR, BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // O1
  tName = "orders";
  howToFrag[tName] = 0;
  fName = "o1";
  sName = "node0";
  // expr1: orders.customer_id < 307000
  cName = "customer_id";
  opType = kOpLess;
  val = 307000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: order.book_id < 215000
  cName = "book_id";
  val = 215000;
  opType = kOpLess;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // O2
  fName = "o2";
  sName = "node1";
  // expr1: orders.customer_id < 307000
  cName = "customer_id";
  opType = kOpLess;
  val = 307000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: order.book_id >= 215000
  cName = "book_id";
  val = 215000;
  opType = kOpGreaterEq;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // O3
  fName = "o3";
  sName = "node2";
  // expr1: orders.customer_id >= 307000
  cName = "customer_id";
  opType = kOpGreaterEq;
  val = 307000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: order.book_id < 215000
  cName = "book_id";
  val = 215000;
  opType = kOpLess;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  // O4
  fName = "o4";
  sName = "node3";
  // expr1: orders.customer_id >= 307000
  cName = "customer_id";
  opType = kOpGreaterEq;
  val = 307000;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  // expr2: order.book_id >= 215000
  cName = "book_id";
  val = 215000;
  opType = kOpGreaterEq;
  cur_expr = initExprTriple(2, tName, cName, opType, 0, val, BLANK_STR,
                            BLANK_STR, BLANK_STR);
  FT[tName][fName].push_back(cur_expr);
  SFT[sName][fName].push_back(cur_expr);
  fragToSite[fName] = sName;

  string primaryKey = "id";
  string primaryKeyTable = "publisher";
  string foreignKey = "publisher_id";
  string foreignKeyTable = "book";
  keyMap[pair<string, string>(primaryKeyTable, primaryKey)].insert(
      pair<string, string>(foreignKeyTable, foreignKey));
  keyMap[pair<string, string>(foreignKeyTable, foreignKey)].insert(
      pair<string, string>(primaryKeyTable, primaryKey));

  primaryKey = "id";
  primaryKeyTable = "customer";
  foreignKey = "customer_id";
  foreignKeyTable = "orders";
  keyMap[pair<string, string>(primaryKeyTable, primaryKey)].insert(
      pair<string, string>(foreignKeyTable, foreignKey));
  keyMap[pair<string, string>(foreignKeyTable, foreignKey)].insert(
      pair<string, string>(primaryKeyTable, primaryKey));

  primaryKey = "id";
  primaryKeyTable = "book";
  foreignKey = "book_id";
  foreignKeyTable = "orders";
  keyMap[pair<string, string>(primaryKeyTable, primaryKey)].insert(
      pair<string, string>(foreignKeyTable, foreignKey));
  keyMap[pair<string, string>(foreignKeyTable, foreignKey)].insert(
      pair<string, string>(primaryKeyTable, primaryKey));
}
} // namespace

void qpFragInit() { fragInit(); }

std::pair<int, std::map<std::string, std::vector<std::string>>>
qpParseSQL(std::string sql) {
  return parseQuery(sql);
}
