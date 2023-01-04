#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cstddef>
#include <deque>
#include <fmt/format.h>
#include <hsql/sql/Expr.h>
#include <hsql/sql/InsertStatement.h>
#include <hsql/sql/SQLStatement.h>
#include <iostream>

#include <fstream>
#include <memory>
#include <parsesql.hh>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <typeinfo>
#include <variant>

#include <SQLiteCpp/ExecuteMany.h>
#include <SQLiteCpp/SQLiteCpp.h>

#include <boost/algorithm/string.hpp>

// include the sql parser
#include "hsql/SQLParser.h"
// contains printing utilities
#include "hsql/util/sqlhelper.h"

int main(void) {
  const std::string query1 =
      "select Customer.name, Book.title, Publisher.name, Orders.quantity from "
      "Customer,Book,Publisher,Orders where Customer.id=Orders.customer_id and "
      "Book.id=Orders.book_id and Book.publisher_id=Publisher.id and "
      "Book.id>210000 and Publisher.nation='PRC' and Orders.customer_id >= "
      "307000 and Orders.book_id < 215000;";
  const std::string query =
      "select Orders.quantity from Orders,Customer where Orders.customer_id = "
      "Customer.id and Customer.id = 300001 and Customer.id = 300002";
  // const std::string query = "select b, c, d from a1, a2 where a1.id = a2.id "
  //                           "and  a1.id = a2.id and  a1.id = a2.id";

  DatabaseMetadata db;
  db.sites = std::vector<std::string>{"node0", "node1", "node2", "node3"};
  processCreateMeta(
      "createmeta t Publisher ON HFRAG where id:int name:str nation:str", &db);
  processCreateMeta("createmeta t Book ON HFRAG where id:int title:str "
                    "authors:str publisher_id:int copies:int",
                    &db);
  processCreateMeta(
      "createmeta t Customer ON VFRAG where id:int name:str rank:int", &db);
  processCreateMeta("createmeta t Orders ON HFRAG where customer_id:int "
                    "book_id:int quantity:int",
                    &db);

  processCreateMeta(
      "createmeta h node0.p1 on Publisher where id < 104000 AND nation='PRC'",
      &db);
  processCreateMeta(
      "createmeta h node1.p2 on Publisher where id < 104000 AND nation='USA'",
      &db);
  processCreateMeta(
      "createmeta h node2.p3 on Publisher where id >= 104000 AND nation='PRC'",
      &db);
  processCreateMeta(
      "createmeta h node3.p4 on Publisher where id >= 104000 AND nation='USA'",
      &db);

  processCreateMeta("createmeta h node0.b1 on Book where id < 205000", &db);
  processCreateMeta(
      "createmeta h node1.b2 on Book where id >= 205000 AND id < 210000", &db);
  processCreateMeta("createmeta h node2.b3 on Book where id >= 210000", &db);

  processCreateMeta("createmeta v node0.c1 on Customer where id name", &db);
  processCreateMeta("createmeta v node1.c2 on Customer where id rank", &db);

  processCreateMeta("createmeta h node0.o1 on Orders where customer_id < "
                    "307000 and book_id < 215000",
                    &db);
  processCreateMeta("createmeta h node1.o2 on Orders where customer_id < "
                    "307000 and book_id >= 215000",
                    &db);
  processCreateMeta("createmeta h node2.o3 on Orders where customer_id >= "
                    "307000 and book_id < 215000",
                    &db);
  processCreateMeta("createmeta h node3.o4 on Orders where customer_id >= "
                    "307000 and book_id >= 215000",
                    &db);

  auto result = parseSelectStmt(query, &db);
  printSelectStmt(result);

  auto node = buildRawNodeTreeFromSelectStmt(result, &db);
  std::cout << node->to_string() << std::endl;

  pushDownAndOptimize(node.get(), {}, {}, "");
  std::cout << node->to_string() << std::endl;

  auto copy = node->copy();
  std::cout << copy->to_string() << std::endl;

  // auto insert = insertStmtFromTSV("Publisher", "publisher.tsv", &db);
  // std::cout << insert.values.size() << std::endl;

  // auto ins_sites = insertStmtToSites(insert, &db);

  // for (auto &&[sname, ins] : ins_sites)
  // {
  //   std::cout << sname << ' ' << ins.values.size() << std::endl;
  // }

  // try {
  //   SQLite::Database db("transaction.db3",
  //                       SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);

  //   db.exec("DROP TABLE IF EXISTS test");

  //   // Begin transaction
  //   SQLite::Transaction transaction(db);

  //   db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");

  //   SQLite::Statement   query(db, "INSERT INTO test VALUES (?, ?)");

  //   for (int i = 0; i < 10; i++)
  //   {
  //     query.bind(1, i);
  //     query.bind(2, std::to_string(i));
  //     query.executeStep();
  //     query.reset();
  //   }

  //   // Commit transaction
  //   transaction.commit();
  // } catch (std::exception &e) {
  //   std::cout << "exception: " << e.what() << std::endl;
  // }

  return 0;
}
