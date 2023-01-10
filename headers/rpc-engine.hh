#ifndef _RPC_ENGINE_HH
#define _RPC_ENGINE_HH

#include "SQLiteCpp/Database.h"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <exception>
#include <filesystem>
#include <fmt/core.h>
#include <fstream>
#include <iterator>
#include <memory>
#include <seastar/core/when_all.hh>
#include <sstream>
#include <string>

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc.hh>

#include <SQLiteCpp/SQLiteCpp.h>

#include <config.hpp>
#include <queryparser.hh>
#include <serializer.hpp>

#include <parsesql.hh>

class SqlRpcEngine {
  using SqlFunc = std::vector<std::vector<std::string>>(std::string);
  using InsertFunc = int(std::string, std::vector<std::vector<std::string>>);
  using ControlFunc = int(std::string, std::string);
  AppConfig &config;
  std::shared_ptr<SQLite::Database> pdb;
  std::map<std::string, std::shared_ptr<SQLite::Database>> db_conns;
  std::map<std::string, std::shared_ptr<DatabaseMetadata>> db_metas;
  rpc::protocol<serializer> rpc_proto;

  std::unique_ptr<rpc::server> pserver;
  std::map<std::string, std::unique_ptr<rpc::client>> pclients;

  decltype(rpc_proto.register_handler(1, (SqlFunc *)nullptr)) rpc_sql_exec;
  decltype(rpc_proto.register_handler(1,
                                      (InsertFunc *)nullptr)) rpc_insert_exec;
  decltype(rpc_proto.register_handler(1, (ControlFunc *)nullptr)) rpc_control;

  DatabaseMetadata *pdb_meta = nullptr;

  enum { RPC_SQL_EXEC = 1, RPC_INSERT_DATA = 2, RPC_CONTROL = 3 };

  std::vector<std::string> sites;

public:
  void init_db_meta() {
    for (auto &&[sname, sconfig] : config.nodes) {
      sites.push_back(sname);
    }

    // std::string line;
    // std::ifstream ifs(config.frag_filename);
    // while (std::getline(ifs, line)) {
    //   if (line.size())
    //     processCreateMeta(line, pdb_meta);
    // }
  }

  void add_db_conn(std::string dbname) {
    std::string filename = dbname + "_" + config.name + ".db";
    std::cout << "add sqlite connection: " << filename << std::endl;

    bool database_need_init = !std::filesystem::exists(filename);

    auto new_db = std::make_shared<SQLite::Database>(
        filename, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);
    auto new_meta = std::make_shared<DatabaseMetadata>();

    db_conns[dbname] = new_db;
    db_metas[dbname] = new_meta;
    new_meta->sites = sites;

    if (database_need_init) {
      SQLite::Transaction transaction(*new_db);
      std::string line = "create table frags (text char(1024));";
      std::cout << line << std::endl;
      new_db->exec(line);
      transaction.commit();
    } else {
      try {
        SQLite::Statement query(*new_db, "select text from frags");

        while (query.executeStep()) {
          processCreateMeta(query.getColumn(0), new_meta.get());
        }
      } catch (std::exception &e) {
        std::cout << e.what() << std::endl;
      }
    }
  }

  SqlRpcEngine(AppConfig &config) : config(config), rpc_proto(serializer{}) {
    init_db_meta();

    rpc_proto.register_handler(
        RPC_SQL_EXEC, [this](std::string sql) { return local_exec_sql(sql); });

    rpc_proto.register_handler(
        RPC_INSERT_DATA, [this](std::string tablename,
                                std::vector<std::vector<std::string>> data) {
          return local_insert(tablename, data);
        });

    rpc_proto.register_handler(RPC_CONTROL,
                               [this](std::string cmd, std::string type) {
                                 sql_control(cmd, type);
                                 return 0;
                               });

    pserver = std::make_unique<rpc::protocol<serializer>::server>(
        rpc_proto,
        ipv4_addr{"0.0.0.0", std::get<1>(config.nodes[config.name])});

    fmt::print("RPC server started at {}:{}\n", "0.0.0.0",
               std::get<1>(config.nodes[config.name]));
  }

  seastar::future<void> clients_init() {
    rpc_sql_exec =
        rpc_proto
            .make_client<std::vector<std::vector<std::string>>(std::string)>(
                RPC_SQL_EXEC);

    rpc_insert_exec = rpc_proto.make_client<InsertFunc>(RPC_INSERT_DATA);
    rpc_control = rpc_proto.make_client<ControlFunc>(RPC_CONTROL);

    for (auto [name, info] : config.nodes) {
      pclients.emplace(std::make_pair(
          name,
          std::make_unique<rpc::protocol<serializer>::client>(
              rpc_proto, ipv4_addr{std::get<0>(info), std::get<1>(info)})));
    }

    return seastar::make_ready_future<>();
  }

  std::vector<std::vector<std::string>> local_exec_sql(std::string sql) {
    fmt::print("RPC sql: {}\n", sql);
    std::vector<std::vector<std::string>> ret;
    SQLite::Statement query(*pdb, sql);
    ret.emplace_back();

    for (int i = 0, end = query.getColumnCount(); i < end; i++)
      ret.back().emplace_back(query.getColumnOriginName(i));

    while (query.executeStep()) {
      ret.emplace_back();
      for (int i = 0, end = query.getColumnCount(); i < end; i++) {
        ret.back().emplace_back(query.getColumn(i));
      }
    }

    return ret;
  }

  int local_insert(std::string table_name,
                   std::vector<std::vector<std::string>> rows) {
    std::stringstream sql_ss;
    sql_ss << "INSERT INTO " << table_name << " (";
    sql_ss << boost::algorithm::join(rows[0], ", ");
    sql_ss << ") VALUES (";
    sql_ss << boost::algorithm::join(
        std::vector<std::string>(rows[0].size(), "?"), ", ");
    sql_ss << ");";
    auto sql = sql_ss.str();

    SQLite::Transaction transaction(*pdb);
    SQLite::Statement query(*pdb, sql);

    for (int i = 1; i < rows.size(); i++) {
      for (int j = 0; j < rows[i].size(); j++)
        query.bind(j + 1, rows[i][j]);

      query.executeStep();
      query.reset();
    }
    transaction.commit();

    return 0;
  }

  seastar::future<std::vector<std::vector<std::string>>>
  exec_query_node(BasicNode *node) {
    if (node->disabled) {
      if (auto projection = dynamic_cast<ProjectionNode *>(node)) {
        return seastar::make_ready_future<
            std::vector<std::vector<std::string>>>(
            std::vector<std::vector<std::string>>{projection->column_names});
      } else
        return seastar::make_ready_future<
            std::vector<std::vector<std::string>>>();
    }

    if (auto projection = dynamic_cast<ProjectionNode *>(node)) {
      auto result = exec_query_node(projection->child.get()).get();
      std::vector<std::vector<std::string>> new_result;
      std::vector<int> keep_columns;

      for (int i = 0; i < result[0].size(); i++) {
        if (std::find(projection->column_names.begin(),
                      projection->column_names.end(),
                      result[0][i]) != projection->column_names.end())
          keep_columns.push_back(i);
      }

      for (auto &&row : result) {
        new_result.emplace_back();
        auto &val = new_result.back();
        for (auto i : keep_columns) {
          val.emplace_back(std::move(row[i]));
        }
      }

      // for (auto &&row : new_result) {
      //   std::cout << boost::algorithm::join(row, ", ") << std::endl;
      // }

      node->result = new_result.size() - 1;

      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          new_result);
    } else if (auto njoin = dynamic_cast<NJoinNode *>(node)) {
      std::vector<seastar::future<std::vector<std::vector<std::string>>>> futs;

      for (auto ch : njoin->join_children) {
        futs.emplace_back(std::move(exec_query_node(ch.get())));
      }

      auto child_result_futs =
          seastar::when_all(futs.begin(), futs.end()).get();

      std::vector<std::vector<std::string>> result;
      std::vector<std::multimap<std::string, std::vector<std::string>>>
          sorted_results;
      std::set<std::string> visited_keys;
      std::set<std::string> join_colnames(njoin->join_column_names.begin(),
                                          njoin->join_column_names.end());

      result.emplace_back();
      result.back().push_back(njoin->join_column_names.front());

      for (auto &&r : child_result_futs) {
        sorted_results.emplace_back();

        auto &&child_result = r.get();
        auto &&child_cols = child_result[0];

        int join_ind = 0;

        for (int i = 0; i < child_cols.size(); i++) {
          if (join_colnames.count(child_cols[i]) != 0) {
            join_ind = i;
          } else {
            result.back().push_back(child_cols[i]);
          }
        }

        for (int i = 1; i < child_result.size(); i++) {
          std::vector<std::string> row;
          for (int j = 0; j < child_result[i].size(); j++) {
            if (j != join_ind)
              row.push_back(child_result[i][j]);
          }
          sorted_results.back().insert({child_result[i][join_ind], row});
        }
      }

      for (auto &&[val, rem] : sorted_results[0]) {

        if (visited_keys.count(val))
          continue;

        visited_keys.insert(val);

        bool have = true;
        for (int i = 1; i < sorted_results.size(); i++) {
          if (sorted_results[i].count(val) == 0) {
            have = false;
            break;
          }
        }

        if (!have)
          continue;

        auto nresult = dfs_join(val, sorted_results);
        std::move(nresult.begin(), nresult.end(), std::back_inserter(result));
      }

      if (njoin->change_all_table_name) {
        for (auto &name : result[0]) {
          auto [c0, c1] = split_column_name(name);
          name = format_column_name(*njoin->change_all_table_name, c1);
        }
      }

      // for (auto &&row : result) {
      //   std::cout << boost::algorithm::join(row, ", ") << std::endl;
      // }

      node->result = result.size() - 1;

      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          result);
    } else if (auto union_ = dynamic_cast<UnionNode *>(node)) {
      std::vector<std::vector<std::string>> result;
      std::vector<seastar::future<std::vector<std::vector<std::string>>>> futs;

      for (auto child : union_->union_children)
        futs.emplace_back(std::move(exec_query_node(child.get())));

      auto child_result_futs =
          seastar::when_all(futs.begin(), futs.end()).get();

      result = child_result_futs[0].get();

      for (int i = 1; i < child_result_futs.size(); i++) {
        auto result1 = child_result_futs[i].get();
        std::move(result1.begin() + 1, result1.end(),
                  std::back_inserter(result));
      }

      if (union_->change_all_table_name) {
        for (auto &name : result[0]) {
          auto [c0, c1] = split_column_name(name);
          name = format_column_name(*union_->change_all_table_name, c1);
        }
      }

      node->result = result.size() - 1;

      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          result);

    } else if (auto rename = dynamic_cast<RenameNode *>(node)) {
      auto result = exec_query_node(rename->child.get()).get();

      for (int i = 0; i < result[0].size(); i++)
        result[0][i] = format_column_name(
            rename->table_name, std::get<1>(split_column_name(result[0][i])));

      node->result = result.size() - 1;

      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          result);

    } else if (auto readtable = dynamic_cast<ReadTableNode *>(node)) {
      auto [site, tablename] = split_column_name(readtable->table_name);
      auto &&table_meta = pdb_meta->tables[readtable->orig_table_name];

      std::stringstream sql_ss;
      sql_ss << "select "
             << boost::algorithm::join(readtable->column_names, ", ")
             << " from " << tablename << " where true";

      for (auto cond : readtable->select_conds) {
        if (table_meta.frag_type == table_meta.VFRAG) {
          auto &&cols = std::get<1>(table_meta.vfrag_cols[site]);
          if (std::find(cols.begin(), cols.end(),
                        std::get<1>(split_column_name(cond.val1))) ==
              cols.end())
            continue;
        }

        sql_ss << " and " << cond.val1 << " " << cond.op << " ";
        if (cond.val2.index() == 0)
          sql_ss << std::get<0>(cond.val2);
        else
          sql_ss << "'" << std::get<1>(cond.val2) << "'";
      }
      sql_ss << ";";

      auto result = rpc_sql_exec(*pclients[site], sql_ss.str()).get();
      for (int i = 0; i < readtable->column_names.size(); i++)
        result[0][i] = format_column_name(
            tablename,
            std::get<1>(split_column_name(readtable->column_names[i])));

      // for (auto &&row : result) {
      //   std::cout << boost::algorithm::join(row, ", ") << std::endl;
      // }

      node->result = result.size() - 1;

      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          result);
    }
  }

  seastar::future<std::string>
  exec_insert_sites(std::map<std::string, InsertStmt> istmt) {
    std::vector<seastar::future<int>> futures;

    for (auto &&[sname, stmt] : istmt) {
      std::vector<std::vector<std::string>> values;

      values.push_back(stmt.columns);

      for (auto &&data : stmt.values) {
        std::vector<std::string> row;
        for (auto val : data) {
          if (val.index() == 0)
            row.push_back(std::to_string(std::get<0>(val)));
          else
            row.push_back(std::get<1>(val));
        }
        values.push_back(row);
      }

      futures.emplace_back(std::move(
          rpc_insert_exec(*pclients[sname], stmt.table_name, values)));
    }

    std::stringstream ss;
    int total = 0;
    for (auto &&[sname, stmt] : istmt) {
      ss << sname << " " << stmt.table_name << " " << stmt.values.size()
         << "\n";
      total += stmt.values.size();
    }
    ss << "TOTAL " << total << "\n";
    auto msg = ss.str();

    return seastar::when_all(futures.begin(), futures.end())
        .then([this, msg](auto futs) {
          for (auto &&fut : futs)
            fut.get();
          return msg;
        });
  }

  seastar::future<std::string> insert_from_file(std::string table,
                                                std::string filename) {
    auto insStmt = insertStmtFromTSV(table, filename, pdb_meta);
    auto site_ins_stmt = insertStmtToSites(insStmt, pdb_meta);

    return exec_insert_sites(std::move(site_ins_stmt));
  }

  void sql_control(std::string command, std::string type) {
    std::cout << "Control cmd " << type << ' ' << command << std::endl;

    if (type == "createdb") {
      add_db_conn(command);
    } else if (type == "usedb") {
      if (db_conns.count(command) == 0)
        add_db_conn(command);
      pdb = db_conns[command];
      pdb_meta = db_metas[command].get();
    } else if (type == "createtable") {
      std::vector<std::string> metas;
      std::vector<std::vector<std::string>> rows;
      auto sqls = parseCreateTable(command, pdb_meta, &metas);
      rows.push_back({{"text"}});
      for (auto meta : metas)
        rows.push_back({{meta}});

      local_insert("frags", rows);

      if (sqls.count(config.name)) {
        local_exec_sql(sqls[config.name]);
      }
    } else if (type == "close") {
      if (config.name == command)
        exit(0);
    }
  }

  seastar::future<std::vector<std::vector<std::string>>>
  exec_sql_(std::string sql) {
    if (boost::starts_with(sql, "createdb")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t;"));

      std::vector<seastar::future<int>> futs_int;
      for (auto sname : sites) {
        futs_int.emplace_back(
            std::move(rpc_control(*pclients[sname], tokens[1], "createdb")));
      }

      return seastar::when_all(futs_int.begin(), futs_int.end())
          .then([](auto futs) -> std::vector<std::vector<std::string>> {
            for (auto &&fut : futs)
              fut.get();
            return {{"created"}};
          });
    } else if (boost::starts_with(sql, "usedb")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t;"));

      std::vector<seastar::future<int>> futs_int;
      for (auto sname : sites) {
        futs_int.emplace_back(
            std::move(rpc_control(*pclients[sname], tokens[1], "usedb")));
      }

      return seastar::when_all(futs_int.begin(), futs_int.end())
          .then([](auto futs) -> std::vector<std::vector<std::string>> {
            for (auto &&fut : futs)
              fut.get();
            return {{"changed"}};
          });
    } else if (boost::starts_with(sql, "close")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t;"));

      std::vector<seastar::future<int>> futs_int;
      for (auto sname : sites) {
        futs_int.emplace_back(
            std::move(rpc_control(*pclients[sname], tokens[1], "close")));
      }

      return seastar::when_all(futs_int.begin(), futs_int.end())
          .then([](auto futs) -> std::vector<std::vector<std::string>> {
            for (auto &&fut : futs)
              fut.get();
            return {{"closed"}};
          });
    }

    if (pdb == nullptr) {
      return seastar::make_ready_future<>().then(
          []() -> std::vector<std::vector<std::string>> {
            return {{"no database selected"}};
          });
    }

    if (boost::starts_with(sql, "import")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t"));

      return insert_from_file(tokens[1], tokens[2])
          .then([](std::string s) -> std::vector<std::vector<std::string>> {
            return {{s}};
          });
    } else if (boost::starts_with(sql, "insert")) {
      auto insert = parseInsertStmt(sql, pdb_meta);
      auto sites = insertStmtToSites(insert, pdb_meta);

      return exec_insert_sites(std::move(sites))
          .then([](std::string s) -> std::vector<std::vector<std::string>> {
            return {{s}};
          });
    } else if (boost::starts_with(sql, "delete")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t;"));

      auto tablename = tokens[2];

      std::vector<seastar::future<std::vector<std::vector<std::string>>>> futs;

      for (auto &&[sname, sdata] : pdb_meta->tables[tablename].hfrag_conds) {
        auto &&[fname, data] = sdata;
        futs.emplace_back(
            std::move(rpc_sql_exec(*pclients[sname], "delete from " + fname)));
      }

      for (auto &&[sname, sdata] : pdb_meta->tables[tablename].vfrag_cols) {
        auto &&[fname, data] = sdata;
        futs.emplace_back(
            std::move(rpc_sql_exec(*pclients[sname], "delete from " + fname)));
      }

      return seastar::when_all(futs.begin(), futs.end())
          .then([](auto futs) -> std::vector<std::vector<std::string>> {
            for (auto &&fut : futs)
              fut.get();
            return {{"deleted"}};
          });
    } else if (boost::starts_with(sql, "createtable")) {
      std::vector<seastar::future<int>> futs_int;
      for (auto sname : sites) {
        futs_int.emplace_back(
            std::move(rpc_control(*pclients[sname], sql, "createtable")));
      }

      return seastar::when_all(futs_int.begin(), futs_int.end())
          .then([](auto futs) -> std::vector<std::vector<std::string>> {
            for (auto &&fut : futs)
              fut.get();
            return {{"created"}};
          });
    }

    auto result = parseSelectStmt(sql, pdb_meta);
    auto node = buildRawNodeTreeFromSelectStmt(result, pdb_meta);
    pushDownAndOptimize(node.get(), {}, {}, "", pdb_meta);
    auto copy = node->copy(pdb_meta);

    std::cout << copy->to_string() << std::endl;

    return seastar::async([this, copy]() {
      auto ret = exec_query_node(copy.get()).get();
      std::cout << copy->to_string() << std::endl;
      return ret;
    });
  }

  seastar::future<std::vector<std::vector<std::string>>>
  exec_sql(std::string sql) {
    return seastar::make_ready_future<>()
        .then([this, sql]() {
          std::vector<std::string> closed_clients;
          for (auto &&[sname, client] : pclients) {
            if (client->error()) {
              closed_clients.push_back(sname);
            }
          }

          for (auto sname : closed_clients) {
            std::cout << "reconnecting " << sname << std::endl;
            pclients.erase(sname);
            pclients.emplace(std::make_pair(
                sname,
                std::make_unique<rpc::protocol<serializer>::client>(
                    rpc_proto, ipv4_addr{std::get<0>(config.nodes[sname]),
                                         std::get<1>(config.nodes[sname])})));
          }

          return exec_sql_(sql);
        })
        .handle_exception_type([this](seastar::rpc::closed_error &e)
                                   -> std::vector<std::vector<std::string>> {
          for (auto &&[sname, client] : pclients) {
            if (client->error())
              return {{"connection from " + sname + " closed"}};
          }
          return {{e.what()}};
        })
        .handle_exception_type(
            [this](std::exception &e) -> std::vector<std::vector<std::string>> {
              return {{e.what()}};
            });
  }
};

#endif
