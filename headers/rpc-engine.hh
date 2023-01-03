#ifndef _RPC_ENGINE_HH
#define _RPC_ENGINE_HH

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
  AppConfig &config;
  std::unique_ptr<SQLite::Database> pdb;
  rpc::protocol<serializer> rpc_proto;

  std::unique_ptr<rpc::server> pserver;
  std::map<std::string, std::unique_ptr<rpc::client>> pclients;

  decltype(rpc_proto.register_handler(1, (SqlFunc *)nullptr)) rpc_sql_exec;
  decltype(rpc_proto.register_handler(1,
                                      (InsertFunc *)nullptr)) rpc_insert_exec;

  DatabaseMetadata db_meta;

  enum { RPC_SQL_EXEC = 1, RPC_INSERT_DATA = 2 };

public:
  void init_db_meta() {
    db_meta.sites =
        std::vector<std::string>{"node0", "node1", "node2", "node3"};

    for (auto &&[sname, sconfig] : config.nodes) {
      db_meta.sites.push_back(sname);
    }

    std::string line;
    std::ifstream ifs(config.frag_filename);
    while (std::getline(ifs, line)) {
      if (line.size())
        processCreateMeta(line, &db_meta);
    }
  }

  SqlRpcEngine(AppConfig &config) : config(config), rpc_proto(serializer{}) {
    init_db_meta();
    bool database_need_init = !std::filesystem::exists(config.sqldb_filename);

    pdb = std::make_unique<SQLite::Database>(
        config.sqldb_filename, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);

    if (database_need_init) {
      SQLite::Transaction transaction(*pdb);
      std::string line;
      std::ifstream ifs(config.sqldb_initfile);
      while (std::getline(ifs, line)) {
        std::cout << line << std::endl;
        pdb->exec(line);
      }
      transaction.commit();
    }

    rpc_proto.register_handler(
        RPC_SQL_EXEC, [this](std::string sql) { return local_exec_sql(sql); });

    rpc_proto.register_handler(
        RPC_INSERT_DATA, [this](std::string tablename,
                                std::vector<std::vector<std::string>> data) {
          return local_insert(tablename, data);
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

  seastar::future<int>
  local_insert(std::string table_name,
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

    return seastar::make_ready_future<int>(0);
  }

  seastar::future<std::vector<std::vector<std::string>>>
  exec_query_node(BasicNode *node) {
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
      auto &&table_meta = db_meta.tables[readtable->orig_table_name];

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
        .then([this, msg](auto) { return msg; });
  }

  seastar::future<std::string> insert_from_file(std::string table,
                                                std::string filename) {
    auto insStmt = insertStmtFromTSV(table, filename, &db_meta);
    auto site_ins_stmt = insertStmtToSites(insStmt, &db_meta);

    return exec_insert_sites(std::move(site_ins_stmt));
  }

  seastar::future<std::vector<std::vector<std::string>>>
  exec_sql(std::string sql) {

    if (boost::starts_with(sql, "import")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t"));

      return insert_from_file(tokens[1], tokens[2])
          .then([](std::string s) -> std::vector<std::vector<std::string>> {
            return {{s}};
          });
    } else if (boost::starts_with(sql, "insert")) {
      auto insert = parseInsertStmt(sql, &db_meta);
      auto sites = insertStmtToSites(insert, &db_meta);

      return exec_insert_sites(std::move(sites))
          .then([](std::string s) -> std::vector<std::vector<std::string>> {
            return {{s}};
          });
    } else if (boost::starts_with(sql, "delete")) {
      std::vector<std::string> tokens;
      boost::split(tokens, sql, boost::is_any_of(" \t;"));

      auto tablename = tokens[2];

      std::vector<seastar::future<std::vector<std::vector<std::string>>>> futs;

      for (auto &&[sname, sdata] : db_meta.tables[tablename].hfrag_conds) {
        auto &&[fname, data] = sdata;
        futs.emplace_back(
            std::move(rpc_sql_exec(*pclients[sname], "delete from " + fname)));
      }

      for (auto &&[sname, sdata] : db_meta.tables[tablename].vfrag_cols) {
        auto &&[fname, data] = sdata;
        futs.emplace_back(
            std::move(rpc_sql_exec(*pclients[sname], "delete from " + fname)));
      }

      return seastar::when_all(futs.begin(), futs.end())
          .then([](auto) -> std::vector<std::vector<std::string>> {
            return {{"deleted"}};
          });
    }

    auto result = parseSelectStmt(sql, &db_meta);
    auto node = buildRawNodeTreeFromSelectStmt(result, &db_meta);
    pushDownAndOptimize(node.get(), {}, {}, "");
    auto copy = node->copy();

    std::cout << copy->to_string() << std::endl;

    return seastar::async([this, copy]() {
      auto ret = exec_query_node(copy.get()).get();
      std::cout << copy->to_string() << std::endl;
      return ret;
    });
  }
};

#endif
