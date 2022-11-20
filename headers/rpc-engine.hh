#ifndef _RPC_ENGINE_HH
#define _RPC_ENGINE_HH

#include <exception>
#include <filesystem>
#include <fmt/core.h>
#include <fstream>
#include <memory>
#include <seastar/core/when_all.hh>
#include <string>

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/rpc/rpc.hh>

#include <SQLiteCpp/SQLiteCpp.h>

#include <config.hpp>
#include <queryparser.hh>
#include <serializer.hpp>

class SqlRpcEngine {
  using SqlFunc = std::vector<std::vector<std::string>>(std::string);
  AppConfig &config;
  std::unique_ptr<SQLite::Database> pdb;
  rpc::protocol<serializer> rpc_proto;

  std::unique_ptr<rpc::server> pserver;
  std::map<std::string, std::unique_ptr<rpc::client>> pclients;

  decltype(rpc_proto.register_handler(1, (SqlFunc *)nullptr)) rpc_sql_exec;

  enum { RPC_SQL_EXEC = 1 };

public:
  SqlRpcEngine(AppConfig &config) : config(config), rpc_proto(serializer{}) {
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

  seastar::future<std::vector<std::vector<std::string>>>
  exec_sql(std::string sql) {
    try {
      int frag_type;
      auto [frag_type_, sqlmap] = qpParseSQL(sql);
      frag_type = frag_type_; // make clangd happy
      // 0: h split
      // 1: v split

      std::vector<seastar::future<std::vector<std::vector<std::string>>>> futs;

      fmt::print("Split type: {}\n", frag_type);

      for (auto &&[name, stmts] : sqlmap) {
        if (stmts.size()) {
          fmt::print("SQL: {} {}\n", name, stmts[0]);
          futs.emplace_back(std::move(rpc_sql_exec(*pclients[name], stmts[0])));
        }
      }

      return seastar::when_all(futs.begin(), futs.end())
          .then([frag_type](
                    std::vector<
                        seastar::future<std::vector<std::vector<std::string>>>>
                        result_futs) {
            std::vector<std::vector<std::string>> ret;
            std::vector<std::vector<std::vector<std::string>>> results;

            for (auto &&fut : result_futs)
              results.emplace_back(std::move(fut.get()));

            int pos_ind = 0;
            std::map<std::string, int> pos_map;

            for (auto &&res : results) {
              for (auto &&col : res[0]) {
                if (pos_map.count(col) == 0)
                  pos_map[col] = pos_ind++;
              }
            }

            ret.emplace_back(pos_ind);

            for (auto &&[name, ind] : pos_map) {
              ret.back()[ind] = name;
            }

            if (frag_type == 0) {
              for (auto &&res : results) {
                assert(res[0].size() == results[0][0].size());
                for (int i = 1; i < res.size(); i++) {
                  ret.emplace_back(pos_ind);
                  for (int j = 0; j < res[i].size(); j++) {
                    ret.back()[pos_map[res[0][j]]] = res[i][j];
                  }
                }
              }
            } else {
              ret.resize(results[0].size(),
                         std::vector<std::string>(ret[0].size()));
              for (auto &&res : results) {
                assert(res.size() == ret.size());
                for (int i = 1; i < res.size(); i++) {
                  for (int j = 0; j < res[i].size(); j++) {
                    ret[i][pos_map[res[0][j]]] = res[i][j];
                  }
                }
              }
            }

            return ret;
          });

      // return rpc_sql_exec(*pclients[config.name], sql);
    } catch (std::exception &e) {
      std::vector<std::vector<std::string>> ret{{e.what()}};
      return seastar::make_ready_future<std::vector<std::vector<std::string>>>(
          std::move(ret));
    }
  }
};

#endif
