#include <chrono>
#include <iostream>

#include <yaml-cpp/yaml.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

#include "config.hpp"
#include <queryparser.hh>
#include <rpc-engine.hh>
#include <tcpcli-engine.hh>

using namespace seastar;
namespace bpo = boost::program_options;

AppConfig &loadConfig(const char *filename) {
  static AppConfig *appconfig = nullptr;

  if (filename) {
    appconfig = new AppConfig;

    YAML::Node node = YAML::LoadFile(filename);
    appconfig->name = node["name"].as<std::string>();

    for (auto &&it : node["nodes"]) {
      auto name = it.first.as<std::string>();
      auto host_port = it.second;
      appconfig->nodes[name] =
          std::make_tuple(host_port["host"].as<std::string>(),
                          host_port["port"].as<unsigned int>(),
                          host_port["cli-port"].as<unsigned int>());
    }

    appconfig->sqldb_filename = node["sqlite"]["filename"].as<std::string>();
    appconfig->sqldb_initfile = node["sqlite"]["initfile"].as<std::string>();
  }

  return *appconfig;
}

SqlRpcEngine *psqlengine;
TcpCliEngine *pcliengine;

int main(int argc, char **argv) {
  seastar::app_template app;
  app.add_options()("config",
                    bpo::value<std::string>()->default_value("config.yaml"),
                    "Config file");
  app.run(argc, argv, [&] {
    auto &&config = app.configuration();
    std::string config_filename = config["config"].as<std::string>();

    auto &&server_config = loadConfig(config_filename.c_str());

    using namespace std::chrono_literals;

    psqlengine = new SqlRpcEngine(server_config);
    pcliengine = new TcpCliEngine(psqlengine);

    qpFragInit();

    return seastar::sleep(1s).then([&server_config] {
      (void)seastar::smp::submit_to(
          seastar::this_shard_id(), [&server_config]() {
            return pcliengine->service_loop(
                std::get<2>(server_config.nodes[server_config.name]));
          });
      return psqlengine->clients_init().then([]() {
        return seastar::repeat([] {
          return seastar::sleep(1s).then(
              [] { return seastar::stop_iteration::no; });
        });
      });
    });
  });
}
