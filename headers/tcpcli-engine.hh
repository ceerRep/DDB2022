#include <seastar/core/future.hh>
#include <sstream>

#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>

#include <rpc-engine.hh>

class TcpCliEngine {
  SqlRpcEngine *pengine;

public:
  TcpCliEngine(SqlRpcEngine *pengine) : pengine(pengine) {}
  seastar::future<> handle_connection(seastar::connected_socket s,
                                      seastar::socket_address a) {
    auto out = s.output();
    auto in = s.input();
    seastar::temporary_buffer<char> c;
    return do_with(
        std::move(s), std::move(out), std::move(in),
        [this](auto &s, auto &out, auto &in) {
          return seastar::repeat([&out, &in, this] {
                   return in.read().then([&out, this](auto buf) {
                     if (buf.size() == 0)
                       return seastar::make_ready_future<
                           seastar::stop_iteration>(
                           seastar::stop_iteration::yes);
                     return pengine
                         ->exec_sql(std::string(buf.get(), buf.size()))
                         .handle_exception_type(
                             [](std::exception &e)
                                 -> std::vector<std::vector<std::string>> {
                               return {{e.what()}};
                             })
                         .then([&out](auto vals) {
                           std::stringstream ss;
                           for (auto &&v1 : vals) {
                             for (auto &&v2 : v1)
                               ss << v2 << '\t';
                             ss << std::endl;
                           }
                           ss << "DONE TOTAL " << vals.size() - 1 << " LINES\n";

                           auto str = ss.str();

                           std::string str1(str.size() + 4, ' ');
                           memcpy(str1.data() + 4, str.data(), str.size());
                           *(int *)(str1.data()) = str.size();

                           return out
                               .write(seastar::temporary_buffer<char>(
                                   str1.data(), str1.size()))
                               .then([&out] { return out.flush(); })
                               .then(
                                   [] { return seastar::stop_iteration::no; });
                         });
                   });
                 })
              .then([&out] { return out.close(); });
        });
  }

  seastar::future<> service_loop(unsigned short port) {
    seastar::listen_options lo;
    lo.reuse_address = false;
    return seastar::do_with(
        seastar::listen(seastar::make_ipv4_address({"0.0.0.0", port}), lo),
        [this, port](auto &listener) {
          fmt::print("Cli server started at {}:{}\n", "0.0.0.0", port);
          return seastar::keep_doing([&listener, this]() {
            return listener.accept().then([this](seastar::accept_result res) {
              // Note we ignore, not return, the future returned by
              // handle_connection(), so we do not wait for one
              // connection to be handled before accepting the next one.
              (void)handle_connection(std::move(res.connection),
                                      std::move(res.remote_address))
                  .handle_exception([](std::exception_ptr ep) {
                    fmt::print(stderr, "Could not handle connection: {}\n", ep);
                  });
            });
          });
        });
  }
};
