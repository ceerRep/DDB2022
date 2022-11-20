#ifndef _CONFIG_HPP

#define _CONFIG_HPP

#include <vector>
#include <string>
#include <map>

struct AppConfig
{
  std::string name;
  std::map<std::string, std::tuple<std::string, unsigned short, unsigned short>> nodes;

  std::string sqldb_filename;
  std::string sqldb_initfile;
};

#endif
