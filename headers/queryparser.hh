#ifndef _QUERYPARSER_HH

#define _QUERYPARSER_HH

#include <map>
#include <string>
#include <vector>

void qpFragInit();

std::pair<int, std::map<std::string, std::vector<std::string>>>
qpParseSQL(std::string sql);

#endif
