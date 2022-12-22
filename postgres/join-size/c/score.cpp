extern "C"
{
#include "postgres.h"
}

#include <string>
#include <unordered_map>
#include <cstdint>

#include "score.h"

using namespace std;

// string
double score_str(char *ch_arr1, int len1, int e_num1, char *ch_arr2, int len2, int e_num2)
{
  string s1 = string(ch_arr1, ch_arr1 + len1);
  string s2 = string(ch_arr2, ch_arr2 + len2);

  size_t pos = 0;
  string token;
  string delimiter = "#sep#";

  unordered_map<string, int> m1, m2;

  while ((pos = s1.find(delimiter)) != std::string::npos)
  {
    token = s1.substr(0, pos);
    // cout << token << endl;
    // elog(WARNING, "%s", token.c_str());
    m1[token]++;
    s1.erase(0, pos + delimiter.length());
  }
  m1[s1]++;

  pos = 0;
  while ((pos = s2.find(delimiter)) != std::string::npos)
  {
    token = s2.substr(0, pos);
    // cout << token << endl;
    // elog(WARNING, "%s", token.c_str());
    m2[token]++;
    s2.erase(0, pos + delimiter.length());
  }
  m2[s2]++;

  int join_size = 0;

  if (m1.size() < m2.size())
  {
    for (auto it = m1.begin(); it != m1.end(); ++it)
    {
      auto got = m2.find(it -> first);
      if (got != m2.end()) {
        join_size += (it -> second * got -> second);
      }
    }
  }
  else
  {
    for (auto it = m2.begin(); it != m2.end(); ++it)
    {
      auto got = m1.find(it -> first);
      if (got != m1.end()) {
        join_size += (it -> second * got -> second);
      }
    }
  }

  // loop through map
  return (float) join_size / (e_num1 * e_num2);
}

// Datum *arr1 <--> int64_t
double score_int(Datum *arr1, int len1, Datum *arr2, int len2)
{
  unordered_map<int64_t, int> m1, m2;
  
  for (int i = 0; i < len1; i++)
  {
    m1[DatumGetInt64(arr1[i])]++;
  }

  for (int i = 0; i < len2; i++)
  {
    m2[DatumGetInt64(arr2[i])]++;
  }

  int join_size = 0;

  if (m1.size() < m2.size())
  {
    for (auto it = m1.begin(); it != m1.end(); ++it)
    {
      auto got = m2.find(it -> first);
      if (got != m2.end()) {
        join_size += (it -> second * got -> second);
      }
    }
  }
  else
  {
    for (auto it = m2.begin(); it != m2.end(); ++it)
    {
      auto got = m1.find(it -> first);
      if (got != m1.end()) {
        join_size += (it -> second * got -> second);
      }
    }
  }

  return (float) join_size / (len1 * len2);
}