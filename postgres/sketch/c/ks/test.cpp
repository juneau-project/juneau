#include <string>
#include <cstdint>
#include <unordered_set>
#include <iostream>

using namespace std;

int rand_range(int low, int high)
{
  return rand() % (high - low + 1) + low;
}

// int main() {
//   for (int i = 0; i < 10; i++) {
//     cout << rand_range(1,2) << endl;
//   }
//   return 0;
// }

int main()
{
  // inputs
  string str = "2017, 2017, 2016";
  int type = 0;
  int arr_len = 3;

  unordered_set<int64_t> unique_s;
  int64_t *arr_content_int = (int64_t *)malloc(sizeof(int64_t) * arr_len);

  // only do sampling if arr_len > 10,0000
  size_t pos = 0;
  string token;
  string delimiter = ",";
  int idx = 0;

  int sample_size = 100;
  if (arr_len > sample_size)
  {
    int64_t *arr_content = (int64_t *)malloc(sizeof(int64_t) * arr_len);

    // construct arr_content
    while ((pos = str.find(delimiter)) != std::string::npos)
    {
      token = str.substr(0, pos);
      int64_t token_int;
      if (type == 0)
      {
        token_int = stoll(token);
      }
      else
      {
        token_int = int64_t(round(stod(token)));
      }
      arr_content[idx++] = token_int;
      str.erase(0, pos + delimiter.length());
    }

    int64_t token_int;
    if (type == 0)
    {
      token_int = stoll(str);
    }
    else
    {
      token_int = int64_t(round(stod(str)));
    }
    arr_content[idx] = token_int;

    arr_content_int = (int64_t *)malloc(sizeof(int64_t) * sample_size);

    unordered_set<int> sample_set;
    for (int i = 0; i < sample_size; i++)
    {
      int rand_idx = rand_range(0, arr_len);

      while (sample_set.count(rand_idx) == 1)
      {
        rand_idx = rand_range(0, arr_len);
      }

      sample_set.insert(rand_idx);

      arr_content_int[i] = arr_content[rand_idx];
      unique_s.insert(arr_content[rand_idx]);
    }

    for (int i = 0; i < sample_size; i++) {
      cout << arr_content_int[i] << ",";
    }

    cout << endl << "unique element = " << unique_s.size() << endl;
  }
  else
  {
    cout << "did not exceed" << endl;

    // insert into array
    while ((pos = str.find(delimiter)) != std::string::npos)
    {
      token = str.substr(0, pos);

      int64_t token_int;

      if (type == 0)
      {
        // elog(WARNING, "%s", "int");
        token_int = stoll(token);
      }
      else
      {
        // elog(WARNING, "%s", "float");
        token_int = int64_t(round(stod(token)));
      }

      unique_s.insert(token_int);
      arr_content_int[idx++] = token_int;

      str.erase(0, pos + delimiter.length());
    }

    int64_t token_int;
    if (type == 0)
    {
      token_int = stoll(str);
    }
    else
    {
      token_int = int64_t(round(stod(str)));
    }

    unique_s.insert(token_int);
    arr_content_int[idx] = token_int;

    for (int i = 0; i < arr_len; i++) {
      cout << arr_content_int[i] << ",";
    }

    cout << "unique element = " << unique_s.size() << endl;
  }

  cout << endl;
}