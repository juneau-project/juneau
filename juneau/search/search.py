# Copyright 2020 Juneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Performs three types of search functionalities in tables,
either by:
    1. Searching for additional training data.
    2. Searching for joinable tables.
    3. Searching for alternative features.
"""

import json
import logging

special_type = ["np", "pd"]

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def search_tables(search_test, query_table, mode, code, var_name):
    if mode == 1:
        logging.info("Search for Additional Training/Validation Tables!")
        tables = search_test.search_additional_training_data(
            query_table, 10, code, var_name, 0.5, 1
        )
        logging.info("%s Tables are returned!" % len(tables))
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_tables_threshold2(
            query_table, 0.1, 10, 1.5, 0.9, 0.2
        )
        logging.info("%s Joinable Tables are returned!" % len(tables))
    else:
        logging.info("Search for Alternative Feature Tables!")
        tables = search_test.search_alternative_features(
            query_table, 10, code, var_name, 90, 200, 0.1, 10, 0.9, 0.2
        )
        logging.info("%s Tables are returned!" % len(tables))

    if not tables:
        return ""
    else:
        metadata = [
            {
                "varName": v[0],
                "varType": type(v[1]).__name__,
                "varSize": str(v[1].size),
                "varContent": v[1].to_html(
                    index_names=True,
                    justify="center",
                    max_rows=10,
                    max_cols=5,
                    header=True,
                ),
            }
            for v in tables
        ]
        return json.dumps(metadata)
