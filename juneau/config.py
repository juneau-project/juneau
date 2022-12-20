#sql_schema = 'public'
#sql_name = "postgres"
#sql_password = "habitat1"
#sql_name = "yizhang"

sql_name = 'juneau_admin'

#sql_password = "yizhang"
sql_password = 'juneau'

#sql_dbname = "view_db"
sql_dbname = 'juneau_storage_ec2'

sql_host = "localhost"
#sql_host = 'ec2-3-237-25-43.compute-1.amazonaws.com' #"localhost"
sql_port = '5432'

sql_graph = "graph_model"
sql_provenance = "nb_provenance"
sql_dbs = "rowstore"
sql_views = "pd_view"

neo_name = "neo4j"
neo_password = "yizhang"#"juneau"
neo_host = "localhost:7474"
neo_port = '7687'
neo_db = "neo4j"

# lshe
max_k = 4
num_hash = 256
num_part = 32
corpus_sig = "test_sig"
corpus_hash = "test_hash"
q_sig = "test_q_sig"
q_hash = "test_q_hash"

# ks
corpus = 'utils'

#topk
return_table_num = 10
sql_schema_ra_states = "topk_ra_states"
sql_schema_sa_states = "topk_sa_states"