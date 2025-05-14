import cluster_mapping
import os 
import pandas as pd
import yaml


cluster_mapping = cluster_mapping.cluster_mapping




parent_directory = "/Users/fkhan/Desktop/confluent retained bytes"
service_team_directory = "/Users/fkhan/Work/asynchronous-messaging-solution/v2/infra/terraform/service-teams/paypal/"

dev_topics = {}
int_topics = {}
prod_topics = {}
stage_topics = {}
all_topics = {}


for root, _, files in os.walk(service_team_directory):
    for file_name in files:
        if file_name.endswith('.yaml') and "topic" in file_name:
            parsed_topic_file = os.path.join(root, file_name)

            with open(parsed_topic_file, 'r') as yaml_file:
                data = yaml.safe_load(yaml_file)
                topics = data.get('topics', [])
                orphan_topics = []
                if topics:
                    for topic in topics:
                        if file_name.startswith("dev"):
                            dev_topics[topic['name']] = {k: topic[k] for k in set(list(topic.keys())) - set(['name'])}
                            dev_topics[topic['name']]['subdivision'] = root.split("/")[-1]
                        elif file_name.startswith("int"):
                            int_topics[topic['name']] = {k: topic[k] for k in set(list(topic.keys())) - set(['name'])}
                            int_topics[topic['name']]['subdivision'] = root.split("/")[-1]
                        elif file_name.startswith("prod"):
                            prod_topics[topic['name']] = {k: topic[k] for k in set(list(topic.keys())) - set(['name'])}
                            prod_topics[topic['name']]['subdivision'] = root.split("/")[-1]
                        elif file_name.startswith("stage"):
                            stage_topics[topic['name']] = {k: topic[k] for k in set(list(topic.keys())) - set(['name'])}
                            stage_topics[topic['name']]['subdivision'] = root.split("/")[-1]
                        elif file_name == "topics.yaml":
                            all_topics[topic['name']] = {k: topic[k] for k in set(list(topic.keys())) - set(['name'])}
                            all_topics[topic['name']]['subdivision'] = root.split("/")[-1]

print(f"General topic length: {len(all_topics)}")
print(f"Dev topic length: {len(dev_topics)}")
print(f"Int topic length: {len(int_topics)}")
print(f"Prod topic length: {len(prod_topics)}")
print(f"Stage topic length: {len(stage_topics)}")      
for topic in all_topics:
    if topic in dev_topics:
        if dev_topics[topic]["subdivision"] == all_topics[topic]["subdivision"]:
            pass
        else:
            print("Topic with same name but different subdivision found in dev topics")
    else:
        dev_topics[topic] = all_topics[topic]
    
    if topic in stage_topics:
        if stage_topics[topic]["subdivision"] == all_topics[topic]["subdivision"]:
            pass
        else:
            print("Topic with same name but different subdivision found in stage topics")
    else:
        stage_topics[topic] = all_topics[topic]
    
    if topic in int_topics:
        if int_topics[topic]["subdivision"] == all_topics[topic]["subdivision"]:
            pass
        else:
            print("Topic with same name but different subdivision found in int topics")
    else:
        int_topics[topic] = all_topics[topic]

    if topic in prod_topics:
        if prod_topics[topic]["subdivision"] == all_topics[topic]["subdivision"]:
            pass
        else:
            print("Topic with same name but different subdivision found in prod topics")
    else:
        prod_topics[topic] = all_topics[topic]

print(f"Final dev topic length: {len(dev_topics)}")
print(f"Final int topic length: {len(int_topics)}")
print(f"Final prod topic length: {len(prod_topics)}")
print(f"Final stage topic length: {len(stage_topics)}")




dev_df = pd.DataFrame()
int_df = pd.DataFrame()
prd_df = pd.DataFrame()
stg_df = pd.DataFrame()

for file_name in os.listdir(parent_directory):
    if file_name.endswith('.csv'):
        cluster_id = file_name.split("_")[0]
        cluster_name = cluster_mapping.get(cluster_id, "Unknown")
        if cluster_name == "Unknown":
            print(f"Cluster ID {cluster_id} not found in mapping.")
            continue

        csv_as_df = pd.read_csv(os.path.join(parent_directory, file_name))
        csv_as_df['cluster_name'] = cluster_name
        
        if "dev" in cluster_name:
            dev_df = pd.concat([dev_df, csv_as_df], ignore_index=True)
            dev_df['environment'] = "dev"

        elif "int" in cluster_name:
            int_df = pd.concat([int_df, csv_as_df], ignore_index=True)
            int_df['environment'] = "int"
        
        elif "prd" in cluster_name:
            prd_df = pd.concat([prd_df, csv_as_df], ignore_index=True)
            prd_df['environment'] = "prd"
        
        elif "stg" in cluster_name:
            stg_df = pd.concat([stg_df, csv_as_df], ignore_index=True)
            stg_df['environment'] = "stg"
        
dev_df = dev_df[dev_df['Topic name'].isin(dev_topics.keys())]
int_df = int_df[int_df['Topic name'].isin(int_topics.keys())]
prd_df = prd_df[prd_df['Topic name'].isin(prod_topics.keys())]
stg_df = stg_df[stg_df['Topic name'].isin(stage_topics.keys())]

columns_to_drop = ['Tags', 'Production', 'Consumption', 'Consumers', 'Has data contract', 'Tableflow']

dev_df = dev_df.drop(columns=columns_to_drop, errors='ignore')
int_df = int_df.drop(columns=columns_to_drop, errors='ignore')
prd_df = prd_df.drop(columns=columns_to_drop, errors='ignore')
stg_df = stg_df.drop(columns=columns_to_drop, errors='ignore')



def enrich_dataframe_with_topic_details(df, topic_dict):
    df['cleanup_policy'] = df['Topic name'].map(lambda x: topic_dict.get(x, {}).get('config', {}).get('cleanup_policy', None))
    df['retention_ms'] = df['Topic name'].map(lambda x: topic_dict.get(x, {}).get('config', {}).get('retention_ms', None))
    df['replication_type'] = df['Topic name'].map(lambda x: topic_dict.get(x, {}).get('replication', {}).get('type', None))
    df['subdivision'] = df['Topic name'].map(lambda x: topic_dict.get(x, {}).get('subdivision', None))
    return df

dev_df = enrich_dataframe_with_topic_details(dev_df, dev_topics)
int_df = enrich_dataframe_with_topic_details(int_df, int_topics)
prd_df = enrich_dataframe_with_topic_details(prd_df, prod_topics)
stg_df = enrich_dataframe_with_topic_details(stg_df, stage_topics)


print(f"Dev DataFrame length: {len(dev_df)}")
print(f"Int DataFrame length: {len(int_df)}")
print(f"Prod DataFrame length: {len(prd_df)}")
print(f"Stage DataFrame length: {len(stg_df)}")

output_directory = os.path.join(parent_directory, "output")
os.makedirs(output_directory, exist_ok=True)



combined_df = pd.concat([dev_df, int_df, prd_df, stg_df], ignore_index=True)
combined_df.to_csv(os.path.join(output_directory, f"{service_team_directory.split("/")[-2]}.csv"), index=False)

print(f"DataFrames have been written to {output_directory}")