import time
from pyflink.table import TableEnvironment, EnvironmentSettings

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars
jars_path = "C:/Users/Ahmad%20Kamiludin/Desktop/flink-project/jar_files/"
jar_files = [
    "file:///" + jars_path + "flink-connector-kafka-1.17.1.jar", 
    "file:///" + jars_path + "flink-sql-connector-kafka-1.17.1.jar",
    "file:///" + jars_path + "avro-1.11.3.jar",
    "file:///" + jars_path + "flink-avro-1.17.1.jar", 
    "file:///" + jars_path + "flink-avro-confluent-registry-1.17.1.jar",
    "file:///" + jars_path + "kafka-clients-3.2.3.jar",
    "file:///" + jars_path + "kafka-schema-registry-client-7.4.0.jar",
    "file:///" + jars_path + "jackson-databind-2.17.2.jar",
    "file:///" + jars_path + "jackson-annotations-2.17.2.jar",
    "file:///" + jars_path + "jackson-core-2.17.2.jar",
    "file:///" + jars_path + "guava-30.1.1-jre.jar",
    "file:///" + jars_path + "flink-connector-mongodb-1.0.1-1.17.jar",
    "file:///" + jars_path + "mongo-java-driver-3.11.2.jar"
]
jar_files_str = ";".join(jar_files)

# Set JAR files for Flink pipeline
t_env.get_config().get_configuration().set_string("pipeline.jars", jar_files_str)

# Define source table DDL
source_ddl = """
    CREATE TABLE source_table(
        author STRING,
        update_at STRING,
        like_count INT,
        text STRING,
        public BOOLEAN
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'youtube_comments',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test',
        'scan.startup.mode' = 'earliest-offset',
        'value.format' = 'avro-confluent',
        'value.avro-confluent.schema-registry.url' = 'http://localhost:8081',
        'value.fields-include' = 'EXCEPT_KEY'
    )
"""

# Define sink table DDL for MongoDB
sink_ddl = """
    CREATE TABLE sink_table(
        author STRING,
        update_at STRING,
        like_count INT,
        text STRING,
        public BOOLEAN
    ) WITH (        
        'connector' = 'mongodb',
        'uri' = 'your_mongodb_uri',
        'database' = 'my_db',
        'collection' = 'youtube_comments'
    )
"""

# Define sink table DDL for console output
console_sink_ddl = """
    CREATE TABLE console_sink_table(
        author STRING,
        update_at STRING,
        like_count INT,
        text STRING,
        public BOOLEAN
    ) WITH (
        'connector' = 'print'
    )
"""

# Execute DDL statements to create the source and sink tables
t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)
t_env.execute_sql(console_sink_ddl)

# Define a SQL query to filter and order the data
sql_query = """
    SELECT * FROM source_table 
    WHERE like_count > 10 
"""

# Insert into the console sink to display data
t_env.execute_sql(f"INSERT INTO console_sink_table {sql_query}")

# Insert the processed data into the MongoDB sink table
t_env.execute_sql(f"INSERT INTO sink_table {sql_query}")

# To prevent the script from exiting immediately, keep it running
print("Streaming data from Kafka & Flink to MongoDB....")
while True:
    time.sleep(10)
