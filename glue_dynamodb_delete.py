from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import boto3
import time

# Inicializar contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Leitura dos arquivos Parquet do S3
df_s3 = spark.read.parquet("s3://your-bucket-name/your-prefix/")

# Exibir o schema para verificação (opcional)
df_s3.printSchema()

# Executar uma query para filtrar ou manipular os dados
df_filtered = df_s3.filter("your_filter_condition")

# Exibir os primeiros registros para verificação (opcional)
df_filtered.show()

# Configurações do DynamoDB e boto3 client
dynamodb = boto3.client('dynamodb', region_name='us-west-2')
table_name = "your-dynamodb-table"
batch_size = 25  # Máximo de 25 itens por operação batch
max_retries = 10  # Número máximo de tentativas em caso de throttling

# Função para Deletar Itens em Lote
def delete_items_batch(items):
    delete_requests = [{'DeleteRequest': {'Key': item}} for item in items]
    attempts = 0
    while attempts < max_retries:
        response = dynamodb.batch_write_item(RequestItems={table_name: delete_requests})
        unprocessed_items = response.get('UnprocessedItems', {})

        if not unprocessed_items:
            break

        delete_requests = unprocessed_items.get(table_name, [])
        attempts += 1
        time.sleep(2 ** attempts)  # Backoff exponencial

    if attempts == max_retries:
        print("Max retries reached. Some items may not have been deleted.")

# Coletar as chaves primárias para exclusão
keys_to_delete = df_filtered.select("partition_key", "sort_key").collect()

# Dividir os dados em lotes e excluir
for i in range(0, len(keys_to_delete), batch_size):
    batch = keys_to_delete[i:i + batch_size]
    items = [{"partition_key": {"S": str(row['partition_key'])},
              "sort_key": {"S": str(row['sort_key'])}} for row in batch]
    delete_items_batch(items)

print("Data deletion process completed.")
