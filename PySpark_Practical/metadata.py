import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile(r'part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet')

# to print metadata information about parquet file
print(parquet_file.metadata)
print(parquet_file.metadata.row_group(0)) 
print(parquet_file.metadata.row_group(0).column(0))
print(parquet_file.metadata.row_group(0).column(0).statistics) 