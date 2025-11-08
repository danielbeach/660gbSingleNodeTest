from boringcatalog import BoringCatalog
from pyiceberg.exceptions import NoSuchTableError
import pyarrow.parquet as pq
import s3fs

print("🚀 Starting Iceberg metadata registration job...")

catalog = BoringCatalog(
    name="TBtest",
    uri="s3://confessions-of-a-data-guy/TBtest/catalog/catalog.json",
)
ns_str = "wendigo"
table_name = "social_media_eats_your_brains"
identifier = f"{ns_str}.{table_name}"

s3 = s3fs.S3FileSystem()
data_path = (
    "s3://confessions-of-a-data-guy/BigTest/ice_default/"
    "social_media_posts/data/"
)

try:
    table = catalog.load_table(identifier)
    print("✅ Table already exists:", identifier)
except NoSuchTableError:
    print("🆕 Table doesn't exist, discovering parquet files to get schema...")
    
    parquet_files_for_schema = []
    for path in s3.find(data_path, detail=False):
        if path.endswith('.parquet'):
            if not path.startswith('s3://'):
                path = f"s3://{path}"
            parquet_files_for_schema.append(path)
            if len(parquet_files_for_schema) >= 1:
                break
    
    if not parquet_files_for_schema:
        raise RuntimeError("No parquet files found in S3 location")
    
    first_file = parquet_files_for_schema[0]
    print(f"📖 Reading schema from: {first_file}")
    parquet_file = pq.ParquetFile(first_file, filesystem=s3)
    source_schema = parquet_file.schema_arrow
    print("🧬 Source schema has", len(source_schema), "fields")
    
    print("🆕 Creating table", identifier)
    table = catalog.create_table(identifier, schema=source_schema)

print("✅ Loaded table:", getattr(table, "identifier", identifier))

print("📋 Discovering parquet files in S3...")
parquet_files = []

for path in s3.find(data_path, detail=False):
    if path.endswith('.parquet'):
        # Ensure full S3 URI format
        if not path.startswith('s3://'):
            path = f"s3://{path}"
        parquet_files.append(path)

print(f"📁 Found {len(parquet_files)} parquet files")

batch_size = 100  # Number of files per commit
total_files = len(parquet_files)
commits = 0

print(
    f"⚙️  Registering {len(parquet_files)} files "
    f"in batches of {batch_size}"
)

for i in range(0, total_files, batch_size):
    batch_files = parquet_files[i:i + batch_size]
    batch_num = i // batch_size + 1
    file_range = f"{i+1}-{min(i+batch_size, total_files)}"
    print(
        f"  📦 Processing batch {batch_num}: "
        f"{len(batch_files)} files (files {file_range})"
    )
    try:
        table.add_files(file_paths=batch_files, check_duplicate_files=False)
        commits += 1
        print(f"✅ commit #{commits}: registered {len(batch_files)} files")
    except Exception as e:
        error_msg = str(e).lower()
        if 'field' in error_msg and 'id' in error_msg:
            print(
                f"    ⚠️  Batch {batch_num}: Files have field IDs. "
                f"Trying manual DataFile construction..."
            )
            try:
                from pyiceberg.manifest import DataFile, DataFileContent
                from pyiceberg.table.update.snapshot import _FastAppendFiles, Operation
                
                with table.transaction() as txn:
                    append_op = _FastAppendFiles(
                        operation=Operation.APPEND,
                        transaction=txn,
                        io=table.io
                    )
                    
                    for file_path in batch_files:
                        try:
                            parquet_file = pq.ParquetFile(file_path, filesystem=s3)
                            file_size = s3.info(file_path).get('Size', 0)
                            
                            data_file = DataFile.from_args(
                                _table_format_version=table.metadata.format_version,
                                content=DataFileContent.DATA,
                                file_path=file_path,
                                file_format="PARQUET",
                                partition={},  # Empty partition dict
                                record_count=parquet_file.metadata.num_rows,
                                file_size_in_bytes=file_size,
                            )
                            
                            append_op.append_data_file(data_file)
                            
                        except Exception as file_err:
                            print(
                                f"    ⚠️  Warning: Could not process "
                                f"{file_path}: {file_err}"
                            )
                            continue
                    txn.update_snapshot(append_op)
                
                commits += 1
                print(f"✅ commit #{commits}: registered {len(batch_files)} files")
            except Exception as manual_err:
                print(f"    ❌ Manual DataFile construction failed: {manual_err}")
                import traceback
                traceback.print_exc()
                continue
        else:
            print(f"    ❌ Error in batch {batch_num}: {e}")
            continue

print(f"🎉 Done: {len(parquet_files)} files registered across {commits} commits.")