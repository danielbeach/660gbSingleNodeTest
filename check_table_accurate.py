# Check Iceberg table files and total size using actual S3 file sizes
from boringcatalog import BoringCatalog
import subprocess
import json
import statistics

print("🔍 Checking Iceberg table files and size (using actual S3 file sizes)...")

catalog = BoringCatalog(
    name="TBtest",
    uri="s3://confessions-of-a-data-guy/TBtest/catalog/catalog.json",
)
ns_str = "wendigo"
table_name = "social_media_eats_your_brains"
identifier = f"{ns_str}.{table_name}"

# Load the table
table = catalog.load_table(identifier)
print(f"✅ Loaded table: {identifier}")

# Get current snapshot
current_snapshot = table.metadata.current_snapshot()
if not current_snapshot:
    print("❌ No snapshots found in table")
    exit(1)

print(f"📸 Current snapshot ID: {current_snapshot.snapshot_id}")

# Get all data files from the table
print("\n📁 Reading all data files from table manifests...")
file_paths = []

try:
    scan = table.scan()
    scan_tasks = scan.plan_files()
    
    for task in scan_tasks:
        data_file = task.file
        file_path = data_file.file_path
        file_paths.append(file_path)
    
    print(f"✅ Found {len(file_paths):,} data files")
    
except Exception as e:
    print(f"❌ Error reading data files: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Write file paths to a local file
paths_file = "iceberg_file_paths.txt"
print(f"\n💾 Writing {len(file_paths):,} file paths to {paths_file}...")
with open(paths_file, 'w') as f:
    for path in file_paths:
        f.write(path + '\n')

print(f"✅ File paths written to {paths_file}")

# Convert S3 paths to s3:// format if needed and extract bucket/key
def parse_s3_path(path):
    """Parse S3 path and return bucket and key"""
    if path.startswith('s3://'):
        path = path[5:]  # Remove s3://
    elif path.startswith('s3a://'):
        path = path[6:]  # Remove s3a://
    
    parts = path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    return bucket, key

# Get actual file sizes from S3 using AWS CLI
print("\n📊 Getting actual file sizes from S3 using AWS CLI...")
print("   This may take a while for large numbers of files...")

file_sizes = []
file_info_list = []
failed_files = []

# Process files in batches to avoid overwhelming AWS CLI
batch_size = 100
total_batches = (len(file_paths) + batch_size - 1) // batch_size

for batch_num in range(total_batches):
    batch_start = batch_num * batch_size
    batch_end = min(batch_start + batch_size, len(file_paths))
    batch_paths = file_paths[batch_start:batch_end]
    
    print(f"   Processing batch {batch_num + 1}/{total_batches} "
          f"(files {batch_start + 1}-{batch_end})...")
    
    for file_path in batch_paths:
        try:
            bucket, key = parse_s3_path(file_path)
            
            # Use AWS CLI to get file size
            # aws s3api head-object returns metadata including ContentLength
            cmd = [
                'aws', 's3api', 'head-object',
                '--bucket', bucket,
                '--key', key
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            obj_info = json.loads(result.stdout)
            file_size = obj_info.get('ContentLength', 0)
            
            file_sizes.append(file_size)
            file_info_list.append({
                'path': file_path,
                'size': file_size
            })
            
        except subprocess.CalledProcessError as e:
            print(f"      ⚠️  Warning: Could not get size for {file_path}: {e.stderr}")
            failed_files.append(file_path)
        except Exception as e:
            print(f"      ⚠️  Warning: Error processing {file_path}: {e}")
            failed_files.append(file_path)

print(f"\n✅ Successfully retrieved sizes for {len(file_sizes):,} files")
if failed_files:
    print(f"⚠️  Failed to get sizes for {len(failed_files):,} files")

# Calculate totals
total_bytes = sum(file_sizes)
total_files = len(file_sizes)
total_rows = sum(info.get('record_count', 0) for info in file_info_list)

# Convert to human-readable sizes
def format_bytes(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

total_size_tb = total_bytes / (1024 ** 4)

# Calculate statistics
if file_sizes:
    min_size = min(file_sizes)
    max_size = max(file_sizes)
    avg_size = total_bytes / total_files
    median_size = statistics.median(file_sizes)
    
    # Percentiles
    sorted_sizes = sorted(file_sizes)
    p25 = sorted_sizes[len(sorted_sizes) // 4] if len(sorted_sizes) >= 4 else 0
    p75 = sorted_sizes[3 * len(sorted_sizes) // 4] if len(sorted_sizes) >= 4 else 0
    p95 = sorted_sizes[95 * len(sorted_sizes) // 100] if len(sorted_sizes) >= 100 else 0
    p99 = sorted_sizes[99 * len(sorted_sizes) // 100] if len(sorted_sizes) >= 100 else 0
else:
    min_size = max_size = avg_size = median_size = 0
    p25 = p75 = p95 = p99 = 0

# Show sample files
print("\n📋 Sample files (first 10):")
for i, info in enumerate(file_info_list[:10], 1):
    filename = info['path'].split('/')[-1]
    print(
        f"  {i:4d}. {filename[:60]:<60} "
        f"{format_bytes(info['size']):>12}"
    )

if len(file_info_list) > 10:
    print(f"  ... and {len(file_info_list) - 10:,} more files")

# Detailed statistics
print("\n" + "="*80)
print("📊 ACCURATE FILE SIZE ASSESSMENT (from actual S3 file sizes)")
print("="*80)
print(f"Total files:              {total_files:,}")
print(f"Total size:               {format_bytes(total_bytes)} ({total_bytes:,} bytes)")
print(f"Total size:               {total_size_tb:.6f} TB")

if total_files > 0:
    print("\nFile Size Statistics:")
    print(f"  Minimum:               {format_bytes(min_size)}")
    print(f"  Maximum:              {format_bytes(max_size)}")
    print(f"  Average:              {format_bytes(avg_size)}")
    print(f"  Median:               {format_bytes(median_size)}")
    if p25 > 0:
        print(f"  25th percentile:      {format_bytes(p25)}")
    if p75 > 0:
        print(f"  75th percentile:      {format_bytes(p75)}")
    if p95 > 0:
        print(f"  95th percentile:      {format_bytes(p95)}")
    if p99 > 0:
        print(f"  99th percentile:      {format_bytes(p99)}")
    
    # Size distribution
    print("\nSize Distribution:")
    small_threshold = 10 * 1024 * 1024  # 10MB
    medium_threshold = 100 * 1024 * 1024  # 100MB
    large_threshold = 1024 * 1024 * 1024  # 1GB
    
    small = sum(1 for s in file_sizes if s < small_threshold)
    medium = sum(
        1 for s in file_sizes
        if small_threshold <= s < medium_threshold
    )
    large = sum(
        1 for s in file_sizes
        if medium_threshold <= s < large_threshold
    )
    xlarge = sum(1 for s in file_sizes if s >= large_threshold)
    
    print(f"  < 10 MB:              {small:,} files ({small/total_files*100:.1f}%)")
    print(f"  10-100 MB:            {medium:,} files ({medium/total_files*100:.1f}%)")
    print(f"  100 MB - 1 GB:        {large:,} files ({large/total_files*100:.1f}%)")
    print(f"  >= 1 GB:              {xlarge:,} files ({xlarge/total_files*100:.1f}%)")

if failed_files:
    print(f"\n⚠️  Failed to get sizes for {len(failed_files):,} files")
    print("   First 5 failed files:")
    for path in failed_files[:5]:
        print(f"     {path}")

print("="*80)

# Check if it's approximately 1TB
if abs(total_size_tb - 1.0) < 0.1:
    print("✅ Table size is approximately 1 TB!")
elif total_size_tb >= 0.9 and total_size_tb <= 1.1:
    print(f"✅ Table size is close to 1 TB ({total_size_tb:.6f} TB)")
else:
    print(f"ℹ️  Table size is {total_size_tb:.6f} TB (not exactly 1 TB)")

# Save results to JSON file
results_file = "iceberg_file_sizes.json"
print(f"\n💾 Saving results to {results_file}...")
with open(results_file, 'w') as f:
    json.dump({
        'total_files': total_files,
        'total_bytes': total_bytes,
        'total_size_tb': total_size_tb,
        'file_info': file_info_list,
        'statistics': {
            'min': min_size,
            'max': max_size,
            'avg': avg_size,
            'median': median_size,
            'p25': p25,
            'p75': p75,
            'p95': p95,
            'p99': p99
        }
    }, f, indent=2)

print(f"✅ Results saved to {results_file}")

