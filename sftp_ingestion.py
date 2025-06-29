# paste your private key literally here between the quotes
private_key = """-----BEGIN OPENSSH PRIVATE KEY-----
XXXX"""
dbutils.fs.put("/FileStore/keys/iqvia_sftp_key", private_key, overwrite=True)

import os

# DBFS files are visible under /dbfs/ on the driver node:
path = "/dbfs/FileStore/keys/iqvia_sftp_key"

# 0o600 = owner-read/write only
os.chmod(path, 0o600)
print(f"Permissions of {path} set to 600")

%pip install paramiko

import re
from datetime import date, timedelta
import paramiko
from pyspark.sql import functions as F

# ————— 0) Compute yesterday’s date —————
yesterday     = date.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y%m%d")   # e.g. "20250520"

# 1) Create client and connect
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(
    hostname="feed.medtargetsystem.com",
    port=22,
    username="pocn.pocn-v4",
    pkey=paramiko.RSAKey.from_private_key_file("/dbfs/FileStore/keys/iqvia_sftp_key"),
    timeout=60,
    banner_timeout=30,
    auth_timeout=30,
    look_for_keys=False,
    allow_agent=False,
    disabled_algorithms={'pubkeys': ['rsa-sha2-256','rsa-sha2-512']}
)

# 2) Now the transport exists—grab it and set keepalive
transport = ssh.get_transport()
if transport is None:
    raise RuntimeError("SSH transport not established!")
transport.set_keepalive(30)

# 3) Open SFTP over that same transport
sftp = paramiko.SFTPClient.from_transport(transport)

# ————— 2) Find yesterday’s file(s) —————
# prefix up through the date; then 6‑digit time, then “.csv”
prefix  = f"AIM_pocn_pocn-v4_webfeed_{yesterday_str}_"
pattern = re.compile(rf"^{re.escape(prefix)}\d{{6}}\.csv$")

remote_files = sftp.listdir(".")
yest_files   = [f for f in remote_files if pattern.match(f)]

if not yest_files:
    print(f"No files found for {yesterday_str}")
else:
    # ————— 3) Download & read each CSV into a list of DataFrames —————
    dfs = []
    for fname in yest_files:
        local_path = f"/dbfs/tmp/{fname}"
        dbfs_path  = f"dbfs:/tmp/{fname}"

        # download
        sftp.get(fname, local_path)

        # get the target DataType name for event_timestamp
        tbl_schema = spark.table("pocn_data.silver.iqvia_uploads").schema
        target_type = tbl_schema["event_timestamp"].dataType.simpleString()  
        # e.g. "string" or "timestamp"

        # later, in your df-builder loop:
        df = (spark.read
                .option("header","true")
                .option("inferSchema","true")
                .csv(dbfs_path)
                .withColumn(
                    "event_timestamp",
                    F.col("event_timestamp").cast(target_type)
                )
                .withColumn("file_date", F.lit(yesterday).cast("date"))
            )
        dfs.append(df)

    # ————— 4) Combine (if multiple) and append to Delta table —————
    full_df = dfs[0]
    for df in dfs[1:]:
        full_df = full_df.unionByName(df)

    (full_df.write
         .format("delta")
         .mode("append")
         .saveAsTable("pocn_data.silver.iqvia_uploads")
    )
    print(f"Appended {len(dfs)} file(s) for {yesterday_str} into pocn_data.silver.iqvia_uploads")

# ————— 5) Cleanup —————
sftp.close()
ssh.close()
