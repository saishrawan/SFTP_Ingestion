# SFTP_Ingestion

The IQVIA SFTP Feed Interface script connects to an SFTP server, downloads the previous day’s CSV file(s), loads them into Spark DataFrames (casting timestamps and adding a file_date), and appends the result to the Delta table pocn_data.silver.iqvia_uploads.

   [SFTP Server: feed.medtargetsystem.com]
                   ↓ (SSH/SFTP via Paramiko)
   [Driver Node’s DBFS: /dbfs/tmp & /dbfs/FileStore/keys]
                   ↓ (Spark CSV reader)
   [Spark DataFrame transformation & unionByName]
                   ↓ (Delta write)
[Spark Table: pocn_data.silver.iqvia_uploads (Delta)]
