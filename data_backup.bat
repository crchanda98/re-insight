@echo off

docker exec -t postgres_usb pg_dump -U admin -d mydb > D:/work/docker_db/mount_folder/mydb_backup.sql