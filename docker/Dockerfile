# Adventure Works Database on SQL Server 2019
FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

# Note: This isn't a secure password, and please don't use this for production.
ENV SA_PASSWORD=ThisIsAReallyCoolPassword123
ENV ACCEPT_EULA=Y

# Setting the user
USER mssql

COPY AdventureWorksDW2019.bak /var/opt/mssql/backup/

# Launch SQL Server, confirm startup is complete, restore the database, then terminate SQL Server.
# The logical files here are all screwy
RUN ( /opt/mssql/bin/sqlservr & ) | grep -q "Service Broker manager has started" \
    && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -Q 'RESTORE DATABASE AdventureWorksDW2019 FROM DISK = "/var/opt/mssql/backup/AdventureWorksDW2019.bak" WITH MOVE "AdventureWorksDW2017" to "/var/opt/mssql/data/AdventureWorksDW2019.mdf", MOVE "AdventureWorksDW2017_Log" to "/var/opt/mssql/data/AdventureWorksDW2019_log.ldf", NOUNLOAD, STATS = 5' \
    && pkill sqlservr

CMD ["/opt/mssql/bin/sqlservr"]
