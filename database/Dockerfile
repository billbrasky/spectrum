FROM docker.io/library/postgres:11.5-alpine

# RUN rm -rf /var/lib/postgresql/data
# RUN COPY myTable FROM '/path/to/file/on/server' ( FORMAT CSV, DELIMITER('|') );
RUN psql -c "SELECT * FROM foo;"
