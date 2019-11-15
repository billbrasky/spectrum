FROM docker.io/library/postgres:11.5-alpine
ENV POSTGRES_USER=spectrum_coffee
# echo "saturn city roast" | shasum
ENV POSTGRES_PASSWORD=f06b6684590599506838e6ee6779e7488e8b0766
ENV PGDATA=/var/lib/postgresql/data/spectrum_coffee
# RUN rm -rf /var/lib/postgresql/data
# RUN COPY myTable FROM '/path/to/file/on/server' ( FORMAT CSV, DELIMITER('|') );
COPY ./data /docker-entrypoint-initdb.d/
#RUN psql -U spectrum_coffee -d spectrum_coffee -c "SELECT * FROM coffee__tastings;"
