FROM usgsastro/miniconda3:latest

ENV DATA_DIR /data
ENV ISISDATA /isis-data

# Create the app user & data directory
RUN useradd -m -s /bin/bash isis && \
  mkdir -m 700 $DATA_DIR && chown isis:isis $DATA_DIR && \
  mkdir $ISISDATA && chown isis:isis $ISISDATA

# Install the app
COPY --chown=isis:isis . /app
WORKDIR /app

# Update conda & install dependencies
RUN conda install -n base -c defaults conda && \
  conda env update -n base -f environment.yml

USER isis
VOLUME /data
VOLUME /isis-data
EXPOSE 8080

RUN conda init bash

CMD ["/bin/bash", "-lic", "gunicorn -c gunicorn.conf.py wsgi:app"]
