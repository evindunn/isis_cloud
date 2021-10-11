FROM usgsastro/miniconda3:latest

COPY . /app
WORKDIR /app

RUN conda install -n base -c defaults conda && \
  conda env update -n base -f environment.yml

CMD ["/bin/bash", "-lc", "./example_server.py"]
