FROM registry.gitlab.com/quarto-forge/docker/jupyter:latest

USER root

RUN groupadd -g 1000 jovyan && \
    useradd -u 1000 -g jovyan -m -s /bin/bash jovyan

COPY requirements.txt .

RUN /opt/conda/bin/python -m pip install --no-cache-dir -r requirements.txt

USER jovyan

WORKDIR /home/jovyan/work

CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token='' --ServerApp.password=$JUPYTER_PASSWORD --notebook-dir=/home/jovyan/work/analysis"]