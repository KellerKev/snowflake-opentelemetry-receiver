FROM python:3.12
RUN useradd -ms /bin/bash otel
RUN usermod -aG sudo otel
USER otel
WORKDIR /home/otel
COPY otel_client_sample_flask.py /home/otel
COPY requirements.txt /home/otel
RUN python3 -m venv otel_env
EXPOSE 5000
RUN . /home/otel/otel_env/bin/activate && pip install -r requirements.txt
CMD . /home/otel/otel_env/bin/activate && python otel_client_sample_flask.py


