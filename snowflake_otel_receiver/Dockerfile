FROM python:3.12
RUN useradd -ms /bin/bash otel
RUN usermod -aG sudo otel
USER otel
WORKDIR /home/otel
COPY otel_server_python_http_tcp_snowflake_fastapi.py /home/otel
COPY requirements.txt /home/otel
RUN python3 -m venv otel_env
RUN . /home/otel/otel_env/bin/activate && pip install -r requirements.txt
#EXPOSE 4317
EXPOSE 4318
CMD . /home/otel/otel_env/bin/activate && python otel_server_python_http_tcp_snowflake_fastapi.py


