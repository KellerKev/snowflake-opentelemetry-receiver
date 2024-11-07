FROM python:3.12
RUN useradd -ms /bin/bash snowotel
RUN usermod -aG sudo snowotel
USER snowotel
WORKDIR /home/snowotel
COPY otel_server_python_http_tcp_snowflake_fastapi.py /home/snowotel
COPY requirements.txt /home/snowotel
RUN python3 -m venv snow_otel_env
RUN . /home/snowotel/snow_otel_env/bin/activate && pip install -r requirements.txt
#EXPOSE 4317
EXPOSE 4318
CMD . /home/snowotel/snow_otel_env/bin/activate && python otel_server_python_http_tcp_snowflake_fastapi.py


