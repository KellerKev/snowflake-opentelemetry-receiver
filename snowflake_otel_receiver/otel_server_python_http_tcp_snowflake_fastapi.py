import grpc
import json
import logging
import asyncio
from concurrent import futures
from datetime import datetime
from threading import Thread
import os

import snowflake.connector
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import uvicorn

# Import OpenTelemetry Protobuf definitions
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2_grpc,
    trace_service_pb2,
)
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2_grpc,
    metrics_service_pb2,
)
from opentelemetry.proto.collector.logs.v1 import (
    logs_service_pb2_grpc,
    logs_service_pb2,
)
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure Snowflake connection

# Configure Snowflake connection
def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

#SPCS

def get_login_token():
  with open('/snowflake/session/token', 'r') as f:
      return f.read()

def connect_to_snowflake_spcs():
    return snowflake.connector.connect(
        host=os.getenv('SNOWFLAKE_HOST'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
        database= os.getenv('SNOWFLAKE_DATABASE'),
        schema= os.getenv('SNOWFLAKE_SCHEMA'),
        token = get_login_token(),
        authenticator = 'oauth'
    )

# Configuration options for compression
ENABLE_GRPC_COMPRESSION = True  # Set to False to disable gRPC compression support
ENABLE_HTTP_COMPRESSION = True  # Set to False to disable HTTP compression support

# Function to parse AnyValue objects
def parse_any_value(any_value):
    if any_value.HasField("string_value"):
        return any_value.string_value
    elif any_value.HasField("bool_value"):
        return any_value.bool_value
    elif any_value.HasField("int_value"):
        return any_value.int_value
    elif any_value.HasField("double_value"):
        return any_value.double_value
    elif any_value.HasField("array_value"):
        return [parse_any_value(val) for val in any_value.array_value.values]
    elif any_value.HasField("kvlist_value"):
        return {kv.key: parse_any_value(kv.value) for kv in any_value.kvlist_value.values}
    elif any_value.HasField("bytes_value"):
        return any_value.bytes_value  # Handle bytes as needed
    else:
        return None

# gRPC server for handling OTLP data
class TraceService(trace_service_pb2_grpc.TraceServiceServicer):
    def __init__(self, snowflake_conn):
        self.snowflake_conn = snowflake_conn

    def Export(self, request, context):
        self.process_trace(request)
        return trace_service_pb2.ExportTraceServiceResponse()

    def process_trace(self, trace_data):
        cursor = self.snowflake_conn.cursor()
        insert_sql = """
            INSERT INTO traces (trace_id, span_id, name, start_time, end_time, attributes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for resource_span in trace_data.resource_spans:
            for scope_span in resource_span.scope_spans:
                for span in scope_span.spans:
                    trace_id = span.trace_id.hex() if span.trace_id else ""
                    span_id = span.span_id.hex() if span.span_id else ""
                    name = span.name or "unknown"
                    start_time = (
                        datetime.fromtimestamp(span.start_time_unix_nano / 1e9)
                        if span.start_time_unix_nano
                        else datetime.now()
                    )
                    end_time = (
                        datetime.fromtimestamp(span.end_time_unix_nano / 1e9)
                        if span.end_time_unix_nano
                        else datetime.now()
                    )
                    attributes_dict = {kv.key: parse_any_value(kv.value) for kv in span.attributes}
                    attributes = json.dumps(attributes_dict) if attributes_dict else "{}"
                    logger.debug(
                        f"Inserting trace: {trace_id}, {span_id}, {name}, {start_time}, {end_time}, {attributes}"
                    )
                    cursor.execute(
                        insert_sql,
                        (trace_id, span_id, name, start_time, end_time, attributes),
                    )
        cursor.close()

class MetricsService(metrics_service_pb2_grpc.MetricsServiceServicer):
    def __init__(self, snowflake_conn):
        self.snowflake_conn = snowflake_conn

    def Export(self, request, context):
        self.process_metrics(request)
        return metrics_service_pb2.ExportMetricsServiceResponse()

    def process_metrics(self, metrics_data):
        cursor = self.snowflake_conn.cursor()
        insert_sql = """
            INSERT INTO metrics (timestamp, metric_name, value, attributes)
            VALUES (%s, %s, %s, %s)
        """
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    timestamp = datetime.now()
                    metric_name = metric.name or "unknown"
                    value = None
                    metric_attributes = []
                    if metric.HasField("gauge"):
                        data_points = metric.gauge.data_points
                        if data_points:
                            dp = data_points[0]
                            value = dp.as_double if dp.HasField("as_double") else dp.as_int
                            metric_attributes = dp.attributes
                    elif metric.HasField("sum"):
                        data_points = metric.sum.data_points
                        if data_points:
                            dp = data_points[0]
                            value = dp.as_double if dp.HasField("as_double") else dp.as_int
                            metric_attributes = dp.attributes
                    attributes_dict = {kv.key: parse_any_value(kv.value) for kv in metric_attributes}
                    attributes = json.dumps(attributes_dict) if attributes_dict else "{}"
                    if value is not None:
                        logger.debug(
                            f"Inserting metric: {timestamp}, {metric_name}, {value}, {attributes}"
                        )
                        cursor.execute(
                            insert_sql, (timestamp, metric_name, value, attributes)
                        )
        cursor.close()

class LogsService(logs_service_pb2_grpc.LogsServiceServicer):
    def __init__(self, snowflake_conn):
        self.snowflake_conn = snowflake_conn

    def Export(self, request, context):
        self.process_logs(request)
        return logs_service_pb2.ExportLogsServiceResponse()

    def process_logs(self, logs_data):
        cursor = self.snowflake_conn.cursor()
        insert_sql = """
            INSERT INTO logs (timestamp, log_level, message, attributes)
            VALUES (%s, %s, %s, %s)
        """
        for resource_log in logs_data.resource_logs:
            for scope_log in resource_log.scope_logs:
                for log in scope_log.log_records:
                    timestamp = (
                        datetime.fromtimestamp(log.time_unix_nano / 1e9)
                        if log.time_unix_nano
                        else datetime.now()
                    )
                    log_level = log.severity_text or "INFO"
                    message = (
                        log.body.string_value
                        if log.body.HasField("string_value")
                        else "No message"
                    )
                    attributes_dict = {kv.key: parse_any_value(kv.value) for kv in log.attributes}
                    attributes = json.dumps(attributes_dict) if attributes_dict else "{}"
                    logger.debug(
                        f"Inserting log: {timestamp}, {log_level}, {message}, {attributes}"
                    )
                    cursor.execute(
                        insert_sql, (timestamp, log_level, message, attributes)
                    )
        cursor.close()

# Start the gRPC server
def start_grpc_server(snowflake_conn):
    if ENABLE_GRPC_COMPRESSION:
        compression_option = grpc.Compression.Gzip
        logger.info("gRPC server will accept compressed data.")
    else:
        compression_option = grpc.Compression.NoCompression
        logger.info("gRPC server will not accept compressed data.")

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        compression=compression_option,
    )

    # Register each OTLP service individually
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(
        TraceService(snowflake_conn), server
    )
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(
        MetricsService(snowflake_conn), server
    )
    logs_service_pb2_grpc.add_LogsServiceServicer_to_server(
        LogsService(snowflake_conn), server
    )

    server.add_insecure_port("[::]:4317")
    logger.info("gRPC server started on port 4317")
    server.start()
    return server

# Start the FastAPI HTTP server
def start_http_server(snowflake_conn):
    app = FastAPI()

    @app.post("/v1/traces")
    async def receive_traces(request: Request):
        if request.headers.get("Content-Type") != "application/x-protobuf":
            raise HTTPException(status_code=400, detail="Unsupported Media Type")
        try:
            data = await request.body()
            encoding = request.headers.get("Content-Encoding", "").lower()
            if encoding == "gzip":
                if ENABLE_HTTP_COMPRESSION:
                    import gzip
                    data = gzip.decompress(data)
                else:
                    raise HTTPException(status_code=415, detail="Compression not supported")
            elif encoding:
                raise HTTPException(
                    status_code=415, detail=f"Unsupported Content-Encoding: {encoding}"
                )
            else:
                pass  # No compression

            trace_data = trace_service_pb2.ExportTraceServiceRequest()
            trace_data.ParseFromString(data)
            trace_service = TraceService(snowflake_conn)
            trace_service.process_trace(trace_data)
            response = trace_service_pb2.ExportTraceServiceResponse()
            return Response(content=response.SerializeToString(), media_type="application/x-protobuf")
        except Exception as e:
            logger.error(f"Error processing traces: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @app.post("/v1/metrics")
    async def receive_metrics(request: Request):
        if request.headers.get("Content-Type") != "application/x-protobuf":
            raise HTTPException(status_code=400, detail="Unsupported Media Type")
        try:
            data = await request.body()
            encoding = request.headers.get("Content-Encoding", "").lower()
            if encoding == "gzip":
                if ENABLE_HTTP_COMPRESSION:
                    import gzip
                    data = gzip.decompress(data)
                else:
                    raise HTTPException(status_code=415, detail="Compression not supported")
            elif encoding:
                raise HTTPException(
                    status_code=415, detail=f"Unsupported Content-Encoding: {encoding}"
                )
            else:
                pass  # No compression

            metrics_data = metrics_service_pb2.ExportMetricsServiceRequest()
            metrics_data.ParseFromString(data)
            metrics_service = MetricsService(snowflake_conn)
            metrics_service.process_metrics(metrics_data)
            response = metrics_service_pb2.ExportMetricsServiceResponse()
            return Response(content=response.SerializeToString(), media_type="application/x-protobuf")
        except Exception as e:
            logger.error(f"Error processing metrics: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @app.post("/v1/logs")
    async def receive_logs(request: Request):
        if request.headers.get("Content-Type") != "application/x-protobuf":
            raise HTTPException(status_code=400, detail="Unsupported Media Type")
        try:
            data = await request.body()
            encoding = request.headers.get("Content-Encoding", "").lower()
            if encoding == "gzip":
                if ENABLE_HTTP_COMPRESSION:
                    import gzip
                    data = gzip.decompress(data)
                else:
                    raise HTTPException(status_code=415, detail="Compression not supported")
            elif encoding:
                raise HTTPException(
                    status_code=415, detail=f"Unsupported Content-Encoding: {encoding}"
                )
            else:
                pass  # No compression

            logs_data = logs_service_pb2.ExportLogsServiceRequest()
            logs_data.ParseFromString(data)
            logs_service = LogsService(snowflake_conn)
            logs_service.process_logs(logs_data)
            response = logs_service_pb2.ExportLogsServiceResponse()
            return Response(content=response.SerializeToString(), media_type="application/x-protobuf")
        except Exception as e:
            logger.error(f"Error processing logs: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    logger.info("HTTP server started on port 4318")
    uvicorn.run(app, host="0.0.0.0", port=4318)

def main():
    SPCS=os.getenv('SPCS')

   

    if SPCS=="True":
        snowflake_conn = connect_to_snowflake_spcs()
        http_thread = Thread(target=start_http_server, args=(snowflake_conn,))
        http_thread.start()
    else:
        snowflake_conn = connect_to_snowflake()
        http_thread = Thread(target=start_http_server, args=(snowflake_conn,))
        http_thread.start()
        # Start the gRPC server
        grpc_server = start_grpc_server(snowflake_conn)
        
        try:
            grpc_server.wait_for_termination()
        except KeyboardInterrupt:
            grpc_server.stop(None)
        
        logger.info("Servers stopped.")


if __name__ == "__main__":
    main()
