from flask import Flask, request, jsonify
import time
import logging
import os
from opentelemetry import trace, metrics
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor



endpoint=os.getenv('ENDPOINT')
insecure_endpoint=os.getenv('ENDPOINT_INSECURE')
insecure=True

if insecure_endpoint=="True":
    insecure=True
else:
    insecure=False


# Enable experimental logging feature
os.environ["OTEL_PYTHON_EXPERIMENTAL_ENABLE_LOGS_EXPORTER"] = "true"

# Initialize Flask application
app = Flask(__name__)

# OpenTelemetry Trace Configuration
trace_provider = TracerProvider()
trace.set_tracer_provider(trace_provider)

# Set up OTLP Span Exporter (Traces) to our custom gRPC server
span_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))

# OpenTelemetry Metrics Configuration
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=endpoint, insecure=insecure)
)
meter_provider = MeterProvider(metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# OpenTelemetry Logging Configuration
logger_provider = LoggerProvider()
set_logger_provider(logger_provider)

# Set up OTLP Log Exporter
log_exporter = OTLPLogExporter(endpoint, insecure=insecure)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)

# Instrument Flask and Logging with OpenTelemetry
FlaskInstrumentor().instrument_app(app)
LoggingInstrumentor().instrument(set_logging_format=True)

# Create a tracer, meter, and logger
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Define metrics
request_counter = meter.create_counter(
    name="http_requests_total",
    description="Total number of HTTP requests",
    unit="1",
)

response_time_histogram = meter.create_histogram(
    name="http_response_time_seconds",
    description="Response time in seconds",
    unit="s",
)

# Example endpoint with tracing, metrics, and logging
@app.route("/hello", methods=["GET"])
def hello():
    with tracer.start_as_current_span("hello-span"):
        logger.info("Received request for /hello")
        request_counter.add(1, {"endpoint": "/hello", "method": request.method})
        start_time = time.time()
        time.sleep(0.2)  # Simulate work
        response_time = time.time() - start_time
        response_time_histogram.record(
            response_time, {"endpoint": "/hello", "method": request.method}
        )
        logger.info(f"Processed /hello in {response_time:.2f} seconds")
        return jsonify({"message": "Hello, OpenTelemetry!"})

# Example endpoint with tracing, metrics, and logging for a parameterized request
@app.route("/greet/<name>", methods=["GET"])
def greet(name):
    with tracer.start_as_current_span("greet-span") as span:
        logger.info(f"Received request for /greet/{name}")
        request_counter.add(
            1, {"endpoint": "/greet/<name>", "method": request.method, "name": name}
        )
        start_time = time.time()
        time.sleep(0.3)  # Simulate work
        response_time = time.time() - start_time
        response_time_histogram.record(
            response_time, {"endpoint": "/greet/<name>", "method": request.method}
        )
        logger.info(f"Processed /greet/{name} in {response_time:.2f} seconds")
        span.set_attribute("user.name", name)
        return jsonify({"message": f"Hello, {name}!"})

# Run the Flask application
if __name__ == "__main__":
    print (endpoint)
    print(insecure_endpoint)
    app.run(debug=True, host="0.0.0.0", port=5000)
