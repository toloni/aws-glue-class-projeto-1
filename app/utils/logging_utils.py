import logging
import time
import boto3
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class ProcessingLogger:
    def __init__(self, log_group: str, log_stream: str, aws_region: str = "us-east-1"):
        self.logger = logging.getLogger(__name__)
        self.stats = {}
        self.log_group = log_group
        self.log_stream = log_stream
        self.cloudwatch_client = boto3.client("logs", region_name=aws_region)
        self.sequence_token = None
        self._initialize_cloudwatch()

    def _initialize_cloudwatch(self):
        """
        Inicializa o grupo e o stream de logs no CloudWatch, se necessário.
        """
        try:
            # Certifica-se de que o log group existe
            self.cloudwatch_client.create_log_group(logGroupName=self.log_group)
        except self.cloudwatch_client.exceptions.ResourceAlreadyExistsException:
            pass

        try:
            # Certifica-se de que o log stream existe
            self.cloudwatch_client.create_log_stream(
                logGroupName=self.log_group, logStreamName=self.log_stream
            )
        except self.cloudwatch_client.exceptions.ResourceAlreadyExistsException:
            pass

    def log_info(self, message: str):
        self.logger.info(message)
        self._send_to_cloudwatch(message, level="INFO")

    def log_error(self, message: str, exc_info=True):
        self.logger.error(message, exc_info=exc_info)
        self._send_to_cloudwatch(message, level="ERROR")

    def log_status_count(self, df: DataFrame, base_name: str):
        """
        Loga a quantidade de registros no DataFrame por status e armazena as estatísticas.
        """
        try:
            status_count = df.groupBy("status").count().collect()
            self.stats[base_name] = {
                row["status"]: row["count"] for row in status_count
            }
            for status, count in self.stats[base_name].items():
                log_message = (
                    f"Base: {base_name}, Status: {status}, Quantidade: {count}"
                )
                self.log_info(log_message)
        except Exception as e:
            self.log_error(f"Erro ao calcular estatísticas de status: {str(e)}")

    def _send_to_cloudwatch(self, message: str, level: str):
        """
        Envia mensagens para o CloudWatch Logs.
        """
        try:
            log_event = {
                "logGroupName": self.log_group,
                "logStreamName": self.log_stream,
                "logEvents": [
                    {
                        "timestamp": int(time.time() * 1000),
                        "message": f"[{level}] {message}",
                    }
                ],
            }
            if self.sequence_token:
                log_event["sequenceToken"] = self.sequence_token

            response = self.cloudwatch_client.put_log_events(**log_event)
            self.sequence_token = response.get("nextSequenceToken")
        except Exception as e:
            self.logger.error(
                f"Erro ao enviar mensagem para o CloudWatch Logs: {str(e)}"
            )
