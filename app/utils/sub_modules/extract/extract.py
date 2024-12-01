from utils.sub_modules.extract.extract_lake import extract_lake
from utils.sub_modules.extract.extract_cache import extract_cache
from awsglue.context import GlueContext


class Extractor:
    def __init__(self, args, glue_context: GlueContext):
        """
        Inicializa a classe Extractor.

        Args:
            args (dict): Argumentos do job, como caminhos de entrada.
            glue_context (GlueContext): Contexto do AWS Glue para leitura de dados.
        """
        self.args = args
        self.glue_context = glue_context

    def extract_lake(self):
        """
        Realiza a extração dos dados do Lake.

        Returns:
            DataFrame: DataFrame extraído do Lake.
        """
        return extract_lake(self.args, self.glue_context)

    def extract_cache(self, base: str):
        """
        Realiza a extração dos dados do Cache.

        Args:
            base (str): Nome da base específica para extração do Cache.

        Returns:
            DataFrame: DataFrame extraído do Cache.
        """
        if not base:
            raise ValueError("O parâmetro 'base' é obrigatório para extração do cache.")
        return extract_cache(args=self.args, glueContext=self.glue_context, base=base)
