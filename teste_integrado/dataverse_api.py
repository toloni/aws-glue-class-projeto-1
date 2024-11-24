import requests
from pyspark.sql import SparkSession


class DataverseAPI:
    def __init__(self, tenant_id, client_id, client_secret, dataverse_url):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.dataverse_url = dataverse_url
        self.access_token = None
        self.spark = SparkSession.builder.appName("DataverseAPI").getOrCreate()

    def authenticate(self):
        """Autentica e obtém o token de acesso"""
        auth_url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": f"{self.dataverse_url}/.default",
        }
        response = requests.post(auth_url, data=data)
        response.raise_for_status()
        self.access_token = response.json().get("access_token")
        print("Autenticado com sucesso!")

    def fetch_data(self, table_name, select_fields=None, filter_condition=None):
        """
        Faz requisição à API do Dataverse para a tabela especificada.

        Args:
            table_name (str): Nome da tabela no Dataverse.
            select_fields (str): Campos a serem selecionados, separados por vírgulas.
            filter_condition (str): Condição de filtro OData.

        Returns:
            DataFrame: PySpark DataFrame com os dados retornados.
        """
        if not self.access_token:
            raise Exception(
                "Autenticação necessária. Chame o método 'authenticate' primeiro."
            )

        # Monta o endpoint da API
        endpoint = f"{self.dataverse_url}/api/data/v9.2/{table_name}"
        if select_fields or filter_condition:
            query_params = []
            if select_fields:
                query_params.append(f"$select={select_fields}")
            if filter_condition:
                query_params.append(f"$filter={filter_condition}")
            endpoint += "?" + "&".join(query_params)

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

        records = []

        # Loop de paginação
        while endpoint:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            records.extend(data.get("value", []))
            endpoint = data.get("@odata.nextLink", None)

        # Converte os registros para PySpark DataFrame
        return self.spark.createDataFrame(records)
