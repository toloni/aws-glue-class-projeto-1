from dataverse_api import DataverseAPI

dataverse = DataverseAPI(
    tenant_id="SEU_TENANT_ID",
    client_id="SEU_CLIENT_ID",
    client_secret="SEU_CLIENT_SECRET",
    dataverse_url="https://SEU_ORGANIZATION_NAME.api.crm.dynamics.com",
)
dataverse.authenticate()

df = dataverse.fetch_data(
    table_name="accounts",
    select_fields="name,accountnumber",
    filter_condition="statecode eq 0",
)

# Exibe o DataFrame
df.printSchema()
df.show()
