#!/usr/bin/env python3
"""
generate_odcs_contracts.py

Gera os 26 contratos ODCS (Open Data Contract Standard / Bitol v3.0.0) em
contracts/<domain>/<entity>.odcs.yaml, um por schema Avro em schemas/<domain>/*.avsc.

- Le os campos reais de cada .avsc (nomes exatos, sem adivinhar).
- Classifica PII por campo com base na coluna "Dado pessoal (PII)" do documento
  de associacao oficial da PoC (RFP COE Data AI Tools - PoC Bases de Origem.html).
- Cita a entidade TM Forum (quando existente) na description -- sem referenciar
  DDL na description, conforme decisao already registrada no projeto; o DDL de
  origem fica apenas em sourceModelRef (campo de rastreabilidade tecnica).
- Calcula tableFingerprint como sha256 do proprio arquivo .avsc (reprodutivel).
- Preenche schema.registry.schemaId a partir de schema-registry-manifest.json
  quando disponivel (25 dos 26 -- usage ainda pendente por quota).

Uso:
    python3 generate_odcs_contracts.py
"""
import hashlib
import json
import os

BASE = os.path.dirname(os.path.abspath(__file__))
# Schemas e manifesto vivem no repo irmao dataai-pubsub. Localmente (fora do CI),
# dev-contracts/ e dev/ sao pastas irmas sob o mesmo diretorio do projeto; no CI,
# o workflow faz checkout de ambos os repos lado a lado (ver .github/workflows/).
PUBSUB_REPO = "vvivopocdataai/dataai-pubsub"
SCHEMAS_DIR = os.environ.get("SCHEMAS_DIR", os.path.join(BASE, "..", "..", "dev", "schemas"))
CONTRACTS_DIR = os.path.join(BASE, "..", "contracts")
MANIFEST_FILE = os.environ.get("MANIFEST_FILE", os.path.join(BASE, "..", "..", "dev", "schema-registry-manifest.json"))

NAMESPACE = "eh-dataai-vivopoc"
SCHEMA_GROUP = "application-schemas"

# domain-folder -> (schema group logico, topico, compatibilidade)
DOMAIN_META = {
    "arrecadacao":           ("sg-arrecadacao",           "evt-arrecadacao",           "backward"),
    "billing":                ("sg-billing",                "evt-billing",                "backward"),
    "crm":                     ("sg-crm",                     "evt-crm",                     "backward"),
    "crm_legado":             ("sg-crm-legado",             "evt-crm",                     "backward"),
    "notificacao":            ("sg-notificacao",            "evt-notificacao",            "backward"),
    "tmf-productorder":       ("sg-tmf-productorder",       "evt-tmf-productorder",       "backward"),
    "tmf-productinventory":  ("sg-tmf-productinventory",  "evt-tmf-productinventory",  "backward"),
    "tmf-productcatalog":     ("sg-tmf-productcatalog",     "evt-tmf-productcatalog",     "forward"),
    "customer_care":          ("sg-customer-care",          "evt-customer-ops",           "backward"),
    "usage":                   ("sg-usage",                   "evt-customer-ops",           "backward"),
}

# file (relative to schemas/) -> metadata
# tmf: entidade TM Forum (ou None se "sem equivalente TMF")
# ddl: (arquivo, tabela) de origem -- usado SOMENTE em sourceModelRef, nunca na description
# pii: lista de campos (nomes exatos do .avsc) considerados dado pessoal
CONTRACTS = [
    dict(file="arrecadacao/payment-method-create-event.avsc", tmf="PaymentMethod (TMF670)",
         ddl=("ddl/ps.sql", "arrecadacao.forma_pgto"), pii=["nr_final_cartao"],
         desc="Forma de pagamento associada a uma conta de cobranca (boleto, debito, cartao ou PIX)."),
    dict(file="arrecadacao/payment-received-event.avsc", tmf="Payment (TMF676)",
         ddl=("ddl/ps.sql", "arrecadacao.pagamento"), pii=[],
         desc="Pagamento recebido e sua baixa contra uma fatura."),
    dict(file="arrecadacao/return-batch-processed-event.avsc", tmf=None,
         ddl=("ddl/ps.sql", "arrecadacao.lote_retorno"), pii=[],
         desc="Processamento de lote de arquivo de retorno bancario."),
    dict(file="billing/billing-account-create-event.avsc", tmf="BillingAccount (TMF666)",
         ddl=("ddl/ps.sql", "billing.conta_cobranca"), pii=[],
         desc="Conta de cobranca do cliente no sistema de faturamento."),
    dict(file="billing/bill-create-event.avsc", tmf="CustomerBill (TMF678)",
         ddl=("ddl/ps.sql", "billing.fatura"), pii=[],
         desc="Fatura mensal emitida para uma conta de cobranca."),
    dict(file="billing/item-fatura-create-event.avsc", tmf="AppliedCustomerBillingRate (TMF678)",
         ddl=("ddl/ps.sql", "billing.item_fatura"), pii=[],
         desc="Item de cobranca de uma fatura (mensalidade, uso, servico ou desconto)."),
    dict(file="billing/doc-fatura-create-event.avsc", tmf="Document, sub-recurso de CustomerBill (TMF678)",
         ddl=("ddl/ps.sql", "billing.doc_fatura"), pii=[],
         desc="Documento (PDF) de uma fatura e suas vias."),
    dict(file="billing/bill-copy-requested-event.avsc", tmf="PartyInteraction (TMF683)",
         ddl=("ddl/ps.sql", "billing.solic_segunda_via"), pii=[],
         desc="Solicitacao de segunda via de fatura."),
    dict(file="crm/individual-create-event.avsc", tmf="Individual (TMF632)",
         ddl=("ddl/ps.sql", "crm_digital.party_individual"),
         pii=["given_name", "family_name", "birth_date", "cpf"],
         desc="Pessoa fisica cadastrada no CRM do canal digital."),
    dict(file="crm/customer-create-event.avsc", tmf="Customer (TMF629)",
         ddl=("ddl/ps.sql", "crm_digital.customer"), pii=["name"],
         desc="Papel de cliente assumido por um Individual no canal digital."),
    dict(file="crm/contact-medium-create-event.avsc", tmf="ContactMedium (TMF629)",
         ddl=("ddl/ps.sql", "crm_digital.contact_medium"), pii=["email_address", "phone_number"],
         desc="Meio de contato (e-mail ou celular) de um cliente do canal digital."),
    dict(file="notificacao/communication-template-create-event.avsc", tmf=None,
         ddl=("ddl/ps.sql", "notificacao.template"), pii=[],
         desc="Modelo de comunicacao (e-mail ou SMS)."),
    dict(file="notificacao/communication-sent-event.avsc", tmf="CommunicationMessage (TMF681)",
         ddl=("ddl/ps.sql", "notificacao.envio_comunicacao"), pii=["ds_destino"],
         desc="Comunicacao enviada ao cliente e seu status de entrega."),
    dict(file="crm_legado/legacy-customer-change-event.avsc", tmf="Customer + Individual (TMF629/632)",
         ddl=("ddl/mssql.sql", "crm_legado.tb_cli"),
         pii=["nm_cli", "nr_cpf", "dt_nasc", "ds_email", "nr_tel"],
         desc="Cadastro unificado do cliente no CRM legado (pessoa fisica + papel de cliente). Capturado via CDC."),
    dict(file="crm_legado/legacy-account-change-event.avsc", tmf="BillingAccount (TMF666)",
         ddl=("ddl/mssql.sql", "crm_legado.tb_conta"), pii=[],
         desc="Conta do cliente no CRM legado. Capturado via CDC."),
    dict(file="crm_legado/legacy-customer-address-change-event.avsc", tmf="GeographicAddress (TMF673)",
         ddl=("ddl/mssql.sql", "crm_legado.tb_end_cli"),
         pii=["ds_logradouro", "nr_logradouro", "nm_cidade", "sg_uf", "nr_cep"],
         desc="Endereco do cliente no CRM legado. Capturado via CDC."),
    dict(file="tmf-productorder/product-order-create-event.avsc", tmf="ProductOrder (TMF622)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_PED"), pii=[],
         desc="Pedido de ativacao, troca de plano ou cancelamento solicitado pelo cliente."),
    dict(file="tmf-productorder/product-order-item-create-event.avsc", tmf="ProductOrderItem (TMF622)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_ITEM_PED"), pii=[],
         desc="Item de um pedido (plano ou servico, acao e valor)."),
    dict(file="tmf-productinventory/product-create-event.avsc", tmf="Product (TMF637)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_ASSIN"), pii=["NR_MSISDN"],
         desc="Linha do cliente (assinatura movel): numero, plano, DDD, portabilidade, ativacao e cancelamento."),
    dict(file="tmf-productinventory/product-service-create-event.avsc", tmf="Product, relacionado via productRelationship (TMF637)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_SERV_ASSIN"), pii=[],
         desc="Servico adicional contratado e associado a uma linha."),
    dict(file="tmf-productinventory/simcard-create-event.avsc", tmf="Resource (TMF639)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_SIMCARD"), pii=["NR_IMSI"],
         desc="Chip (SIM card) associado a uma linha."),
    dict(file="tmf-productcatalog/plano-create-event.avsc", tmf="ProductOffering (TMF620)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_PLANO"), pii=[],
         desc="Oferta comercial: plano recorrente (pre-pago, pos-pago ou controle)."),
    dict(file="tmf-productcatalog/servico-create-event.avsc", tmf="ProductOffering (TMF620)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_SERV"), pii=[],
         desc="Oferta comercial: servico adicional avulso (dados, roaming e similares)."),
    dict(file="customer_care/service-ticket-create-event.avsc", tmf="PartyInteraction (TMF683)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_ATEND"), pii=["DS_OBS", "DS_END_INST"],
         desc="Atendimento/ligacao do cliente, registrado como uma interacao com motivo, canal e desfecho."),
    dict(file="customer_care/motivo-atend-create-event.avsc", tmf="Dominio associado a PartyInteraction (sem TMF numerado)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_MOTIVO_ATEND"), pii=[],
         desc="Dominio dos motivos de atendimento (tabela de referencia estatica)."),
    dict(file="usage/usage-create-event.avsc", tmf="Usage (TMF635)",
         ddl=("ddl/ora1.sql", "ATEND_PROD.TB_CONSUMO_MES"), pii=[],
         desc="Consumo mensal de uma linha: minutos, dados moveis, SMS e excedente."),
]


def load_manifest():
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def sha256_of_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()


def yaml_str(s, indent=0):
    """Serializa uma string simples para YAML (sem libs externas)."""
    pad = " " * indent
    if s is None:
        return "null"
    s = str(s)
    needs_quote = any(c in s for c in [":", "#", "'", '"']) or s.strip() != s
    if needs_quote:
        escaped = s.replace('"', '\\"')
        return f'"{escaped}"'
    return s


def main():
    manifest = load_manifest()
    os.makedirs(CONTRACTS_DIR, exist_ok=True)
    generated = []

    for c in CONTRACTS:
        avsc_path = os.path.join(SCHEMAS_DIR, c["file"])
        with open(avsc_path, "r", encoding="utf-8") as fh:
            schema = json.load(fh)

        event_record = schema["fields"][4]["type"]
        field_defs = event_record["fields"]
        fqn = f'{schema["namespace"]}.{schema["name"]}'
        domain = c["file"].split("/")[0]
        entity_slug = os.path.splitext(os.path.basename(c["file"]))[0]
        table_name = c["ddl"][1].split(".")[-1].lower()

        sg_logical, topic, compat = DOMAIN_META[domain]
        owner_slug = domain.replace("_", "-")
        fingerprint = sha256_of_file(avsc_path)
        reg = manifest.get(fqn)
        schema_id_line = f'schemaId: {reg["schemaId"]}' if reg else "schemaId: null  # pendente de registro (limite de 25 versoes de schema excedido no tier Standard)"

        lines = []
        lines.append("apiVersion: v3.0.0")
        lines.append("kind: DataContract")
        lines.append(f"id: {domain}.{table_name}.v1")
        lines.append(f"name: {schema['name']}")
        lines.append("version: 1.0.0")
        lines.append("status: draft")
        lines.append(f"domain: {domain}")
        lines.append(f"owner: data-steward-{owner_slug}@vivo.example")
        tmf_suffix = f" Entidade TM Forum: {c['tmf']}." if c["tmf"] else " Sem entidade TM Forum equivalente."
        lines.append(f"description: {yaml_str(c['desc'] + tmf_suffix)}")
        lines.append("sourceModelRef:")
        lines.append("  repo: vivo-dataai-poc")
        lines.append(f"  path: {c['ddl'][0]}")
        lines.append(f"  table: {c['ddl'][1]}")
        lines.append(f"  tableFingerprint: sha256:{fingerprint}  # sha256 do arquivo .avsc (schemas/{c['file']})")
        lines.append("schema:")
        lines.append("  physicalType: avro")
        lines.append(f"  repo: {PUBSUB_REPO}  # schema fisico vive no repo dataai-pubsub, nao neste repo de contratos")
        lines.append(f"  file: schemas/{c['file']}  # caminho dentro do repo acima")
        lines.append("  registry:")
        lines.append("    type: azure-event-hubs-schema-registry")
        lines.append(f"    namespace: {NAMESPACE}")
        lines.append(f"    schemaGroup: {SCHEMA_GROUP}  # unico grupo possivel no tier Standard; '{sg_logical}' e apenas o agrupamento logico/topico")
        lines.append(f"    {schema_id_line}")
        lines.append(f"    compatibility: {compat}")
        lines.append("  properties:")
        for f in field_defs:
            fname = f["name"]
            classification = "confidential" if fname in c["pii"] else "internal"
            lines.append(f"    - name: {fname}")
            lines.append(f"      classification: {classification}")
        lines.append("servers:")
        lines.append("  - type: kafka")
        lines.append(f"    bootstrapServers: {NAMESPACE}.servicebus.windows.net:9093")
        lines.append(f"    topic: {topic}")
        lines.append("    format: avro")
        lines.append("security:")
        lines.append("  authentication: entra-id-oauth2")
        pii_list = c["pii"] if c["pii"] else []
        if pii_list:
            lines.append("  piiFields:")
            for p in pii_list:
                lines.append(f"    - {p}")
        else:
            lines.append("  piiFields: []")
        lines.append("support:")
        lines.append("  approval:")
        lines.append(f"    requiredReviewers: [data-steward-{owner_slug}]")
        lines.append("    humanInTheLoop: true")
        lines.append("")  # trailing newline

        out_dir = os.path.join(CONTRACTS_DIR, domain)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{entity_slug}.odcs.yaml")
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
        generated.append(out_path)
        print(f"OK  {out_path}")

    print(f"\n{len(generated)} contratos ODCS gerados em {os.path.abspath(CONTRACTS_DIR)}")


if __name__ == "__main__":
    main()
