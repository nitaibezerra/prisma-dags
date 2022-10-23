"""
Monitoramento de Publicação de Consultas Públicas.

Esta DAG realiza diariamente a pesquisa por novas consultas públicas
publicadas no portal de consultas públicas do governo federal e envia
por email os resultados encontrados para os destinatários cadastrados.

Os emails dos destinatários devem ser cadastrados na variável do airflow
com nome `consulta_publica_recipients` (um por linha ou separados por
vírgula).
"""

import json
import re
from datetime import datetime
from typing import List, Tuple

import markdown
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.email import send_email
from bs4 import BeautifulSoup

WEBSITE_URL = 'https://www.gov.br/participamaisbrasil/consultas-publicas'

def _get_soup(url: str) -> BeautifulSoup:
    page_req = requests.get(url)
    return BeautifulSoup(page_req.content, from_encoding='iso-8859-1')

def _parse(consulta: dict) -> Tuple[str, str]:
    """Parse to email fields"""
    subject = f'Nova Consulta Pública do Órgão {consulta["nom_orgao"]}'

    link = ('https://www.gov.br/participamaisbrasil/'
            f'{consulta["dsc_urlamigavel"]}')
    blocks = [
        f'### Nome: {consulta["nom_titulo"]}\n',
        f'* **link**: [{link}]({link})\n',
        f'* **Orgão**: {consulta["nom_orgao"]}\n',
        f'* **sigla**: {consulta["sigla"]}\n',
        f'* **área**: {consulta["area"]}\n',
        f'* **setor**: {consulta["setor"]}\n',
        f'* **Data de Abertura**: {consulta["data_abertura"]}\n',
        f'* **Data de Encerramento**: {consulta["data_encerramento"]}\n',
        f'* **Status**: {consulta["titulo_status"]}\n',
    ]

    return markdown.markdown('\n'.join(blocks))

def _report_new_publications():
    soup = _get_soup(WEBSITE_URL)
    pattern = re.compile(
        r"let grupoConsultaPublica = ([\s\S]+]);",
        re.MULTILINE | re.DOTALL
    )
    js_var = pattern.search(str(soup.html)).group(1)

    consultas_publicas_dict = {
        c['cod_objeto']: c
        for c in json.loads(js_var)
    }

    published_ids = set(consultas_publicas_dict.keys())
    old_published_ids = json.loads(Variable.get('consulta_publica_ids', []))

    if old_published_ids:
        new_ids = published_ids - set(old_published_ids)
        new_publications = [
            consultas_publicas_dict[new_id] for new_id in new_ids
        ]

        send_new_publications(new_publications)

    Variable.set("consulta_publica_ids",
                 list(published_ids),
                 serialize_json=True)


def send_new_publications(publications: List[dict]):
    recipients_var = Variable.get('consulta_publica_recipients')
    recipients = re.split(r',|\n', recipients_var)
    recipients = list(map(str.strip, recipients))

    for publication in publications:
        html_content = _parse(publication)
        for recipient in recipients:
            send_email(to=[recipient],
                       subject='Nova Consulta Pública Publicada',
                       html_content=html_content)


default_args = {
    "owner": "nitai",
    "start_date": datetime(2022, 8, 4),
    "retries": 3,
}

@dag(
    dag_id="monitora_consultas_publicas",
    schedule_interval="0 3 * * *",
    default_args=default_args,
    catchup=False,
    tags=["govbr", "consulta_publica"]
)
def monitora_consultas_publicas():

    @task
    def report_new_publications():
        _report_new_publications()

    report_new_publications()

dag = monitora_consultas_publicas()
