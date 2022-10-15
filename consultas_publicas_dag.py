"""
Monitoramento de Publicação de Consultas Públicas
"""

import json
import re
from datetime import datetime
from typing import Tuple

import markdown
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from bs4 import BeautifulSoup

WEBSITE_URL = 'https://www.gov.br/participamaisbrasil/consultas-publicas'

def _get_soup(url: str) -> BeautifulSoup:
    page_req = requests.get(url)
    return BeautifulSoup(page_req.content, from_encoding='iso-8859-1')

def _parse(consulta: dict) -> Tuple[str, str]:
    """Parse to email fields"""
    subject = f'Nova Consulta Pública do Órgão {consulta["nom_orgao"]}'

    blocks = []
    blocks.append(f'### Nome: {consulta["nom_titulo"]}\n')
    link = f'https://www.gov.br/participamaisbrasil/{consulta["dsc_urlamigavel"]}'
    blocks.append(f'* **link**: [{link}]({link})\n')
    blocks.append(f'* **Orgão**: {consulta["nom_orgao"]}\n')
    blocks.append(f'* **sigla**: {consulta["sigla"]}\n')
    blocks.append(f'* **área**: {consulta["area"]}\n')
    blocks.append(f'* **setor**: {consulta["setor"]}\n')
    blocks.append(f'* **Data de Abertura**: {consulta["data_abertura"]}\n')
    blocks.append(f'* **Data de Encerramento**: {consulta["data_encerramento"]}\n')
    blocks.append(f'* **Status**: {consulta["titulo_status"]}\n')

    return markdown.markdown('\n'.join(blocks))

def _report_new_publications():
    soup = _get_soup(WEBSITE_URL)
    pattern = re.compile(
        r"let grupoConsultaPublica = ([\s\S]+]);",
        re.MULTILINE | re.DOTALL
    )
    js_var = pattern.search(str(soup.html)).group(1)
    consultas_publicas = json.loads(js_var)
    consultas_publicas_dict = {
        c['cod_objeto']: c
        for c in consultas_publicas
    }

    recipients_var = Variable.get('consulta_publica_recipients')
    recipients = re.split(r',|\n', recipients_var)
    recipients = list(map(str.strip, recipients))

    published_ids = set(consultas_publicas_dict.keys())
    old_published_ids = json.loads(Variable.get('consulta_publica_ids', []))

    if old_published_ids and (new_ids := published_ids - set(old_published_ids)):
        for new_publication_id in new_ids:
            new_publication = consultas_publicas_dict[new_publication_id]

            send_email(to=recipients,
                       subject='Nova Consulta Pública Publicada',
                       html_content=_parse(new_publication)
                       )

    Variable.set("consulta_publica_ids",
                 list(published_ids),
                 serialize_json=True)


default_args = {
    "owner": "nitai",
    "start_date": datetime(2022, 8, 4),
    "retries": 0,
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
