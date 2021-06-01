from typing import NamedTuple, List, Dict
from csv import DictReader
from pathlib import Path
from re import sub
from io import StringIO

from requests import post
from requests.auth import HTTPBasicAuth

class Sample(NamedTuple):
    alias: str
    title: str
    description: str
    tax_id: str
    scientific_name: str
    attributes: Dict[str, str]

class SampleList(NamedTuple):
    checklist_id: str
    prefix: str
    samples: List[Sample]


def read_sample_tsv(fp: Path):
    with fp.open('rt') as f:
        checklist_id = sub('^#checklist_accession\t(.*)[\r\n]+$',r'\1',f.readline())
        unique_name_prefix = sub('^#unique_name_prefix\t?(.*)[\r\n]+$',r'\1',f.readline())
        row_reader = DictReader(f, dialect='excel-tab')
        
        return SampleList(
            checklist_id,
            unique_name_prefix,
            [
                Sample(
                    alias=sample['sample_alias'],
                    title=sample['sample_title'],
                    description=sample['sample_description'],
                    tax_id=sample['tax_id'],
                    scientific_name=sample['scientific_name'],
                    attributes={
                        key:val
                        for key, val in sample.items()
                        if key not in ('sample_alias', 'sample_title', 'sample_description', 'tax_id', 'scientific_name')
                    },
                )
                for sample in row_reader
            ]
        )

def study_xml(fp: Path, project_id:str, title: str, description: str, ):
    with StringIO() as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        f.write(f'<PROJECT_SET>\n')
        f.write(f'  <PROJECT alias="{project_id}">\n')
        f.write(f'      <TITLE>{_escape(title)}</TITLE>\n')
        f.write(f'      <DESCRIPTION>{_escape(description)}</DESCRIPTION>\n')
        f.write(f'      <SUBMISSION_PROJECT>\n')
        f.write(f'         <SEQUENCING_PROJECT/>\n')
        f.write(f'      </SUBMISSION_PROJECT>\n')
        f.write(f'  </PROJECT>\n')
        f.write(f'</PROJECT_SET>\n')

        fp.write_text(f.getvalue())


def samples_tsv2xml(fp: Path, samples_list: List[Sample], checklist_id:str='', prefix:str = ''):
    with StringIO() as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        f.write('<SAMPLE_SET>\n')
        for sample in samples_list:
            f.write(f'<SAMPLE alias="{prefix+sample.alias}">\n')
            f.write(f'    <TITLE>{_escape(sample.title)}</TITLE>\n')
            f.write(f'    <SAMPLE_NAME>\n')
            f.write(f'        <TAXON_ID>{sample.tax_id}</TAXON_ID>\n')
            f.write(f'        <SCIENTIFIC_NAME>{sample.scientific_name}</SCIENTIFIC_NAME>\n')
            f.write(f'    </SAMPLE_NAME>\n')
            f.write(f'    <DESCRIPTION>{_escape(sample.description)}</DESCRIPTION>\n')
            f.write(f'    <SAMPLE_ATTRIBUTES>\n')
            for tag, value in sample.attributes.items():
                f.write(f'        <SAMPLE_ATTRIBUTE>\n')
                f.write(f'            <TAG>{tag}</TAG>\n')
                f.write(f'            <VALUE>{_escape(value)}</VALUE>\n')
                f.write(f'        </SAMPLE_ATTRIBUTE>\n')
            f.write(f'        <SAMPLE_ATTRIBUTE>\n')
            f.write(f'            <TAG>ENA-CHECKLIST</TAG>\n')
            f.write(f'            <VALUE>{checklist_id}</VALUE>\n')
            f.write(f'        </SAMPLE_ATTRIBUTE>\n')
            f.write(f'    </SAMPLE_ATTRIBUTES>\n')
            f.write(f'</SAMPLE>\n')
        f.write('</SAMPLE_SET>')
        fp.write_text(f.getvalue())

def submission_xml(fp: Path, actions: tuple = (('ADD', {}),)):
    with StringIO() as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        f.write(f'<SUBMISSION_SET>\n')
        f.write(f'    <SUBMISSION>\n')
        f.write(f'        <ACTIONS>\n')
        for action, attributes in actions:
            f.write(f'            <ACTION>\n')
            f.write(f'                <{action}{_mapper_to_attributes(attributes)}/>\n')
            f.write(f'            </ACTION>\n')
        f.write(f'        </ACTIONS>\n')
        f.write(f'    </SUBMISSION>\n')
        f.write(f'</SUBMISSION_SET>\n')
        fp.write_text(f.getvalue())

def submission_add_xml(fp: Path):
    submission_xml(fp, (
        ('ADD', {}),
    ))

def submission_add_hold_xml(fp: Path, HoldUntilDate: str):
    submission_xml(fp, (
        ('ADD', {}),
        ('HOLD', {'HoldUntilDate': HoldUntilDate}),
    ))

def submission_modify_xml(fp: Path):
    submission_xml(fp, (
        ('MODIFY', {}),
    ))

def submission_cancel_xml(fp: Path, accessions: list):
    submission_xml(fp, (
        ('CANCEL', {'target': accession})
        for accession in accessions
    ))

def submission_suppress_xml(fp: Path, accessions: list):
    submission_xml(fp, (
        ('SUPPRESS', {'target': accession})
        for accession in accessions
    ))

def submission_release_xml(fp: Path, accessions: list):
    submission_xml(fp, (
        ('SUPPRESS', {'target': accession})
        for accession in accessions
    ))

def _mapper_to_attributes(d: dict):
    return ''.join([
        f' {k}="{_escape(v)}"' for k, v in d.items()
    ])

def submit_xml(fp: Path, webin_user, webin_pass, test=True,  **files):
    host = 'wwwdev.ebi.ac.uk' if test else 'www.ebi.ac.uk'
    response = post(
        f'https://{host}/ena/submit/drop-box/submit/',
        auth=HTTPBasicAuth(webin_user, webin_pass),
        files={
            key.upper(): (path.name, path.open('rt', encoding='utf-8'))
            for key, path in files.items()
        },
    )
    response.raise_for_status()
    fp.write_text(response.text)


def _escape(txt):
    return txt.translate(_xml_translations)

_xml_translations = str.maketrans({
    "<": "&lt;",
    ">": "&gt;",
    "&": "&amp;",
    "'": "&apos;",
    '"': "&quot;",
})