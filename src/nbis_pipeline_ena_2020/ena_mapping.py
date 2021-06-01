from csv import DictWriter, DictReader
from gzip import open as gzip_open
from pathlib import Path
from re import sub
from typing import TypedDict, List

sample_template = {
    'sample_alias': 'sample_id',
    'sample_title': "Samples from Swedish individuals positive for Covid-19 by qPCR",
    'sample_description': "Samples from Swedish individuals positive for Covid-19 by qPCR.",
    'tax_id': '2697049',
    'scientific_name': "Severe acute respiratory syndrome coronavirus 2",
    'collection date': 'collection_date',
    'geographic location (country and/or sea)': "Sweden",
    'host subject id': '9606',
    'host common name': "human",
    'host scientific name': "homo sapiens",
    'host health state': "not provided",
    'host sex': "not provided",
    'collector name': "unknown",
    'collecting institution': "Karolinska institutet",
    'isolate': "isolate",
}

paired_fastq_run_template = {
    'sample_alias': 'sample_id',
    'design_description': 'Multiplex PCR approach',
    'library_name': '',
    'library_strategy': 'AMPLICON',
    'library_source': 'VIRAL RNA',
    'library_selection': 'PCR',
    'library_layout': 'paired',
    'insert_size': '100',
    'library_construction_protocol': '',
    'platform': 'DNBSEQ',
    'instrument_model': 'DNBSEQ-G400',
    'forward_file_name': 'forward_file',  # sample_id + "__1.fq.gz",
    'forward_file_md5': '',
    'reverse_file_name': 'reverse_file',  # sample_id + "__2.fq.gz",
    'reverse_file_md5': '',
}

assembly_manifest_template = {
    'STUDY': 'study_id',
    'SAMPLE': 'sample_id',
    'ASSEMBLYNAME': 'sample_id',
    'ASSEMBLY_TYPE': 'COVID-19 outbreak',
    'COVERAGE': '50',
    'PROGRAM': 'SARS-CoV-2 Multi-PCR v1.0 (MGI Tech Ltd., Co.)',
    'MOLECULETYPE': 'genomic RNA',
    'PLATFORM': 'DNBSEQ',
    'DESCRIPTION': 'Full-length SARS-Cov-2 genomes, the regions with <50x'
                   ' sequencing depths were masked with N. Program available at:'
                   ' https://github.com/MGI-tech-bioinformatics/SARS-CoV-2_Multi-PCR_v1.0',
    'FASTA': 'fasta_file',  # sample_id + ".fasta.gz",
    'CHROMOSOME_LIST': 'chromosome_list_file',
}

chromosome_list_template = {
    'OBJECT_NAME': 'sequence_name',
    'CHROMOSOME_NAME': '1',
    'CHROMOSOME_TYPE': 'monopartite',
}

MetadataRow = TypedDict('MetadataRow', {
    'sample_id': str,
    'sampling_date': str,
})


class NPC2ENAFiles:
    study_id: str
    sample_ids: List[str]
    fasta_local_dir: str
    fastq_ftp_dir: str

    def __init__(self, study_id: str, sample_ids: List[str], local_dir: str = '', ftp_dir: str = ''):
        self.study_id = study_id
        self.sample_ids = sample_ids
        self.fasta_local_dir = Path(local_dir)
        self.fastq_ftp_dir = Path(ftp_dir)

    @staticmethod
    def from_tsv(metadata_tsv_path: Path):
        with metadata_tsv_path.open('r') as f:
            reader = DictReader(f, dialect='excel-tab')
            return [row['sample_id']for row in reader]

    def write_sample_tsv(self, fp: Path):
        with fp.open('w') as f:
            f.writelines([
                '#checklist_accession	ERC000033\r\n',
                '#unique_name_prefix\r\n'
            ])
            writer = DictWriter(f, sample_template.keys(), dialect='excel-tab')
            writer.writeheader()
            writer.writerows({
                **sample_template,
                'sample_alias': sample_id,
                'collection date': sub(r'(\d\d)(\d\d)(\d\d)', r'20\1-\2-\3', sample_id[0:6]),
                'isolate': f'SARS-CoV-2/human/SWE/NPC-{sample_id}/2020',
            } for sample_id in self.sample_ids)

    def write_sample_xml(self, fp: Path):
        template=f'''
<SAMPLE alias="{sample_alias}">
    <IDENTIFIERS>
        <SUBMITTER_ID namespace="SCILIFELAB STOCKHOLM">{sample_alias}</SUBMITTER_ID>
    </IDENTIFIERS>
    <TITLE>{sample_title}</TITLE>
    <SAMPLE_NAME>
        <TAXON_ID>{tax_id}</TAXON_ID>
        <SCIENTIFIC_NAME>{scientific_name}</SCIENTIFIC_NAME>
    </SAMPLE_NAME>
    <DESCRIPTION>{sample_description}</DESCRIPTION>
    <SAMPLE_ATTRIBUTES>{"""
        <SAMPLE_ATTRIBUTE>
            <TAG>{tag}</TAG>
            <VALUE>{value}</VALUE>
        </SAMPLE_ATTRIBUTE>"""
        for tag, value in sample_template
        if tag not in ['sample_alias', 'sample_title', 'sample_description', 'tax_id', 'scientific_name']
        }
        <SAMPLE_ATTRIBUTE>
            <TAG>ENA-CHECKLIST</TAG>
            <VALUE>{'ERC000033'}</VALUE>
        </SAMPLE_ATTRIBUTE>
    </SAMPLE_ATTRIBUTES>
</SAMPLE>'''
        with fp.open('w') as f:
            f.writelines([
                '#checklist_accession	ERC000033\r\n',
                '#unique_name_prefix\r\n'
            ])
            writer = DictWriter(f, sample_template.keys(), dialect='excel-tab')
            writer.writeheader()
            writer.writerows({
                **sample_template,
                'sample_alias': sample_id,
                'collection date': sub(r'(\d\d)(\d\d)(\d\d)', r'20\1-\2-\3', sample_id[0:6]),
                'isolate': f'SARS-CoV-2/human/SWE/NPC-{sample_id}/2020',
            } for sample_id in self.sample_ids)

    def write_run_paired_fasta_tsv(self, fp: Path):
        with fp.open('w') as f:
            writer = DictWriter(f, paired_fastq_run_template.keys(), dialect='excel-tab')
            writer.writeheader()
            writer.writerows({
                **paired_fastq_run_template,
                'sample_alias': sample_id,
                'forward_file_name': str(self.fastq_ftp_dir.joinpath(f'{sample_id}__1.fq.gz')),
                'reverse_file_name': str(self.fastq_ftp_dir.joinpath(f'{sample_id}__2.fq.gz')),
            } for sample_id in self.sample_ids)

    def write_assembly_manifest(self, fp: Path, sample_id: str):
        with fp.open('w') as f:
            f.writelines(
                f'{key}\t{val}\r\n'
                for key, val in {
                    **assembly_manifest_template,
                    'STUDY': self.study_id,
                    'SAMPLE': sample_id,
                    'ASSEMBLYNAME': sample_id,
                    'FASTA': str(self.fasta_local_dir.joinpath(f'{sample_id}.fasta.gz')),
                    'CHROMOSOME_LIST': str(fp.parent.joinpath(f'{sample_id}.chromosome_list.tsv.gz')),
                }.items()
            )
        
    def write_assembly_chromosome_tsv(self, fp: Path, object_name: str):
        with gzip_open(fp,'wt', encoding='ascii') as f:
            writer = DictWriter(f, chromosome_list_template.keys(), dialect='excel-tab')
            # writer.writeheader()
            writer.writerows([{
                **chromosome_list_template,
                'OBJECT_NAME': object_name,
            }])

    def write_assembly_manifests(self, manifest_dir: str):
        for sample_id in self.sample_ids:
            fp = Path(manifest_dir).joinpath(f'{sample_id}.manifest')
            self.write_assembly_manifest(fp, sample_id)
            fp = Path(manifest_dir).joinpath(f'{sample_id}.chromosome_list.tsv.gz')
            self.write_assembly_chromosome_tsv(fp, sample_id)

