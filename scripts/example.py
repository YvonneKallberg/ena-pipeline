from pathlib import Path
from getpass import getpass
from gzip import open as gzip_open

# Use the webin interactive interface to register the project in ENA and get the project accession id
webin_user = 'Webin-57703'
webin_password = getpass('Webin password:')
project_id = 'PRJEB42614'
assemby_files_dir = 'data/assemblies'
ena_ftp_dir = 'ftp_dir'
manifests_dir = 'data_output'
samples_tsv = Path('samples-2021-01-20.tsv')
runs_tsv = Path('runs-2021-01-20.tsv')
webin_jar = 'lib/webin-cli-3.4.0.jar'  # download from https://github.com/enasequence/webin-cli/releases/

# %% Gzip the .fasta files for the assemblies
for input_path in Path('data_raw/nextcloud/1.assemblies/single_sequence_fasta').glob('*.fasta'):
    output_path = Path(assemby_files_dir) / f'{input_path.name}.gz'
    with input_path.open('rt') as f_in, gzip_open(output_path, 'wt') as f_out:
        f_out.writelines(f_in.readlines())

# %% Generate a list of all the sample aliases to be sumbitted
sample_list = [
    p.name[:-9]  # get the file name without the extension
    for p in Path(assemby_files_dir).glob('*.fasta.gz')
]

# %% Generate files for submitting to ENA
from nbis_pipeline_npc_ena_2020.npc_ena_mapping import NPC2ENAFiles

t = NPC2ENAFiles(project_id, sample_list, local_dir=assemby_files_dir, ftp_dir=ena_ftp_dir)
# .tsv file listing sample metadata for the Webin Interactive interface
t.write_sample_tsv(samples_tsv)
# .tsv file listing runs for the Webin Interactive interface
t.write_run_paired_fasta_tsv(runs_tsv)
# manifest files for submitting assemblies using the Webin CLI
t.write_assembly_manifests(manifests_dir)

# %% Transferring files from Nextcloud to ENA:s ftp
from nbis_pipeline_npc_ena_2020.npc_ena_transfer import NextcloudNPCReader, ENAFTPWriter, transfer

webdav_host = 'https://your.host.com/'
webdav_user = 'account-12345'
webdav_pass = getpass('Nextcloud token: ')
webdav_dir = '/remote.php/webdav/folder/path/'

ena_ftp_host = 'webin.ebi.ac.uk'

npc = NextcloudNPCReader(webdav_host, webdav_user, webdav_pass, webdav_dir)
ena = ENAFTPWriter(ena_ftp_host, webin_user, webin_password, ena_ftp_dir)
transfer(npc, ena, webdav_dir)

# %% Submission using the Webin-CLI
from npc_ena_mapping import WebinCLI

c = WebinCLI(webin_user, webin_password, test=True, webin_jar=webin_jar)
for manifest_file in Path(manifests_dir).glob('*.manifest'):
    c.webin_cli_run(str(manifest_file), submit=True)
