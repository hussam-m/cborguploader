#!/usr/bin/env python
import click as ck
import arvados
from arvados.collection import CollectionReader
import os
import gzip
from Bio import SeqIO
import urllib
import getpass
import json
import yaml
import socket
import subprocess
import tempfile
import logging


ARVADOS_API_HOST = os.environ.get('ARVADOS_API_HOST', 'cborg.cbrc.kaust.edu.sa')
ARVADOS_API_TOKEN = os.environ.get('ARVADOS_API_TOKEN', '')

def run_workflow(api, parent_project, workflow_uuid, name, inputobj):
    project = api.groups().create(body={
        "group_class": "project",
        "name": name,
        "owner_uuid": parent_project,
    }, ensure_unique_name=True).execute()

    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(json.dumps(inputobj, indent=2).encode('utf-8'))
        tmp.flush()
        cmd = ["arvados-cwl-runner",
               "--submit",
               "--no-wait",
               "--project-uuid=%s" % project["uuid"],
               "arvwf:%s" % workflow_uuid,
               tmp.name]
        logging.info("Running %s" % ' '.join(cmd))
        proc = subprocess.run(cmd, capture_output=True)
    return project, proc

def get_cr_state(api, cr):
    if cr['container_uuid'] is None:
        return cr['state']
    c = api.containers().get(uuid=cr['container_uuid']).execute()
    if cr['state'] == 'Final' and c['state'] != 'Complete':
        return 'Cancelled'
    elif c['state'] in ['Locked', 'Queued']:
        if c['priority'] == 0:
            return 'On hold'
        else:
            return 'Queued'
    elif c['state'] == 'Complete' and c['exit_code'] != 0:
        return 'Failed'
    elif c['state'] == 'Running':
        if c['runtime_status'].get('error', None):
            return 'Failing'
        elif c['runtime_status'].get('warning', None):
            return 'Warning'
    return c['state']


def submit_new_request(
        api, workflows_project, workflow_uuid, sample_id,
        portable_data_hash, is_paired):
    inputobj = {
        "ref_fasta": {
            "class": "File",
            "location": "keep:9df5dcc0054bfc9e588f1273e9974c72+474/NC_045512.2.fasta"
        },
        "sample_id": sample_id
    }
    inputobj["fastq_forward"] = {
        "class": "File",
        "location": "keep:%s/reads1.fastq" % portable_data_hash
    }
    if is_paired:
        inputobj["fastq_reverse"] = {
            "class": "File",
            "location": "keep:%s/reads2.fastq" % portable_data_hash
        }
    name = f'Generate FASTA for {sample_id}'
    project, proc = run_workflow(
        api, workflows_project, workflow_uuid, name, inputobj)
    status = 'error'
    container_request = None
    if proc.returncode != 0:
        logging.error(proc.stderr.decode('utf-8'))
    else:
        output = proc.stderr.decode('utf-8')
        lines = output.splitlines()
        if lines[-2].find('container_request') != -1:
            container_request = lines[-2].split()[-1]
            status = 'submitted'
    return container_request, status


def submit_pangenome(
        api, workflows_project, pangenome_workflow_uuid, data):
    inputobj = {
        "gff_files": [],
        "reference": {
            "class": "File",
            "location": "keep:1630555a9f4d1d70d5bc19ac5f1d6800+133/reference.fasta"
        },
        "reference_gb": {
            "class": "File",
            "location": "keep:1630555a9f4d1d70d5bc19ac5f1d6800+133/reference.gb"
        },
        "metadata": {
            "class": "File",
            "location": "keep:e5c2e53119ea3aa1d0a2fd44de1d1a69+60/metadata.tsv"
        },
        "dirs": [],
    }
    for s_id, pdh in data:
        inputobj["gff_files"].append({
            "class": "File",
            "location": f'keep:{pdh}/{s_id}.gff'})
        inputobj["dirs"].append({
            "class": "Directory",
            "location": f'keep:{pdh}/{s_id}'})
    
    name = f'Pangenome analysis for'
    project, proc = run_workflow(
        api, workflows_project, pangenome_workflow_uuid, name, inputobj)
    status = 'error'
    container_request = None
    if proc.returncode != 0:
        logging.error(proc.stderr.decode('utf-8'))
    else:
        output = proc.stderr.decode('utf-8')
        lines = output.splitlines()
        if lines[-2].find('container_request') != -1:
            container_request = lines[-2].split()[-1]
            status = 'submitted'
    return container_request, status


    
@ck.command()
@ck.option('--uploader-project', '-up', default='cborg-j7d0g-nyah4ques5ww7pk', help='Uploader project uuid')
@ck.option('--workflows-project', '-wp', default='cborg-j7d0g-3yx09joxonkhbru', help='Workflows project uuid')
@ck.option('--fasta-workflow-uuid', '-mwid', default='cborg-7fd4e-zzk6vpo8d1k9zea', help='FASTQ2FASTA workflow uuid')
@ck.option('--pangenome-workflow-uuid', '-pwid', default='cborg-7fd4e-7zy0h7uhizql6vb', help='Pangenome workflow uuid')
@ck.option('--pangenome-result-col-uuid', '-prcid', default='cborg-4zz18-7hurjl2943atdoz', help='Pangenome results collection uuid')
def main(uploader_project, workflows_project, fasta_workflow_uuid, pangenome_workflow_uuid, pangenome_result_col_uuid):    
    api = arvados.api('v1', host=ARVADOS_API_HOST, token=ARVADOS_API_TOKEN)
    col = arvados.collection.Collection(api_client=api)
    state = {}
    if os.path.exists('state.json'):
        state = json.loads(open('state.json').read())
    reads = arvados.util.list_all(api.collections().list, filters=[["owner_uuid", "=", uploader_project]])
    subprojects = arvados.util.list_all(api.groups().list, filters=[["owner_uuid", "=", uploader_project]])
    for sp in subprojects:
        subreads = arvados.util.list_all(api.collections().list, filters=[["owner_uuid", "=", sp['uuid']]])
        reads += subreads
    update_pangenome = False
    pangenome_data = []
    print('Total number of uploaded sequences:', len(reads))
    for it in reads:
        col = api.collections().get(uuid=it['uuid']).execute()
        if 'sequence_label' not in it['properties']:
            continue
        sample_id = it['properties']['sequence_label']
        if 'analysis_status' in it['properties']:
            pangenome_data.append((sample_id, col['portable_data_hash']))
            continue
        if sample_id not in state:
            state[sample_id] = {
                'status': 'new',
                'container_request': None,
                'output_collection': None,
            }
        sample_state = state[sample_id]
        if sample_state['status'] == 'new' and not it['properties']['is_fasta']:
            container_request, status = submit_new_request(
                api, workflows_project, fasta_workflow_uuid, sample_id,
                it['portable_data_hash'], it['properties']['is_paired'])
            sample_state['status'] = status
            sample_state['container_request'] = container_request
            print(f'Submitted analysis request for {sample_id}')
        elif sample_state['status'] == 'submitted':
            # TODO: check container request status
            if sample_state['container_request'] is None:
                raise Exception("Container request cannot be empty when status is submitted")
            cr = api.container_requests().get(
                uuid=sample_state["container_request"]).execute()
            cr_state = get_cr_state(api, cr)
            print(f'Container request for {sample_id} is {cr_state}')
            if cr_state == 'Complete':
                out_col = api.collections().get(uuid=cr["output_uuid"]).execute()
                sample_state['output_collection'] = cr["output_uuid"]
                sample_state['status'] = 'complete'
                # Copy output files to reads collection
                it['properties']['analysis_status'] = 'complete'
                api.collections().update(
                    uuid=it['uuid'],
                    body={"manifest_text": col["manifest_text"] + out_col["manifest_text"],
                          "properties": it["properties"]}).execute()
                pangenome_data.append((sample_id, col['portable_data_hash']))
                # update_pangenome = True
            elif cr_state == 'Failed':
                state[sample_id] = {
                    'status': 'new',
                    'container_request': None,
                    'output_collection': None,
        
                }
        elif sample_state['status'] == 'complete':
            # TODO: do nothing
            pass
    if update_pangenome:
        container_request, status = submit_pangenome(api, workflows_project, pangenome_workflow_uuid, pangenome_data)
        if status == 'submitted':
            state['last_pangenome_request'] = container_request
            state['last_pangenome_request_status'] = 'submitted'
            print('Submitted pangenome request', container_request)
    elif 'last_pangenome_request' in state:
        cr = api.container_requests().get(
            uuid=state["last_pangenome_request"]).execute()
        cr_state = get_cr_state(api, cr)
        print(f'Container request for pangenome workflow is {cr_state}')
        if state['last_pangenome_request_status'] == 'submitted' and cr_state == 'Complete':
            print('Updating results collection')
            out_col = api.collections().get(uuid=cr["output_uuid"]).execute()
            api.collections().update(
                uuid=pangenome_result_col_uuid,
                body={"manifest_text": out_col["manifest_text"]}).execute()
            state['last_pangenome_request_status'] = 'complete'

    
    with open('state.json', 'w') as f:
        f.write(json.dumps(state))


if __name__ == '__main__':
    main()
