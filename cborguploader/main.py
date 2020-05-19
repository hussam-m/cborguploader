import argparse
import time
import arvados
import arvados.collection
import json
import magic
from pathlib import Path
import urllib.request
import socket
import getpass
import sys
import os
import json
import yaml
sys.path.insert(0,'.')
from cborguploader.qc_metadata import qc_metadata
from cborguploader.qc_fasta import qc_fasta
from cborguploader.qc_fastq import qc_fastq

ARVADOS_API_HOST=os.environ.get('ARVADOS_API_HOST', 'cborg.cbrc.kaust.edu.sa')
ARVADOS_API_TOKEN=os.environ.get('ARVADOS_API_TOKEN', '')
UPLOAD_PROJECT='cborg-j7d0g-zcdm4l3ts28ioqo'

def main():
    parser = argparse.ArgumentParser(description='Upload SARS-CoV-19 sequences for analysis')
    parser.add_argument('--fasta', type=argparse.FileType('r'), default=None, help='sequence FASTA')
    parser.add_argument('--fastq1', type=argparse.FileType('r'), default=None, help='sequence FASTQ')
    parser.add_argument('--fastq2', type=argparse.FileType('r'), default=None, help='sequence FASTQ second read for paired-end reads')
    parser.add_argument('--metadata', type=argparse.FileType('r'), help='sequence metadata json')
    parser.add_argument("--validate", action="store_true", help="Dry run, validate only")
    args = parser.parse_args()

    print(ARVADOS_API_HOST, ARVADOS_API_TOKEN)
    api = arvados.api(host=ARVADOS_API_HOST, token=ARVADOS_API_TOKEN, insecure=True)

    if args.fasta:
        try:
            target = qc_fasta(args.fasta)
        except ValueError as e:
            print(e)
            exit(1)
    elif args.fastq1:
        try:
            target = qc_fastq(args.fastq1)
            if args.fastq2:
                qc_fastq(args.fastq2)
                target = ['reads1.fastq', 'reads2.fastq']
        except ValueError as e:
            print(e)
            exit(1)
    else:
        print('Please provide a sequence in FASTA or FASTQ formats')
        exit(1)

    if not qc_metadata(args.metadata.name):
        print("Failed metadata qc")
        exit(1)

    if args.validate:
        print("Valid")
        exit(0)

    col = arvados.collection.Collection(api_client=api)

    print("Reading metadata")
    with col.open("metadata.yaml", "w") as f:
        metadata = args.metadata.read()
        f.write(metadata)
    args.metadata.close()
    metadata = yaml.load(metadata, Loader=yaml.FullLoader)
    seqlabel = metadata['sample']['sample_id']
    if args.fasta:
        with col.open('sequence.fasta', 'w') as f:
            r = args.fasta.read(65536)
            while r:
                f.write(r)
                r = args.fasta.read(65536)
        args.fasta.close()
    elif args.fastq1:
        with col.open('reads1.fastq', 'w') as f:
            r = args.fastq1.read(65536)
            seqlabel = r[1:r.index("\n")]
            while r:
                f.write(r)
                r = args.fastq1.read(65536)
        args.fastq1.close()
        if args.fastq2:
            with col.open('reads2.fastq', 'w') as f:
                r = args.fastq2.read(65536)
                while r:
                    f.write(r)
                    r = args.fastq2.read(65536)
            args.fastq2.close()
        
    external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')

    try:
        username = getpass.getuser()
    except KeyError:
        username = "unknown"

    properties = {
        "sequence_label": seqlabel,
        "upload_app": "cborguploader",
        "upload_ip": external_ip,
        "upload_user": "%s@%s" % (username, socket.gethostname())
    }

    result = col.save_new(owner_uuid=UPLOAD_PROJECT, name="%s uploaded by %s from %s" %
                 (seqlabel, properties['upload_user'], properties['upload_ip']),
                 properties=properties, ensure_unique_name=True)
    print(json.dumps(col.api_response()))

if __name__ == "__main__":
    main()
