import pkg_resources
import tempfile
import magic
import subprocess
import tempfile
import logging
import re

def qc_fastq(sequence):
    schema_resource = pkg_resources.resource_stream(__name__, "validation/formats")
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(schema_resource.read())
        tmp.flush()
        val = magic.Magic(magic_file=tmp.name,
                          uncompress=False, mime=True)
    seq_type = val.from_buffer(sequence.read(4096)).lower()
    sequence.seek(0)
    if seq_type == "text/fastq":
        return "reads.fastq"
    else:
        raise ValueError("Sequence file does not look like a DNA FASTA or FASTQ")
