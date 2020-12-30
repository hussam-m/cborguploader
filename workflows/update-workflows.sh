#!/bin/sh
arvados-cwl-runner --project-uuid=cborg-j7d0g-3yx09joxonkhbru --update-workflow=cborg-7fd4e-7zy0h7uhizql6vb pangenome-generate/pangenome-generate.cwl
arvados-cwl-runner --project-uuid=cborg-j7d0g-3yx09joxonkhbru --update-workflow=cborg-7fd4e-zzk6vpo8d1k9zea fastq2fasta/fastq2fasta.cwl
