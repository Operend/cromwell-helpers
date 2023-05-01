#!/usr/bin/python3
from opyrnd.api_backed import ApiBacked
from opyrnd.entities import Entity
from opyrnd.workfile import WorkFile
import argparse


def copy_fastqs(api_base_url, api_token_secret, api_verify_https,
                fastq_query, temp_workfile_dir):
    cfg = {'api_base_url': api_base_url,
           "api_token_secret": api_token_secret,
           'verify_https': api_verify_https}

    ApiBacked.configure_from_dict(cfg)
    tsv_file = open(f"{temp_workfile_dir}/this_tsv_file.tsv", "w")
    tsv_file.write('SAMPLE_ID\tR1_ID\tR2_ID\tR1_NAME\tR2_NAME\n')
    # expecting an entity that returns the fastq workfiles in
    # an array: [r1,r2]
    # Assumes at least r1 MUST Exist
    entities = Entity.get_by_query_params(fastq_query)
    for ent in entities:
        wf_1_name = ""
        wf_2_name = ""
        wf_array = ent.__getitem__('workFiles')
        r1 = ent.__getitem__('workFiles')[0]
        wf_1_name = WorkFile.get_by_system_id(r1).originalName

        if len(wf_array) > 1:
            r2 = ent.__getitem__('workFiles')[1]
            wf_2_name = WorkFile.get_by_system_id(r2).originalName


        tsv_file.write(f"ample\t{r1}\t{r2}\t{wf_1_name}\t{wf_2_name}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('api_base_url', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('api_token_secret', type=str,
                        help="The token secert")
    parser.add_argument('api_verify_https', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('fastq_query', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('temp_workfile_dir', type=str,
                        help="The base URL of the Operend Server")
    args = parser.parse_args()
    print(args)
    copy_fastqs(args.api_base_url, args.api_token_secret, args.api_verify_https, args.fastq_query,
                args.temp_workfile_dir)