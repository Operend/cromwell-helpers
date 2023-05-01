#!/usr/bin/python3
from opyrnd.api_backed import ApiBacked
from opyrnd.entities import Entity
from opyrnd.workfile import WorkFile
import argparse


def copy_workfile(api_base_url, api_token_secret, api_verify_https,
                  workfile_id, temp_workfile_dir):
    cfg = {'api_base_url': api_base_url,
           "api_token_secret": api_token_secret,
           'verify_https': api_verify_https}

    ApiBacked.configure_from_dict(cfg)
    wf = WorkFile.get_by_system_id(workfile_id)
    new_file = open(f"{temp_workfile_dir}/{wf.originalName}", "wb")
    new_file.write(wf.open().read())
    return wf.originalName


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('api_base_url', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('api_token_secret', type=str,
                        help="The token secert")
    parser.add_argument('api_verify_https', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('workfile_id', type=str,
                        help="The base URL of the Operend Server")
    parser.add_argument('temp_workfile_dir', type=str,
                        help="The base URL of the Operend Server")
    args = parser.parse_args()
    print(args)
    copy_workfile(args.api_base_url, args.api_token_secret, args.api_verify_https, args.workfile_id,
                  args.temp_workfile_dir)