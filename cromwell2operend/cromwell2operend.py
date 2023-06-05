import sys
import os
import argparse
import json
from opyrnd import ApiBacked, EntityClass, Entity;


class CromwellIO:
    """Takes Cromwell metadata (as gathered via a json.load() of 
    Cromwell's -m output file) and accumulates .input_rows and .output_rows,
    dicts of key-value dicts. The outer keys are integers relating to 
    Cromwell shard numbering, and the inner keys are dot-separated names
    relating to Cromwell fields. The values are whatever types they were
    in the JSON parse. 

    input_rows_and output_rows have the same outer keys, and.row_numbers is
    those keys sorted in numerically ascending order.

    This assumes the important part of the Cromwell run is performing a
    scatter, so that each shard of that scatter corresponds to a row. If there
    are multiple scatters, they need to be non-nested and to be ordered so 
    that corresponding rows refer to the same entity."""
    def __init__(self,run_metadata):
        self.run_metadata=run_metadata
        self.input_rows={}
        self.output_rows={}
        self.clean=True;
        self.gather_data();
        for k in self.input_rows.keys():
            if k not in self.output_rows:
                self.output_rows[k]={}
        for k in self.output_rows.keys():
            if k not in self.input_rows:
                self.input_rows[k]={}
        self.row_numbers=sorted(self.input_rows.keys());
        if not self.clean:
            raise Exception("Parser reached an ambiguity reading Cromwell metadata. (This is probably a bug in the parser.)");
    def record_input(self,row,key,value):
        if row not in self.input_rows:
           self.input_rows[row]={}
        if key in self.input_rows[row] and value!=self.input_rows[row][key]:
            # if this happens, the row number logic is probably wrong;
            # we reach this point if we are looking in two different places
            # in the metadata and think we have the same input name and
            # the same shard number.
            print(f"confused by duplicate input key {key} for row {row}",file=sys.stderr);
            print(f"saw values {self.input_rows[row][key]} and {value}")
            self.clean=False;
        else:
            self.input_rows[row][key]=value;
    def record_output(self,row,key,value):
        if row not in self.output_rows:
           self.output_rows[row]={}
        if key in self.output_rows[row] and value!=self.output_rows[row][key]:
            # if this happens, the row number logic is probably wrong;
            # we reach this point if we are looking in two different places
            # in the metadata and think we have the same output name and
            # the same shard number.
            print(f"confused by duplicate output key {key} for row {row}",file=sys.stderr);
            self.clean=False;
        else:
            self.output_rows[row][key]=value;
            
    def gather_data(self):
        for k in self.run_metadata["calls"]:
            shards=self.run_metadata["calls"][k];
            if "." in k:
                call_name= ".".join(k.split(".")[1:]);
            else:
                call_name=k;
            for i in range(len(shards)):
                shard_index=shards[i]["shardIndex"]
                if shard_index>-1:
                    if "inputs" in shards[i]:
                        for ki in shards[i]["inputs"]:                   
                            self.record_input(shard_index,f"{call_name}.{ki}",
                                              shards[i]["inputs"][ki]);
                    if "outputs" in shards[i]:
                        for ki in shards[i]["outputs"]:
                            self.record_output(shard_index,f"{call_name}.{ki}",
                                               shards[i]["outputs"][ki]);
                if "subWorkflowMetadata" in shards[i]:
                    self.gather_subcall_data(call_name,
                                             shard_index,
                                             shards[i]["subWorkflowMetadata"])

    def gather_subcall_data(self,parent_name, parent_row, sub_meta):
        for k in sub_meta["calls"]:
            shards=sub_meta["calls"][k];
            for i in range(len(shards)):
                shard_index=shards[i]["shardIndex"]
                if parent_row==-1: 
                    row=shard_index
                else:
                    if shard_index>-1:
                        # if this happens, the row number logic is probably
                        # wrong; we reach this point if we've already chosen
                        # a row number based on a shard number, then inside
                        # a workflow on that shard we see a shard number again.
                        print(f"confused by subsharding, "
                              "{parent_name} has too many {k}",
                              file=sys.stderr);
                        self.clean=False
                    row=parent_row
                if row>-1:
                    if "inputs" in shards[i]:
                        for ki in shards[i]["inputs"]:                   
                            self.record_input(row,f"{k}.{ki}",
                                              shards[i]["inputs"][ki]);
                    if "outputs" in shards[i]:
                        for ki in shards[i]["outputs"]:
                            self.record_output(row,f"{k}.{ki}",
                                               shards[i]["outputs"][ki]);
                if "subWorkflowMetadata" in shards[i]:
                    # if this happens, a case I didn't code for has arisen.
                    # It will be a quick fix to support if it ever happens;
                    # we just need to call gather_subcall_data recursively!
                    # We're not doing that now because I'm not sure how
                    # the dotted naming of nested subworkflows works, so
                    # I don't know how much to trim off parent_name in the
                    # subcall to get the right concatenated name. Just one
                    # example of Cromwell metadata that triggers this line
                    # should be enough to know what the convention is.
                    print("Confused by nesting depth, {k} is a sharded subworkflow with internal subworkflows.");
                    self.clean=False;

                            
    def print_matches(self,io_spec):        
        row_indices=set(self.input_rows.keys());
        row_indices.update(self.output_rows.keys());
        row_indices=list(row_indices);
        row_indices.sort();
        print("\t".join(io_spec["inputs"])+"\t"+
              "\t".join(io_spec["outputs"]));        
        for row_index in row_indices:
            fields=[]
            for input_key in io_spec["inputs"]:
                if (row_index in self.input_rows and
                    input_key in self.input_rows[row_index]):
                    fields.append(str(self.input_rows[row_index][input_key]));
                else:
                    fields.append("");
            for output_key in io_spec["outputs"]:
                if (row_index in self.output_rows and
                    output_key in self.output_rows[row_index]):
                    fields.append(str(self.output_rows[row_index][output_key]));
                else:
                    fields.append("");            
            print("\t".join(fields))
    def print_input_keys(self):
        keys=set()
        for row in self.input_rows:
            keys.update(set(self.input_rows[row].keys()))        
        keys=list(keys);
        keys.sort();
        print(keys)
    def print_output_keys(self):
        keys=set()
        for row in self.output_rows:
            keys.update(set(self.output_rows[row].keys()))
        keys=list(keys);
        keys.sort();
        print(keys)                

def list_metadata(metadata_filename):
    table=CromwellIO(json.load(open(metadata_filename)));
    input_examples={}
    output_examples={}
    for row in table.input_rows.values():
        for k in row:
            v=row[k]
            if k not in input_examples or (v!=None and input_examples[k]==None):
                input_examples[k]=v;
    for row in table.output_rows.values():
        for k in row:
            v=row[k]
            if k not in output_examples or (v!=None and output_examples[k]==None):
                output_examples[k]=v;
    # The point here is to see the data types; seeing the entire string for a
    # filename or long string value shouldn't be important, so this snips out
    # middles to keep the concise. repr() is used instead of str() so that
    # string-shaped numbers will be visibly different from actual strings.
    print(f"{len(input_examples)} INPUTS. Names and example values:");
    for k in sorted(input_examples.keys()):
        v=repr(input_examples[k]);
        if len(v)>40 :
           v=v[:17]+" [...] "+v[-17:]
        print(f"{k}\t{v}");
    print(f"\n{len(output_examples)} OUTPUTS. Names and example values:")
    for k in sorted(output_examples.keys()):
        v=repr(output_examples[k]);
        if len(v)>40 :
           v=v[:17]+" [...] "+v[-17:]
        print(f"{k}\t{v}");

class IOMapping:
    """Takes the data describing a mapping from Cromwell to Operend 
    (as gathered via a json.load() from a suitable JSON file) for
    application to a CromwellIO object.
    """;
    def __init__(self,manifest_data, mock_filename=None):
        self.mock_filename=mock_filename;
        def grab(key, validator, validTypeName):
            if key not in manifest_data:
                raise Exception(f"key '{key}' not in manifest");
            if not validator(manifest_data[key]):
                raise Exception(f"{key} in manifest isn't a {validTypeName}");
            return manifest_data[key];
        self.entity_class=grab('entityClass',
                              lambda v: isinstance(v,str),
                              'string');
        def is_string_dict(v):
            if not isinstance(v,dict):
                return False;
            for k in v:
                if not isinstance(k,str):
                    return False;
                if not isinstance(v[k],str):
                    return False;
            return True;
        self.input_values=grab('inputValues',is_string_dict,'dict of strings');
        self.output_files=grab('outputFiles',is_string_dict,'dict of strings');
        if 'inputFiles' in manifest_data:
            raise Exception("inputFiles is not yet supported.");
        if 'outputValues' in manifest_data:
           raise Exception("outputValues is not yet supported.");
        if len(manifest_data)>3:
            raise Exception("Manifest contains keys other than entityClass, inputValues, and outputFiles.");
    def dry_validate(self, cromwell_io):
        for k in self.input_values:
            if not any( (k in row) for row in cromwell_io.input_rows.values()):
                print (cromwell_io.input_rows[0].keys())
                raise Exception(f"Manifest specifies input {k}, which is not found in the Cromwell metadata.");
        for k in self.output_files:
            if not any( (k in row) for row in cromwell_io.output_rows.values()):
                raise Exception(f"Manifest specifies output {k}, which is not found in the Cromwell metadata.");
        for k in self.output_files:
            for row in cromwell_io.output_rows.values():
                if k in row and row[k]!=None:
                    fname=row[k];
                    # Check that the file exists, and that permissions
                    # allow opening it for reading.
                    self.open_file(fname).close();
    def validate(self, cromwell_io):
        # Local checks first...
        self.dry_validate(cromwell_io);
        # Now, does it match the entity class?
        ec=EntityClass.get_by_name(self.entity_class);
        if not ec:
            raise Exception(f"Operend server has no visible entity class named {self.entity_class}");
        
    def open_file(self,fname):
        return open(self.mock_filename or fname);
            
def very_dry_run(metadata_filename, manifest_filename,
                 job_run_id, mock_filename=None):
    table= CromwellIO(json.load(open(metadata_filename)));
    manifest=IOMapping(json.load(open(manifest_filename)), mock_filename);
    manifest.dry_validate(table);
    dry_run_posts(table, manifest,
                  job_run_id, mock_filename);

def dry_run(metadata_filename, manifest_filename,
                 job_run_id, mock_filename=None):
    table= CromwellIO(json.load(open(metadata_filename)));
    manifest=IOMapping(json.load(open(manifest_filename)), mock_filename);
    manifest.validate(table);
    dry_run_posts(table, manifest,
                  job_run_id, mock_filename);
    
def dry_run_posts(table, manifest,
                  job_run_id, mock_filename=None):
    next_wfid=101
    def mock_wfid():
        nonlocal next_wfid;
        this_wfid=next_wfid
        next_wfid = next_wfid+1;
        return this_wfid
    jr_wfids={}
    for row in table.row_numbers:
        mock_entity={}
        mock_entity['_class']=manifest.entity_class;
        for k in manifest.input_values:
            if k in table.input_rows[row] and table.input_rows[row][k]!=None:
                mock_entity[manifest.input_values[k]]=table.input_rows[row][k]
        for k in manifest.output_files:
            if k in table.output_rows[row]:
                fname=table.output_rows[row][k];
                wfid=mock_wfid()
                print(f"would be POSTing file {fname}... pretending it has wfid {wfid}");
                field_name=manifest.output_files[k];
                mock_entity[field_name]=wfid;
                if field_name in jr_wfids:
                    jr_wfids[field_name].append(wfid)
                else:
                    jr_wfids[field_name]=[wfid];
        print(f"would be posting entity {mock_entity}...");
    if job_run_id!=None:
        print(f"would be updating job run {job_run_id} with file outputs {jr_wfids}");

        
def main(argv):
    parser=argparse.ArgumentParser();
    parser.add_argument('METADATA', help="Filename of Cromwell metadata JSON, as output from Cromwell's -m.")
    parser.add_argument('MANIFEST', nargs='?', help="Filename of JSON mapping from Cromwell field names to Operend entity schema. (Required except for --list)");
    parser.add_argument('JOB_RUN_ID', nargs="?", help="Operend ID of JobRun to update. If omitted, only Entities will be posted to Operend, with no JobRun update.");
    parser.add_argument('-i','--ini',help='ini file for Operend credentials. If omitted, looks for filename in OPYRND_CONFIG environment variable.')
    parser.add_argument('-l','--list',action='store_true',help='Just lists the inputs and outputs found in the Cromwell metadata, with one example value each, and exits.');
    parser.add_argument('--dry-run',action='store_true',help='validates against Operend server, but just print instead of writing output to the server.');
    parser.add_argument('--very-dry-run',action='store_true',help='do not contact Operend server, just validate as far as possible without doing that and print');
    parser.add_argument('--mock-file',help="use this filename for all file uploads, instead of the actual Cromwell output file (CAUTION: If you don't also --dry-run or --very-dry-run, this will end up sending the mock data to the Operend server, annotated like it's real!)")
    if len(argv)==0:
        parser.print_help();
        return;
    parsed_args=parser.parse_args(argv[1:]);
    if parsed_args.list:
        list_metadata(parsed_args.METADATA)
        return;
    if not parsed_args.MANIFEST:
        parser.print_help();
        print("For the requested usage, the MANIFEST argument is required.");
        return;
    if parsed_args.very_dry_run:
        very_dry_run(parsed_args.METADATA,
                     parsed_args.MANIFEST,
                     parsed_args.JOB_RUN_ID,
                     parsed_args.mock_file);
        return;
    ini=parsed_args.ini or os.getenv('OPYRND_CONFIG')
    if not ini:
        parser.print_help();
        print("For the requested usage, either the --ini argument or OPYRND_CONFIG environment variable is required.");
        return;
    ApiBacked.configure_from_file(ini);
    if parsed_args.dry_run:
        dry_run(parsed_args.METADATA,
                parsed_args.MANIFEST,
                parsed_args.JOB_RUN_ID,
                parsed_args.mock_file);
        return;
    full_run(parsed_args.METADATA,
             parsed_args.MANIFEST,
             parsed_args.JOB_RUN_ID,
             parsed_args.mock_file);

if __name__=="__main__":
    main(sys.argv)

