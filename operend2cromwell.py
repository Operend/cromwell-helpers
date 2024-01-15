# given:
# entity query
# template json
# download directory name

# make:
# download directory contents
# template-populated json

import sys
import os,os.path
import argparse
import json
import hashlib
import shutil

from opyrnd import ApiBacked, Entity, WorkFile, EntityClass;
from opyrnd.jobs import JobRun;

class FileIdTemplate:
    def __init__(self,v,prefix,suffix):
        self.value=v
        self.prefix=prefix
        self.suffix=suffix

class TemplateFiller:
    def __init__(self, template_in, json_out, download_dir,
                 mock_file=None):
        self.template_in_fname=template_in;
        self.json_out_fname=json_out
        self.download_dir=download_dir
        self.mock_file=mock_file
    def run(self):
        outfile=open(self.json_out_fname,"w");
        template=json.load(open(self.template_in_fname));
        if "_query" not in template:
            raise Exception("Template JSON must have a _query in it.");
        if not os.path.isdir(self.download_dir):
            os.mkdir(self.download_dir)
        query=template["_query"]
        self.entities=Entity.get_by_query_params(template["_query"]);
        if len(self.entities)==0:
            raise Exception("Query returned no entities.");
        # assumption: Either we're querying one class, or the fields we
        #  want are defined consistently across all the classes the
        #  query can match.
        self.entity_class = \
            EntityClass.get_by_name(self.entities[0].entity_class_name);
        del template["_query"];
        self.wanted_file_ids=set();
        self.downloaded_file_paths={}
        for fieldname in template:
            template_value=template[fieldname]
            template[fieldname]=self.resolve_template_value(template_value);
        self.download_wanted_files();
        print("Downloaded the following files:");
        print(self.downloaded_file_paths)
        json.dump(template, outfile,
                  default=lambda x:self.resolve_to_final_string(x));    
    def resolve_template_value(self, template_value):
        if not isinstance(template_value,str):
            return template_value;
        if isinstance(template_value,str):
            # ASSUMPTIONS (we can add more cases later if they come up;
            #  these are based on the cases we've seen Terra's GUI produce.)
            # - we don't have more than one ${}
            # - a ${} body is either a number, a this.fieldname,
            #   or a this.fieldname.id
            if "${" in template_value:
                [prefix,rest]=template_value.split("${",1);
                if "}" not in rest:
                    raise Exception("Template value has unbalanced brackets: "+
                                    template_value);
                [bracketed,suffix]=rest.split("}",1);
                bracketed=bracketed.strip()
                try:
                    number=int(bracketed)
                    is_number=True
                except ValueError:
                    try:
                        number=float(bracketed)
                        is_number=True
                    except ValueError:
                        is_number=False
                if is_number:                    
                    if prefix.strip()==" " and suffix.strip()==" ":
                        return number
                    else:
                        return prefix+bracketed+suffix;
                if bracketed.startswith("this."):
                    fieldname=bracketed[5:]
                    raw_id=False;
                    if fieldname.endswith(".id"):
                        fieldname=fieldname[:-3]
                        raw_id=True
                    return self.make_array_for_fieldname(
                        fieldname, prefix, suffix, raw_id)
            else:
                return template_value;
    def resolve_to_final_string(self,x):
        return x.prefix + self.downloaded_file_paths[str(x.value)]+x.suffix
    def make_array_for_fieldname(self, fieldname, prefix, suffix, raw_id):
        if fieldname not in self.entity_class.variables:
            raise Exception("Field "+fieldname+
                            " not found in entity class "+
                            this.entity_class.name);
        vtype = self.entity_class.variables[fieldname].type
        if vtype=="E" and not raw_id:
            raise Exception("Field "+fieldname+
                            " is a cross-entity reference (not implemented), "+
                            " did you mean "+fieldname+".id?");
        entity_values=[]
        for e in self.entities:            
            if fieldname not in e.values or e.values[fieldname]==None:
                raise Exception("Field "+fieldname+
                                " is not present in entity "+
                                str(e.entity_id));            
            entity_values.append(e.values[fieldname]);        
        if vtype=="W" and not raw_id:
            arr=[]
            for v in entity_values:
                self.wanted_file_ids.add(v);
                arr.append(FileIdTemplate(v,prefix,suffix))
            return arr;
        elif len(prefix)==0 and len(suffix)==0:
            return entity_values;
        else:
            arr=[]
            for v in entity_values:
                arr.append(prefix+str(v)+suffix);
            return arr;
    def download_wanted_files(self):
        for v in self.wanted_file_ids:
            wf = WorkFile.get_by_system_id(v);
            #only use the orig name if its there!
            if len(wf.originalName) > 0:
                file_path=os.path.join(self.download_dir,wf.originalName)
            else:
                file_path=os.path.join(self.download_dir,
                                   "input_file_"+str(v));            
            file_exists = os.path.exists(file_path);            
            if self.mock_file:
                if not file_exists:
                    shutil.copyfile(self.mock_file,
                                    file_path);
            else:
                if file_exists:
                    hash_in_system = wf.hash;
                    with open(file_path,"rb") as f:
                        hash_on_disk=hashlib.md5(f.read()).hexdigest()
                    if hash_in_system.lower() == hash_on_disk.lower():
                        self.already_downloaded(wf,file_path);
                    else:
                        os.remove(file_path);
                        self.download(wf,file_path);
                else:
                    self.download(wf,file_path);
    def download(self,wf,file_path):
        wf.download(file_path);
        self.downloaded_file_paths[str(wf.systemId)]=file_path;
    def already_downloaded(self,wf,file_path):
        self.downloaded_file_paths[str(wf.systemId)]=file_path;
    def create_job_run(self, typename):
        jr=JobRun(
            {
                "inputWorkFileIds":{
                    "inputs":sorted(list(self.wanted_file_ids))
                },
                "jobTypeName": typename,
                "status":"REQUESTED",
            }
        )
        jr.save()
        return jr.id
        
def main(argv):
    parser=argparse.ArgumentParser();
    parser.add_argument("TEMPLATE",
                        help="Existing filename of JSON template");
    parser.add_argument("JSON_OUT",
                        help="Filename to create of Cromwell input JSON");    
    parser.add_argument("DOWNLOADDIR",
                        help="Directory to download input file contents");
    parser.add_argument("-j","--create-job-run",metavar="JOB_TYPE",
                        help="After downloading, create a job run using this "+
                        "type name, with the files as inputs.")
    parser.add_argument("--job-run-file",
                        help="(with -j/--create-job-run) Create "+
                        "a file with this name that holds the new job run's "+
                        "ID.")
    parser.add_argument("--mock-file",
                        help="use this filename for every file's contents "+
                        "instead of performing real downloads (for testing "+
                        "template syntax")
    parser.add_argument('-i','--ini',
                        help="ini file for Operend credentials. "+
                        "If omitted, looks for filename in "+
                        "OPYRND_CONFIG environment variable.")

    if len(argv)==0:
        parser.print_help()
        return
    parsed_args=parser.parse_args(argv[1:])
    ini=parsed_args.ini or os.getenv('OPYRND_CONFIG')
    if not ini:
        parser.print_help();
        print("Either the --ini argument or OPYRND_CONFIG "+
              "environment variable is required.");
        return;
    ApiBacked.configure_from_file(ini);
    filler=TemplateFiller(parsed_args.TEMPLATE,
                          parsed_args.JSON_OUT,
                          parsed_args.DOWNLOADDIR,
                          parsed_args.mock_file)
    filler.run()
    if parsed_args.create_job_run:
        job_id=filler.create_job_run(parsed_args.create_job_run)
        print(f"Created job with id {job_id}");
        if parsed_args.job_run_file:
            jifile=open(parsed_args.job_run_file,"w")
            jifile.write(str(job_id))
            jifile.close()
    print("Done pre-pipeline Operend integration.")
    
if __name__=="__main__":
    main(sys.argv)
