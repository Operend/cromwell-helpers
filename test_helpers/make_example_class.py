# Hard-codey script just for testing cromwell2operend with the meta_big_job json; not part of deployment.
import sys
import os
import opyrnd;
from opyrnd import ApiBacked, EntityClass, VariableDefinition;

def main():
    class_name = "rnaseqETLForTesting"
    if len(sys.argv)==1:
        ini_name=os.getenv("OPYRND_CONFIG")
    else:
        ini_name=sys.argv[1];
    if not ini_name:
        print("no config ini; pass an argument or setenv OPYRND_CONFIG")
        return;
    ApiBacked.configure_from_file(ini_name);
    EntityClass.delete_by_name("rnaseqETLForTesting");
    variables={}
    def add_variable(name, vtype):
        variables[name]=VariableDefinition(name,vtype);
    add_variable("R1_id","T");
    add_variable("R2_id","T");
    add_variable("fastqc_1_html","W");
    add_variable("fastqc_1_zip","W");
    add_variable("fastqc_2_html","W");
    add_variable("fastqc_2_zip","W");
    add_variable("somalier_output","W");
    ec=EntityClass(class_name=class_name,variables=variables)
    ec.save();
    
if __name__=="__main__":
    main()
