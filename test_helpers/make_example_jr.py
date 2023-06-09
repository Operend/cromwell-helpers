# Hard-codey script just for testing cromwell2operend with the meta_big_job json; not part of deployment.
import sys
import os
import opyrnd;
from opyrnd import ApiBacked
from opyrnd.jobs import JobType, JobRun;

def main():
    jt_name = "rnaseqETLForTesting"
    if len(sys.argv)==1:
        ini_name=os.getenv("OPYRND_CONFIG")
    else:
        ini_name=sys.argv[1];
    if not ini_name:
        print("no config ini; pass an argument or setenv OPYRND_CONFIG")
        return;
    ApiBacked.configure_from_file(ini_name);
    jt=JobType.get_by_name(jt_name)
    if not jt:
        jt=JobType({"name":jt_name})
        jt.save_new()
    jr=JobRun({"jobTypeName":jt_name,"status":"RUNNING"})
    jr.save()
    print(jr.id);

if __name__=="__main__":
    main()
