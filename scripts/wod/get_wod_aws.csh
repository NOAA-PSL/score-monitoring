#!/bin/csh
#
#
set yr=2023
set mo="02"
set dd="$1"
set aws="/Users/sfredrick/.local/bin/aws"
set bucket="noaa-reanalyses-pds/analyses/scout_runs/3dvar_coupledreanl_scoutrun_v2"

foreach hr(00 06 12 18)
   set gdas="gdas."${yr}${mo}${dd}
   set rest_bucket="${yr}${mo}${dd}${hr}/${gdas}/${hr}/analysis/ocean/diags"
   set file="wod_s_pfl.nc"
   ${aws} s3 cp s3://${bucket}/${yr}/${mo}/${rest_bucket}/${file} .
   mv ${file} "wod_s_pfl_${yr}${mo}${dd}_${hr}.nc"
   
   set file="wod_t_pfl.nc"
   ${aws} s3 cp s3://${bucket}/${yr}/${mo}/${rest_bucket}/${file} .
   mv ${file} "wod_t_pfl_${yr}${mo}${dd}_${hr}.nc"
end
end
