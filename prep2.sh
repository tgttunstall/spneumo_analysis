#!/usr/bin/bash

#To run checkm2, we need to give it a dir containing files as the inputlist on the cmd would be limited.
#So we have symlinked files for the atb dirs

#wc -l  /nfs/research/martin/uniprot/research/spneumo_dataset/atb_proteome_paths_TT.txt
#119701
#cd /nfs/research/martin/uniprot/research/spneumo_dataset
#mkdir -pv atb_data_1k/{1..120}
#split -a 3 --numeric-suffixes=1 -l 1000 ../atb_proteome_paths_TT.txt paths_

for i in paths_{001}; do k=$(echo $i | sed -E 's/paths_0*//g'); echo $i; for j in $(cat $i); do echo "DOING $i"; ln -s $j $k/ ;done ;done 
