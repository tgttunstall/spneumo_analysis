#!/usr/bin/bash
#TODO: make this is a cmd
#To run checkm2, we need to give it a dir containing files as the inputlist on the cmd would be limited.
#So we have symlinked files for the up dirs

wc -l /nfs/research/martin/uniprot/research/spneumo_dataset/up_proteome_paths_TT.txt
#26749
# but remember there are 76 proteomes with 0 proteins26749

cd /nfs/research/martin/uniprot/research/spneumo_dataset

mkdir -pv up_data_1k/{1..27}
split -a 3 --numeric-suffixes=1 -l 1000 ../up_proteome_paths_TT.txt paths_

for i in paths_{01..27}; do
  k=$(echo $i | sed -E 's/paths_0*//g')
  echo $i
  for j in $(cat $i); do
    echo "DOING $i"; ln -s $j $k/
  done
done


