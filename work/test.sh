
thread_ids=`awk '{print $3}' analysis.txt | sort -n | uniq `
for id in $thread_ids
do
  grep $id analysis.txt | tail -n1
done
