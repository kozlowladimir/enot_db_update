cd $1
git checkout master
git pull
git log --pretty=format:'%H' -n 1 > $2/current_commit_hash
cd $2
python script.py > log.txt