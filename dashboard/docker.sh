#!/bin/bash

repo=giesekow/mongo-qas-dashboard
tag=0.1.20

latest=false

while getopts ":lh" option; do
  case $option in
    h) echo "usage: $0 command [tag] [-h] [-l]"; exit ;;
    l) latest=true ;;
    ?) echo "error: option -$OPTARG is not implemented"; exit ;;
  esac
done

# remove the options from the positional parameters
shift $(( OPTIND - 1 ))

ls_opts=()
$latest && ls_opts+=( -l )

if [ -z $2 ]
then
  echo "tag not provided using $tag"
else
  tag=$2
fi

if [ $1 = "build" ]
then
  filter="${repo}:${tag}"
  docker rmi -f $(docker images --filter=reference="$filter" -q)
  docker build -f ./Dockerfile -t ${repo}:${tag} .
fi

if [ $1 = "clean" ]
then
  filter="${repo}-*:${tag}"
  docker rmi -f $(docker images --filter=reference="$filter" -q)
fi

if [ $1 = "push" ]
then
  docker push ${repo}:${tag}
  if [ $latest = true ]
  then
    docker rmi ${repo}:latest
    docker tag ${repo}:${tag} ${repo}:latest
    docker push ${repo}:latest
  fi
fi