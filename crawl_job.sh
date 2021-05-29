#!/bin/bash
echo "To pause crawl job, press Ctrl^C"
echo "Starting..."
scrapy crawl $1 -s JOBDIR=./crawls/$1