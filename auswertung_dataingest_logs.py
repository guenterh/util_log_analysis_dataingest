import os
import re
import string
import random
import datetime

docker_cc = "/swissbib/harvesting/docker.cc/logs"
cc_classic = "/swissbib/harvesting/rundir_sbucoai1"
docker_consumercbs = "/swissbib/harvesting/docker.consumercbs/logging"

#docker_cc = "docker.cc"
#cc_classic = "cc.classic"
#docker_consumercbs = "docker.consumercbs"

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
 return ''.join(random.choice(chars) for _ in range(size))

def run_log_analysis():
    today =  datetime.date.today()

    #start with docker.cc
    (_, _, filenames) = next(os.walk(docker_cc))
    summary_files = list(filter(lambda elem :
                                elem.find("summary.log") != -1,filenames))
    summary_dic = {}
    summary_pattern = re.compile("{}.*?Anzahl der nach Kafka gesendeten messages.*$".format(today))
    summary_pattern_rero = re.compile("{}.*?number of deleted records.*?number of updated records.*$".format(today))
    for file in summary_files:
        with open(docker_cc + os.sep + file,"r") as content_file:

            line = content_file.readline()
            while line:
                if file == "rero_summary.log":
                    if summary_pattern_rero.match(line):
                        summary_dic[file] = line
                        break
                elif summary_pattern.match(line):
                    summary_dic[file] = line
                    break
                line = content_file.readline()
            #complete_content = content_file.read
            #summary_dic[file] = complete_content

    (_, _, filenames) = next(os.walk(docker_consumercbs))
    consumer_files = list(filter(lambda elem :
                                elem.find("cbsconsumer.log") != -1,filenames))
    consumer_dic = {}
    consumer_pattern = re.compile("{}.*?number of written messages.*$".format(today))

    for file in consumer_files:
        with open(docker_consumercbs + os.sep + file,"r") as content_file:

            line = content_file.readline()
            while line:
                if consumer_pattern.match(line):
                    consumer_dic[id_generator()] = line

                line = content_file.readline()


    (_, _, filenames) = next(os.walk(cc_classic))
    cc_classic_files = list(filter(lambda elem:
                                 elem.find("process.harvesting.log") != -1, filenames))
    cc_classic_dic = {}
    cc_classic_pattern_date = re.compile("{}.*$".format(today))

    for file in cc_classic_files:
        with open(cc_classic+ os.sep + file, "r") as content_file:

            line = content_file.readline()
            last_task_found = False
            message = []
            while line:

                if cc_classic_pattern_date.match(line):
                    last_task_found = True

                if last_task_found and (line.find("records deleted") != -1 or
                            line.find("records parse error") != -1 or
                            line.find("records to cbs inserted") != -1 or
                            line.find("records to cbs updated") != -1 or
                            line.find("records to cbs (without") != -1):
                    message.append(line)

                line = content_file.readline()
            cc_classic_dic[file] = " # ".join(message)

    with open("{}_summary_of_all_files.txt".format(str(today)), "a") as result_file:

        result_file.write("results of new data ingest\n\n")

        for key, value in summary_dic.items():
            result_file.write(key + "  " + value + "\n")

        result_file.write("\nresults of new cbs consumer\n\n")

        for key, value in consumer_dic.items():
            result_file.write(key + "  " + value + "\n")


        result_file.write("\nresults of classic\n\n")

        for key, value in cc_classic_dic.items():
            result_file.write(key + "  " + value + "\n")

    #todo
    # keys sortieren
    # files consumer
    # per mail verschicken??
    print()


if __name__ == '__main__':
    run_log_analysis()
