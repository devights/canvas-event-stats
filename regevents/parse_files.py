import re
import os
import json
from dateutil.parser import parse
from db import bulk_insert_data


def walk_dirs():
    directory = 'logs/'
    for year in ["2024", "2023"]:
        for month in os.listdir(directory + year):
            for day in os.listdir(directory + year + '/' + month):
                for file in os.listdir(directory + year + '/' + month + '/' + day):
                    print(year, month, day)
                    if file.endswith('.json'):
                        with open(directory + year + '/' + month + '/' + day + '/' + file, 'r') as f:
                            # try:
                            proces_file_data(year, month, day, f.read())
                            # except Exception as e:
                            #     print(f'Error processing file: {file} - {e}')
                            #     traceback.print_exc()




def proces_file_data(year, month, day, contents):
    log_entries = contents.split('\n')
    insert_data = []

    for entry in log_entries:
        if entry:
            data = json.loads(entry)
            payload = data['textPayload']
            timestamp = parse(data['timestamp'])
            if "sis_provisioner.events" in payload and "enrollment ENROLLMENT" \
                    in payload:
                match = re.search(r'code: (\w+), regid: ([\w]+), section: ([\w\s-]+)', payload)
                if match:
                    accept_code = match.group(1)
                    regid = match.group(2)
                    section = match.group(3)
                    # print(f'Accept Code: {accept_code}, RegID: {regid}, Section: {section}, Timestamp: {timestamp}')
                    insert_data.append((timestamp, accept_code, section, regid))

                else:
                    print("No match", payload)
    bulk_insert_data(insert_data)

walk_dirs()
