import json
import os


info_dict_json = "/D_data/Front/data/MEAD_frames_names/all_files.json"


def read_json_file(file):
    with open(file, "r") as r:
        response = r.read()
        response = response.replace("\n", "")
        response = response.replace("}{", "},{")
        response = "[" + response + "]"
        return json.loads(response)


info_dict = read_json_file(info_dict_json)

file_names = []
for i, info in enumerate(info_dict):
    message_content = info["MessageContent"]
    if ".png" not in message_content:
        continue
    # if "M019_frames/front/angry/level_1/022/" in message_content:
    #     print(message_content)
    if "0.00 B" in message_content:
        print(message_content)
        continue
    file_name = message_content.split(";")[0][6:]
    file_names.append(file_name)
    # if i < 100:
    #     print(message_content)
    #     print(file_name)

file_names_dict = {}
file_names_dict["file_names"] = file_names

with open("/D_data/Front/data/MEAD_frames_names/file_names.json", "w") as w:
    json.dump(file_names_dict, w)
