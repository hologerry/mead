import json
import os


file_names_json = "/D_data/Front/data/MEAD_frames_names/file_names.json"

file_names_dict = json.load(open(file_names_json, "r"))

file_names = file_names_dict["file_names"]

all_file_names_num = len(file_names)
print("all_file_names_num", all_file_names_num)

all_data_dict = {}

valid_file_names_num = 0
for name in file_names:
    try:
        id_name, angle, expression, level, clip, frame = name.strip().split("/")
    except:
        print("name", name)
        continue

    valid_file_names_num += 1

    if id_name not in all_data_dict:
        all_data_dict[id_name] = {}
    if angle not in all_data_dict[id_name]:
        all_data_dict[id_name][angle] = {}
    if expression not in all_data_dict[id_name][angle]:
        all_data_dict[id_name][angle][expression] = {}
    if level not in all_data_dict[id_name][angle][expression]:
        all_data_dict[id_name][angle][expression][level] = {}
    if clip not in all_data_dict[id_name][angle][expression][level]:
        all_data_dict[id_name][angle][expression][level][clip] = []
    all_data_dict[id_name][angle][expression][level][clip].append(frame)

print("valid_file_names_num", valid_file_names_num)

actual_frames = 0
sorted_all_data_dict = {}
for id_name in sorted(all_data_dict.keys()):
    sorted_all_data_dict[id_name] = {}
    for angle in sorted(all_data_dict[id_name].keys()):
        sorted_all_data_dict[id_name][angle] = {}
        for expression in sorted(all_data_dict[id_name][angle].keys()):
            sorted_all_data_dict[id_name][angle][expression] = {}
            for level in sorted(all_data_dict[id_name][angle][expression].keys()):
                sorted_all_data_dict[id_name][angle][expression][level] = {}
                for clip in sorted(all_data_dict[id_name][angle][expression][level].keys()):
                    if len(all_data_dict[id_name][angle][expression][level][clip]) < 15:
                        print(
                            f"{len(all_data_dict[id_name][angle][expression][level][clip])} {id_name} {angle} {expression} {level} {clip} skipped"
                        )
                        continue
                    actual_frames += len(all_data_dict[id_name][angle][expression][level][clip])
                    sorted_all_data_dict[id_name][angle][expression][level][clip] = sorted(
                        all_data_dict[id_name][angle][expression][level][clip]
                    )


print("actual_frames", actual_frames)
mead_data_meta_json = "/D_data/Front/data/MEAD_processed/video_meta_frame_files.json"

with open(mead_data_meta_json, "w") as f:
    json.dump(sorted_all_data_dict, f)
