import json
import os

import cv2

from torchvision.io import read_video
from tqdm import tqdm


video_dir = "../data/MEAD_extracted"
mead_data_json = "../data/MEAD_processed/video_files.json"
mead_data_meta_json = "../data/MEAD_processed/video_meta_files.json"

with open(mead_data_json, "r") as f:
    mead_data = json.load(f)

print(mead_data.keys())

videos = mead_data["videos"]

all_data_dict = {}


def check_video_valid(video_file):
    if not os.path.exists(video_file):
        print("video file not exists", video_file)
        return False
    try:
        video = cv2.VideoCapture(video_file)
        video.get(cv2.CAP_PROP_FRAME_COUNT)
        success, image = video.read()
        return success
    except:
        print("video file not valid", video_file)
        return False


for i, video in tqdm(enumerate(videos), total=len(videos)):
    splits = video.split("/")
    id_name, came_name, express_name, degree_name, clip = splits
    video_file = os.path.join(video_dir, id_name, came_name, express_name, degree_name, clip)
    v_frames, a_frames, info = read_video(video_file)
    if v_frames.shape[0] < 30:
        print("video file not valid", video_file)
        continue
    if id_name not in all_data_dict:
        all_data_dict[id_name] = {}
    if came_name not in all_data_dict[id_name]:
        all_data_dict[id_name][came_name] = {}
    if express_name not in all_data_dict[id_name][came_name]:
        all_data_dict[id_name][came_name][express_name] = {}
    if degree_name not in all_data_dict[id_name][came_name][express_name]:
        all_data_dict[id_name][came_name][express_name][degree_name] = []
    all_data_dict[id_name][came_name][express_name][degree_name].append(clip)

print(all_data_dict)
with open(mead_data_meta_json, "w") as f:
    json.dump(all_data_dict, f)

# with open(mead_data_meta_json, "r") as f:
#     all_data_dict = json.load(f)

# print(all_data_dict.keys())

# for k, v in all_data_dict.items():
#     print(k, v.keys())

# ids = list(all_data_dict.keys())
# val_ids = ["M003_video", "M007_video", "W009_video", "W011_video"]
# not_usd_ids = ["W017_video"]

# ids = [id for id in ids if id not in val_ids]
# ids = [id for id in ids if id not in not_usd_ids]
# print("ids", ids)
