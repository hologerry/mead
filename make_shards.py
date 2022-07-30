import argparse
import json
import os
import os.path
import random

import webdataset as wds

from tqdm import tqdm


parser = argparse.ArgumentParser("""Generate sharded dataset from original ImageNet data.""")
parser.add_argument("--split", default="train", help="which splits to write")
parser.add_argument("--part", default="cropped_clips_256", help="which part to write")
parser.add_argument("--file_key", action="store_true", help="use file as key (default: index)")
parser.add_argument("--max_size", type=float, default=5e8)
parser.add_argument("--max_count", type=float, default=100000)
parser.add_argument("--shards", default="shards/", help="directory where shards are written")
parser.add_argument("--data_process_type", type=str, default="raw")
parser.add_argument("--bitrate_json", type=str, default=None)
parser.add_argument("--front_face_json", type=str, default=None)
parser.add_argument("--direction_json", type=str, default=None)
parser.add_argument("--good_video_json", type=str, default=None)
parser.add_argument("--good_video_bit_rate_json", type=str, default=None)
parser.add_argument("--small_front_json", type=str, default=None)
parser.add_argument("--bitrate_thres", type=int, default=128)
parser.add_argument("--debug", action="store_true")
parser.add_argument("--sub", action="store_true")

args = parser.parse_args()


os.makedirs(args.shards, exist_ok=True)


def read_file(fname):
    "Read a binary file from disk."
    os.path.exists(fname)
    with open(fname, "rb") as stream:
        return stream.read()


all_keys = set()


def write_dataset(base="./shards", split="train", root_path="../MEAD_processed"):
    desc = split

    if split == 'train':
        video_paths_json = os.path.join(root_path, "train_angle_front_pair_videos.json")
    else:
        video_paths_json = os.path.join(root_path, "val_angle_front_pair_videos.json")
    angle_videos = video_dict["angle_videos"]
    front_videos = video_dict["front_videos"]

    # We shuffle the indexes to make sure that we
    # don't get any large sequences of a single class
    # in the dataset.
    n_videos = len(angle_videos)
    indexes = list(range(n_videos))
    random.shuffle(indexes)

    # This is the output pattern under which we write shards.
    pattern = os.path.join(base, f"{split}-%06d.tar")

    # number of shards must bigger than world size
    with wds.ShardWriter(pattern, max_size=int(args.max_size), max_count=int(args.max_count)) as sink:
        for i in tqdm(indexes, desc=desc):

            # Internal information from the ImageNet dataset
            # instance: the file name and the numerical class.
            angle_video_file = os.path.join("../MEAD_extracted", angle_videos[i])
            front_video_file = os.path.join("../MEAD_extracted", front_videos[i])

            # Read the JPEG-compressed image file contents.
            angle_video = read_file(angle_video_file)
            front_video = read_file(front_video_file)

            # Construct a unique key from the filename.
            key = f"{angle_video_file}"
            # Useful check.
            assert key not in all_keys
            all_keys.add(key)

            # Construct a sample.
            x_key = key if args.file_key else "%09d" % i
            sample = {"__key__": x_key, "angle_mp4": angle_video, "front_mp4": front_video}

            # Write the sample to the sharded tar archives.
            sink.write(sample)


print("# split", args.split)
write_dataset(
    base=args.shards,
    split=args.split,
    part=args.part,
)
