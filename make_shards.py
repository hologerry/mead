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


def write_dataset(base="./shards", split="train", part="mead", video_paths_json=None, sub=False):
    desc = split
    assert os.path.exists(video_paths_json)

    # We shuffle the indexes to make sure that we
    # don't get any large sequences of a single class
    # in the dataset.
    nvideos = len(videos_names)
    indexes = list(range(nvideos))
    random.shuffle(indexes)

    # This is the output pattern under which we write shards.
    pattern = os.path.join(base, f"{split}-%06d.tar")

    # number of shards must bigger than world size
    with wds.ShardWriter(pattern, max_size=int(args.max_size), max_count=int(args.max_count)) as sink:
        for i in tqdm(indexes, desc=desc):

            # Internal information from the ImageNet dataset
            # instance: the file name and the numerical class.
            video_file = os.path.join("../TalkingHead-1KH_datasets", split, part, videos_names[i])

            # Read the JPEG-compressed image file contents.
            video = read_file(video_file)

            # Construct a unique key from the filename.
            video_name = videos_names[i].split(".")[0]
            key = f"{video_name}"
            # Useful check.
            assert key not in all_keys
            all_keys.add(key)

            # Construct a sample.
            xkey = key if args.file_key else "%09d" % i
            sample = {"__key__": xkey, "mp4": video}

            # Write the sample to the sharded tar archives.
            sink.write(sample)


print("# split", args.split)
write_dataset(
    base=args.shards,
    split=args.split,
    part=args.part,
    sub=args.sub,
    bitrate_json=args.bitrate_json,
    bitrate_thres=args.bitrate_thres,
    front_face_json=args.front_face_json,
    direction_json=args.direction_json,
    good_video_json=args.good_video_json,
    good_video_bit_rate_json=args.good_video_bit_rate_json,
    small_front_json=args.small_front_json,
)
