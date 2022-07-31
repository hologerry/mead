import argparse
import json
import os
import os.path
import random

import webdataset as wds

from tqdm import tqdm


def read_file(fname):
    "Read a binary file from disk."
    os.path.exists(fname)
    with open(fname, "rb") as stream:
        return stream.read()


def same_video(angle_file_path, front_file_path):
    angle_splits = angle_file_path.split("/")
    front_splits = front_file_path.split("/")
    for angle_split, front_split in zip(angle_splits, front_splits):
        if angle_split != front_split and front_split != "front":
            return False

    return True


def write_dataset(
    base="../shards_mead", split="train", video_folder="../MEAD_resized_256", processed_folder="../MEAD_processed"
):
    os.makedirs(base, exist_ok=True)

    all_keys = set()

    desc = split
    if split == "train":
        video_json = os.path.join(processed_folder, "train_angle_front_pair_videos.json")
    else:
        video_json = os.path.join(processed_folder, "val_angle_front_pair_videos.json")
    with open(video_json, "r") as f:
        video_dict = json.load(f)
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

    # for i in tqdm(indexes, desc=desc):

    #     # Internal information from the ImageNet dataset
    #     # instance: the file name and the numerical class.
    #     angle_video_file = os.path.join(video_folder, angle_videos[i])
    #     front_video_file = os.path.join(video_folder, front_videos[i])

    #     assert same_video(angle_video_file, front_video_file)

    # number of shards must bigger than world size
    with wds.ShardWriter(pattern, maxsize=int(args.maxsize), maxcount=int(args.maxcount)) as sink:
        for i in tqdm(indexes, desc=desc):

            # Internal information from the ImageNet dataset
            # instance: the file name and the numerical class.
            angle_video_file = os.path.join(video_folder, angle_videos[i])
            front_video_file = os.path.join(video_folder, front_videos[i])

            assert same_video(angle_video_file, front_video_file)

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
            sample = {"__key__": x_key, "angle.mp4": angle_video, "front.mp4": front_video}

            # Write the sample to the shard tar archives.
            sink.write(sample)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Generate shard dataset from original ImageNet data.""")
    parser.add_argument("--split", default="train", help="which splits to write")

    parser.add_argument("--file_key", action="store_true", help="use file as key (default: index)")
    parser.add_argument("--maxsize", type=float, default=1e10)
    parser.add_argument("--maxcount", type=float, default=1e6)

    parser.add_argument(
        "--shards", default="../MEAD_processed/shards_mead/", help="directory where shards are written"
    )

    args = parser.parse_args()

    write_dataset(
        base=args.shards,
        split=args.split,
    )
