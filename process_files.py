import argparse
import json
import os

from tqdm import tqdm


def extract_tar(extracted_folder="../MEAD_extracted", downloaded_folder="../MEAD"):

    id_folders = os.listdir(os.path.join(downloaded_folder))

    for id_name in tqdm(id_folders, desc="Extracting", leave=False):
        id_path = os.path.join(downloaded_folder, id_name)
        if not os.path.isdir(id_path):
            continue
        files = os.listdir(id_path)
        for f in tqdm(files, desc=id_path, leave=True):
            if "video" in f:
                output_path = os.path.join(extracted_folder, id_name + "_" + f.split(".")[0])
                os.makedirs(output_path, exist_ok=True)
                cmd = f"tar xf {id_path}/{f} -C {output_path} --strip-components=1"
                # print(cmd)
                try:
                    os.system(cmd)
                except:
                    # print(e)
                    print(cmd)


def create_json_files(extracted_folder="../MEAD_extracted", processed_folder="../MEAD_processed"):
    os.makedirs(processed_folder, exist_ok=True)
    video_files = []

    id_folders = sorted(os.listdir(os.path.join(extracted_folder)))
    for id_name in tqdm(id_folders, desc="Creating Json files", leave=False):
        id_path = os.path.join(extracted_folder, id_name)
        if not os.path.isdir(id_path):
            continue
        files = sorted(os.listdir(id_path))
        for f in tqdm(files, desc=id_path, leave=True):
            if "mp4" in f:
                f_path = os.path.join(id_name, f)
                video_files.append(f_path)

    video_files_dict = {}
    video_files_dict["videos"] = video_files

    with open(os.path.join(processed_folder, "video_files.json"), "w") as f:
        json.dump(video_files_dict, f)

    angle_videos_files = [v for v in video_files if "front" not in v]

    print("number of angle videos:", len(angle_videos_files))
    print("number of front videos:", len(video_files) - len(angle_videos_files))

    pair_front_video_files = []
    for v in angle_videos_files:
        vf = v.replace("down", "front")
        vf = vf.replace("left_30", "front")
        vf = vf.replace("left_60", "front")
        vf = vf.replace("right_30", "front")
        vf = vf.replace("right_60", "front")
        vf = vf.replace("up", "front")
        pair_front_video_files.append(vf)

    print("number of pair front videos:", len(pair_front_video_files))
    angle_front_pair_dict = {}
    angle_front_pair_dict["angle_videos"] = angle_videos_files
    angle_front_pair_dict["front_videos"] = pair_front_video_files

    with open(os.path.join(processed_folder, "angle_front_videos.json"), "w") as f:
        json.dump(angle_front_pair_dict, f)

    val_id_names = ["M040", "M041", "M042", "W037", "W038", "W040"]

    def is_val_video(v):
        for id_name in val_id_names:
            if id_name in v:
                return True
        return False

    train_angle_videos = []
    train_front_videos = []
    val_angle_videos = []
    val_front_videos = []
    for angle_video in angle_videos_files:
        if is_val_video(angle_video):
            val_angle_videos.append(angle_video)
        else:
            train_angle_videos.append(angle_video)

    for front_video in pair_front_video_files:
        if is_val_video(front_video):
            val_front_videos.append(front_video)
        else:
            train_front_videos.append(front_video)

    print("number of train angle videos:", len(train_angle_videos))
    print("number of train front videos:", len(train_front_videos))
    print("number of val angle videos:", len(val_angle_videos))
    print("number of val front videos:", len(val_front_videos))

    train_angle_front_pair_dict = {}
    train_angle_front_pair_dict["angle_videos"] = train_angle_videos
    train_angle_front_pair_dict["front_videos"] = train_front_videos

    val_angle_front_pair_dict = {}
    val_angle_front_pair_dict["angle_videos"] = val_angle_videos
    val_angle_front_pair_dict["front_videos"] = val_front_videos

    with open(os.path.join(processed_folder, "train_angle_front_pair_videos.json"), "w") as f:
        json.dump(train_angle_front_pair_dict, f)

    with open(os.path.join(processed_folder, "val_angle_front_pair_videos.json"), "w") as f:
        json.dump(val_angle_front_pair_dict, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Process the downloaded MEAD tar files""")
    parser.add_argument("op", default="extract", help="operation to perform")

    args = parser.parse_args()

    if args.op == "extract":
        extract_tar()
    elif args.op == "create":
        create_json_files()
    else:
        raise Exception("unknown operation")
