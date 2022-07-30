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


def create_json_files(extracted_folder="../MEAD_extracted"):
    video_files_json = os.path.join(extracted_folder, "video_files.json")
    angle_front_pair_video_files_json = os.path.join(extracted_folder, "angle_front_video_files.json")

    video_files = []

    id_folders = os.listdir(os.path.join(extracted_folder))
    for id_name in tqdm(id_folders, desc="Creating video_files.json", leave=False):
        id_path = os.path.join(extracted_folder, id_name)
        if not os.path.isdir(id_path):
            continue
        files = os.listdir(id_path)
        for f in tqdm(files, desc=id_path, leave=True):
            if "mp4" in f:
                f_path = os.path.join(id_name, f)
                video_files.append(f_path)

    video_files_dict = {}
    video_files_dict["videos"] = video_files

    with open(video_files_json, "w") as f:
        json.dump(video_files_dict, f)

    angle_videos = [v for v in video_files if "front" not in v]
    angle_front_pair_dict = {}
    angle_front_pair_dict["angle_videos"] = angle_videos

    print("number of angle videos:", len(angle_videos))
    print("number of front videos:", len(video_files) - len(angle_videos))

    pair_front_video_files = []
    for v in angle_videos:
        vf = v.replace("down", "front")
        vf = vf.replace("left_30", "front")
        vf = vf.replace("left_60", "front")
        vf = vf.replace("right_30", "front")
        vf = vf.replace("right_60", "front")
        vf = vf.replace("up", "front")
        pair_front_video_files.append(vf)

    print("number of pair front videos:", len(pair_front_video_files))
    angle_front_pair_dict["front_videos"] = pair_front_video_files

    with open(angle_front_pair_video_files_json, "w") as f:
        json.dump(angle_front_pair_dict, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Process the downloaded MEAD tar files""")
    parser.add_argument("op", default="extract", help="operation to perform")

    args = parser.parse_args()

    if args.op == "extract":
        extract_tar()
    elif args.op == "create_json":
        create_json_files()
    else:
        raise Exception("unknown operation")
