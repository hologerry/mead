import argparse
import json
import os

from multiprocessing import Pool

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
                cmd = f"tar xf {id_path}/{f} --strip-components=1 --skip-old-files -C {output_path}"
                # print(cmd)
                try:
                    os.system(cmd)
                except:
                    # print(e)
                    print(cmd)


def cmd_wrapper(program):
    out_dir, cmd = program
    os.makedirs(out_dir, exist_ok=True)
    os.system(cmd)


def resize(
    job_idx,
    job_nums,
    extracted_folder="../MEAD_extracted",
    processed_folder="../MEAD_processed",
    resized_folder="../MEAD_resized_256",
):
    video_files_json = os.path.join(processed_folder, "video_files.json")
    with open(video_files_json, "r") as f:
        video_files_dict = json.load(f)
    video_files = video_files_dict["videos"]

    video_files_per_job = len(video_files) // job_nums

    current_job_video_files = video_files[job_idx * video_files_per_job : (job_idx + 1) * video_files_per_job]

    programs = []
    for video_file in current_job_video_files:
        cmd = f"ffmpeg -y -i {os.path.join(extracted_folder, video_file)} -hide_banner -loglevel error -vf scale=-2:256 {os.path.join(resized_folder, video_file)}"
        out_dir = os.path.join(resized_folder, os.path.dirname(video_file))
        programs.append([out_dir, cmd])

    pool = Pool(48)
    pool.map(cmd_wrapper, programs)
    pool.close()


def create_json_files(extracted_folder="../MEAD_resized_256", processed_folder="../MEAD_processed"):
    os.makedirs(processed_folder, exist_ok=True)
    video_files = []

    id_folders = sorted(os.listdir(os.path.join(extracted_folder)))
    for id_name in id_folders:
        id_path = os.path.join(extracted_folder, id_name)
        angle_folders = sorted(os.listdir(id_path))
        for angle_folder in angle_folders:
            angle_path = os.path.join(id_path, angle_folder)
            expression_folder = sorted(os.listdir(angle_path))
            for expression_name in expression_folder:
                expression_path = os.path.join(angle_path, expression_name)
                levels = sorted(os.listdir(expression_path))
                for level in levels:
                    level_path = os.path.join(expression_path, level)
                    videos = sorted(os.listdir(level_path))
                    for video in videos:
                        # video_path = os.path.join(level_path, video)
                        relative_path = os.path.join(id_name, angle_folder, expression_name, level, video)
                        video_files.append(relative_path)

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

    valid_angle_videos_files = []
    valid_pair_front_video_files = []
    for angle_file, front_file in zip(angle_videos_files, pair_front_video_files):
        if not os.path.exists(os.path.join(extracted_folder, front_file)):
            continue
        if not os.path.exists(os.path.join(extracted_folder, front_file)):
            continue
        valid_angle_videos_files.append(angle_file)
        valid_pair_front_video_files.append(front_file)

    angle_front_pair_dict = {}
    angle_front_pair_dict["angle_videos"] = valid_angle_videos_files
    angle_front_pair_dict["front_videos"] = valid_pair_front_video_files

    print("number of valid angle videos:", len(valid_angle_videos_files))
    print("number of valid front videos:", len(valid_pair_front_video_files))

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
    for angle_video in valid_angle_videos_files:
        if is_val_video(angle_video):
            val_angle_videos.append(angle_video)
        else:
            train_angle_videos.append(angle_video)

    for front_video in valid_pair_front_video_files:
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
    parser.add_argument("job_idx", type=int, default=0)
    parser.add_argument("job_num", type=int, default=500)

    args = parser.parse_args()

    if args.op == "extract":
        extract_tar()
    elif args.op == "resize":
        resize(job_idx=args.job_idx, job_num=args.job_num)
    elif args.op == "create":
        create_json_files()
    else:
        raise Exception("unknown operation")
